package migrate_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/internal/migrate"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestMigrate(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostDir := t.TempDir()
	siad, err := startSiad(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	defer siad.Close()

	err = siad.Host().SetInternalSettings(modules.HostInternalSettings{
		AcceptingContracts: true,
		WindowSize:         10,
		MaxDuration:        100,
		MaxCollateral:      stypes.SiacoinPrecision.Mul64(10000),
		CollateralBudget:   stypes.SiacoinPrecision.Mul64(100000),
		MinStoragePrice:    stypes.NewCurrency64(1),
		Collateral:         stypes.NewCurrency64(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := siad.Host().AddStorageFolder(t.TempDir(), rhp2.SectorSize*64); err != nil {
			t.Fatal(err)
		}
	}

	hostKey := types.PublicKey(siad.Host().PublicKey().Key)
	hostUC, err := siad.Wallet().NextAddress()
	if err != nil {
		t.Fatal(err)
	}
	hostWalletAddr := hostUC.UnlockHash()
	hostAddr := string(siad.Host().ExternalSettings().NetAddress)

	// create a renter
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterNode, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renterNode.Close()

	renter, err := test.NewRenter(renterKey, t.TempDir(), renterNode, log.Named("renter"))
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()

	if err := siad.Gateway().Connect(modules.NetAddress(renterNode.GatewayAddr())); err != nil {
		t.Fatal(err)
	}

	miner := test.NewMiner(renterNode.ChainManager())
	if err := renterNode.ChainManager().Subscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	renterNode.TPool().Subscribe(miner)

	// mine enough blocks to fund both wallets
	if err := miner.Mine(types.Address(hostWalletAddr), 20); err != nil {
		t.Fatal(err)
	} else if err := miner.Mine(types.Address(renter.Wallet().Address()), 20); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// form a few contracts with the host and upload some data with them
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// mine until the contracts have expired
	for i := 0; i < 50; i++ {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// form additional contracts with the host and upload some data with them
	var activeContracts []types.FileContractID
	var activeSectors []types.Hash256
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}
		activeContracts = append(activeContracts, revision.ID())

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
				activeSectors = append(activeSectors, root)
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// shutdown the siad node
	if err := siad.Close(); err != nil {
		t.Fatal(err)
	}

	// migrate the host
	db, err := sqlite.OpenDatabase(filepath.Join(hostDir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := migrate.Siad(context.Background(), db, hostDir, false, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}

	// start a hostd node using the renter's wallet key so it has existing funds
	hostNode, err := test.NewEmptyHost(renterKey, hostDir, renterNode, log.Named("hostd"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostNode.Close()

	settings, err := hostNode.RHP2Settings()
	if err != nil {
		t.Fatal(err)
	} else if settings.TotalStorage != 3*rhp2.SectorSize*64 {
		t.Fatalf("expected host to have %d total storage, got %d", 3*rhp2.SectorSize*64, settings.TotalStorage)
	} else if settings.AcceptingContracts != true {
		t.Fatal("expected host to be accepting contracts")
	} else if settings.MaxDuration != 100 {
		t.Fatalf("expected host to have max duration of 100, got %v", settings.MaxDuration)
	}

	expectedUsage := uint64(10 * 10) // 10 contracts * 10 sectors
	expectedTotal := uint64(3 * 64)  // 3 folder * 64 sectors

	usedSectors, totalSectors, err := hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != expectedUsage {
		t.Fatalf("expected host to have %d used sectors, got %v", expectedUsage, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err := hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != expectedUsage {
		t.Fatalf("expected host to have %d contract sectors, got %v", expectedUsage, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != expectedUsage {
		t.Fatalf("expected host to have %d physical sectors, got %v", expectedUsage, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 10 {
		t.Fatalf("expected host to have 10 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 0 {
		t.Fatalf("expected host to have 0 successful contracts, got %v", metrics.Contracts.Successful)
	}

	if hostNode.Store().HostKey().PublicKey() != hostKey {
		t.Fatal("host key mismatch")
	}

	for _, root := range activeSectors {
		data, err := hostNode.Storage().Read(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(data) != root {
			t.Fatalf("root mismatch: expected %v, got %v", root, rhp2.SectorRoot(data))
		}
	}

	var maxWindowEnd uint64
	for _, contractID := range activeContracts {
		fc, err := hostNode.Store().Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if fc.Revision.Filesize != 10*rhp2.SectorSize {
			t.Fatalf("expected contract to have 10 sectors, got %v", fc.Revision.Filesize/rhp2.SectorSize)
		}

		if fc.Revision.WindowEnd > maxWindowEnd {
			maxWindowEnd = fc.Revision.WindowEnd
		}
	}

	waitForSync := func() {
		// wait for the host node to sync
		for {
			if hostNode.Contracts().ScanHeight() == hostNode.ChainManager().TipState().Index.Height {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	waitForSync()

	// mine a block and wait for it to be processed
	for {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		waitForSync()

		if hostNode.Contracts().ScanHeight() >= maxWindowEnd+10 {
			break
		}
	}

	time.Sleep(time.Second)

	// check that all contracts are now successful
	c, _, err := hostNode.Store().Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	}

	for _, contract := range c {
		if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatalf("expected contract %v to be successful, got %v", contract.Revision.ParentID, contract.Status)
		}
	}

	// check that everything was cleaned up
	usedSectors, totalSectors, err = hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != 0 {
		t.Fatalf("expected host to have %d used sectors, got %v", 0, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err = hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != 0 {
		t.Fatalf("expected host to have %d contract sectors, got %v", 0, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != 0 {
		t.Fatalf("expected host to have %d physical sectors, got %v", 0, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 0 {
		t.Fatalf("expected host to have 0 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 10 {
		t.Fatalf("expected host to have 10 successful contracts, got %v", metrics.Contracts.Successful)
	}
}

func TestPartialMigrate(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostDir := t.TempDir()
	siad, err := startSiad(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	defer siad.Close()

	err = siad.Host().SetInternalSettings(modules.HostInternalSettings{
		AcceptingContracts: true,
		WindowSize:         10,
		MaxDuration:        100,
		MaxCollateral:      stypes.SiacoinPrecision.Mul64(10000),
		CollateralBudget:   stypes.SiacoinPrecision.Mul64(100000),
		MinStoragePrice:    stypes.NewCurrency64(1),
		Collateral:         stypes.NewCurrency64(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	var folderPaths []string
	for i := 0; i < 3; i++ {
		fp := t.TempDir()
		if err := siad.Host().AddStorageFolder(fp, rhp2.SectorSize*64); err != nil {
			t.Fatal(err)
		}
		folderPaths = append(folderPaths, fp)
	}

	hostKey := types.PublicKey(siad.Host().PublicKey().Key)
	hostUC, err := siad.Wallet().NextAddress()
	if err != nil {
		t.Fatal(err)
	}
	hostWalletAddr := hostUC.UnlockHash()
	hostAddr := string(siad.Host().ExternalSettings().NetAddress)

	// create a renter
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterNode, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renterNode.Close()

	renter, err := test.NewRenter(renterKey, t.TempDir(), renterNode, log.Named("renter"))
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()

	if err := siad.Gateway().Connect(modules.NetAddress(renterNode.GatewayAddr())); err != nil {
		t.Fatal(err)
	}

	miner := test.NewMiner(renterNode.ChainManager())
	if err := renterNode.ChainManager().Subscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	renterNode.TPool().Subscribe(miner)

	// mine enough blocks to fund both wallets
	if err := miner.Mine(types.Address(hostWalletAddr), 20); err != nil {
		t.Fatal(err)
	} else if err := miner.Mine(types.Address(renter.Wallet().Address()), 20); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	var activeContracts []types.FileContractID
	var activeSectors []types.Hash256

	// form a few contracts with the host and upload some data with them
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}
		activeContracts = append(activeContracts, revision.ID())

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
				activeSectors = append(activeSectors, root)
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	// shutdown the siad node
	if err := siad.Close(); err != nil {
		t.Fatal(err)
	}

	// rename one of the storage folders so it can't be found
	oldPath := folderPaths[2]
	newPath := oldPath + "missing"
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}

	// migrate the host
	db, err := sqlite.OpenDatabase(filepath.Join(hostDir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// migration will fail
	if err := migrate.Siad(context.Background(), db, hostDir, false, log.Named("migrate")); err == nil {
		t.Fatalf("migration should fail due to missing folder")
	}

	// move the folder back
	if err := os.Rename(newPath, oldPath); err != nil {
		t.Fatal(err)
	}

	dir, err := os.ReadDir(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, fi := range dir {
		log.Info(fi.Name())
	}

	// try the migration again, should succeed
	if err := migrate.Siad(context.Background(), db, hostDir, false, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}

	// start a hostd node using the renter's wallet key so it has existing funds
	hostNode, err := test.NewEmptyHost(renterKey, hostDir, renterNode, log.Named("hostd"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostNode.Close()

	settings, err := hostNode.RHP2Settings()
	if err != nil {
		t.Fatal(err)
	} else if settings.TotalStorage != 3*rhp2.SectorSize*64 {
		t.Fatalf("expected host to have %d total storage, got %d", 3*rhp2.SectorSize*64, settings.TotalStorage)
	} else if settings.AcceptingContracts != true {
		t.Fatal("expected host to be accepting contracts")
	} else if settings.MaxDuration != 100 {
		t.Fatalf("expected host to have max duration of 100, got %v", settings.MaxDuration)
	}

	expectedUsage := uint64(10 * 10) // 10 contracts * 10 sectors
	expectedTotal := uint64(3 * 64)  // 3 folder * 64 sectors

	usedSectors, totalSectors, err := hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != expectedUsage {
		t.Fatalf("expected host to have %d used sectors, got %v", expectedUsage, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err := hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != expectedUsage {
		t.Fatalf("expected host to have %d contract sectors, got %v", expectedUsage, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != expectedUsage {
		t.Fatalf("expected host to have %d physical sectors, got %v", expectedUsage, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 10 {
		t.Fatalf("expected host to have 10 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 0 {
		t.Fatalf("expected host to have 0 successful contracts, got %v", metrics.Contracts.Successful)
	}

	if hostNode.Store().HostKey().PublicKey() != hostKey {
		t.Fatal("host key mismatch")
	}

	for _, root := range activeSectors {
		data, err := hostNode.Storage().Read(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(data) != root {
			t.Fatalf("root mismatch: expected %v, got %v", root, rhp2.SectorRoot(data))
		}
	}

	var maxWindowEnd uint64
	for _, contractID := range activeContracts {
		fc, err := hostNode.Store().Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if fc.Revision.Filesize != 10*rhp2.SectorSize {
			t.Fatalf("expected contract to have 10 sectors, got %v", fc.Revision.Filesize/rhp2.SectorSize)
		}

		if fc.Revision.WindowEnd > maxWindowEnd {
			maxWindowEnd = fc.Revision.WindowEnd
		}
	}

	waitForSync := func() {
		// wait for the host node to sync
		for {
			if hostNode.Contracts().ScanHeight() == hostNode.ChainManager().TipState().Index.Height {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	waitForSync()

	// mine a block and wait for it to be processed
	for {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		waitForSync()

		if hostNode.Contracts().ScanHeight() >= maxWindowEnd+10 {
			break
		}
	}

	time.Sleep(time.Second)

	// check that all contracts are now successful
	c, _, err := hostNode.Store().Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	}

	for _, contract := range c {
		if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatalf("expected contract %v to be successful, got %v", contract.Revision.ParentID, contract.Status)
		}
	}

	// check that everything was cleaned up
	usedSectors, totalSectors, err = hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != 0 {
		t.Fatalf("expected host to have %d used sectors, got %v", 0, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err = hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != 0 {
		t.Fatalf("expected host to have %d contract sectors, got %v", 0, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != 0 {
		t.Fatalf("expected host to have %d physical sectors, got %v", 0, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 0 {
		t.Fatalf("expected host to have 0 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 10 {
		t.Fatalf("expected host to have 10 successful contracts, got %v", metrics.Contracts.Successful)
	}
}

func TestDestructiveMigrate(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostDir := t.TempDir()
	siad, err := startSiad(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	defer siad.Close()

	err = siad.Host().SetInternalSettings(modules.HostInternalSettings{
		AcceptingContracts: true,
		WindowSize:         10,
		MaxDuration:        100,
		MaxCollateral:      stypes.SiacoinPrecision.Mul64(10000),
		CollateralBudget:   stypes.SiacoinPrecision.Mul64(100000),
		MinStoragePrice:    stypes.NewCurrency64(1),
		Collateral:         stypes.NewCurrency64(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := siad.Host().AddStorageFolder(t.TempDir(), rhp2.SectorSize*64); err != nil {
			t.Fatal(err)
		}
	}

	hostKey := types.PublicKey(siad.Host().PublicKey().Key)
	hostUC, err := siad.Wallet().NextAddress()
	if err != nil {
		t.Fatal(err)
	}
	hostWalletAddr := hostUC.UnlockHash()
	hostAddr := string(siad.Host().ExternalSettings().NetAddress)

	// create a renter
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterNode, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renterNode.Close()

	renter, err := test.NewRenter(renterKey, t.TempDir(), renterNode, log.Named("renter"))
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()

	if err := siad.Gateway().Connect(modules.NetAddress(renterNode.GatewayAddr())); err != nil {
		t.Fatal(err)
	}

	miner := test.NewMiner(renterNode.ChainManager())
	if err := renterNode.ChainManager().Subscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	renterNode.TPool().Subscribe(miner)

	// mine enough blocks to fund both wallets
	if err := miner.Mine(types.Address(hostWalletAddr), 20); err != nil {
		t.Fatal(err)
	} else if err := miner.Mine(types.Address(renter.Wallet().Address()), 20); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	var activeContracts []types.FileContractID
	var activeSectors []types.Hash256

	// form a few contracts with the host and upload some data with them
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}
		activeContracts = append(activeContracts, revision.ID())

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
				activeSectors = append(activeSectors, root)
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	// shutdown the siad node
	if err := siad.Close(); err != nil {
		t.Fatal(err)
	}

	// migrate the host
	db, err := sqlite.OpenDatabase(filepath.Join(hostDir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := migrate.Siad(context.Background(), db, hostDir, true, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}

	// start a hostd node using the renter's wallet key so it has existing funds
	hostNode, err := test.NewEmptyHost(renterKey, hostDir, renterNode, log.Named("hostd"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostNode.Close()

	settings, err := hostNode.RHP2Settings()
	if err != nil {
		t.Fatal(err)
	} else if settings.TotalStorage != 3*rhp2.SectorSize*64 {
		t.Fatalf("expected host to have %d total storage, got %d", 3*rhp2.SectorSize*64, settings.TotalStorage)
	} else if settings.AcceptingContracts != true {
		t.Fatal("expected host to be accepting contracts")
	} else if settings.MaxDuration != 100 {
		t.Fatalf("expected host to have max duration of 100, got %v", settings.MaxDuration)
	}

	expectedUsage := uint64(10 * 10) // 10 contracts * 10 sectors
	expectedTotal := uint64(3 * 64)  // 3 folder * 64 sectors

	usedSectors, totalSectors, err := hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != expectedUsage {
		t.Fatalf("expected host to have %d used sectors, got %v", expectedUsage, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err := hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != expectedUsage {
		t.Fatalf("expected host to have %d contract sectors, got %v", expectedUsage, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != expectedUsage {
		t.Fatalf("expected host to have %d physical sectors, got %v", expectedUsage, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 10 {
		t.Fatalf("expected host to have 10 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 0 {
		t.Fatalf("expected host to have 0 successful contracts, got %v", metrics.Contracts.Successful)
	}

	if hostNode.Store().HostKey().PublicKey() != hostKey {
		t.Fatal("host key mismatch")
	}

	for _, root := range activeSectors {
		data, err := hostNode.Storage().Read(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(data) != root {
			t.Fatalf("root mismatch: expected %v, got %v", root, rhp2.SectorRoot(data))
		}
	}

	var maxWindowEnd uint64
	for _, contractID := range activeContracts {
		fc, err := hostNode.Store().Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if fc.Revision.Filesize != 10*rhp2.SectorSize {
			t.Fatalf("expected contract to have 10 sectors, got %v", fc.Revision.Filesize/rhp2.SectorSize)
		}

		if fc.Revision.WindowEnd > maxWindowEnd {
			maxWindowEnd = fc.Revision.WindowEnd
		}
	}

	waitForSync := func() {
		// wait for the host node to sync
		for {
			if hostNode.Contracts().ScanHeight() == hostNode.ChainManager().TipState().Index.Height {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	waitForSync()

	// mine a block and wait for it to be processed
	for {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		waitForSync()

		if hostNode.Contracts().ScanHeight() >= maxWindowEnd+10 {
			break
		}
	}

	time.Sleep(time.Second)

	// check that all contracts are now successful
	c, _, err := hostNode.Store().Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	}

	for _, contract := range c {
		if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatalf("expected contract %v to be successful, got %v", contract.Revision.ParentID, contract.Status)
		}
	}

	// check that everything was cleaned up
	usedSectors, totalSectors, err = hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != 0 {
		t.Fatalf("expected host to have %d used sectors, got %v", 0, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err = hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != 0 {
		t.Fatalf("expected host to have %d contract sectors, got %v", 0, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != 0 {
		t.Fatalf("expected host to have %d physical sectors, got %v", 0, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 0 {
		t.Fatalf("expected host to have 0 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 10 {
		t.Fatalf("expected host to have 10 successful contracts, got %v", metrics.Contracts.Successful)
	}
}

func TestDestructivePartialMigrate(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostDir := t.TempDir()
	siad, err := startSiad(hostDir)
	if err != nil {
		t.Fatal(err)
	}
	defer siad.Close()

	err = siad.Host().SetInternalSettings(modules.HostInternalSettings{
		AcceptingContracts: true,
		WindowSize:         10,
		MaxDuration:        100,
		MaxCollateral:      stypes.SiacoinPrecision.Mul64(10000),
		CollateralBudget:   stypes.SiacoinPrecision.Mul64(100000),
		MinStoragePrice:    stypes.NewCurrency64(1),
		Collateral:         stypes.NewCurrency64(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	var folderPaths []string
	for i := 0; i < 3; i++ {
		fp := t.TempDir()
		if err := siad.Host().AddStorageFolder(fp, rhp2.SectorSize*64); err != nil {
			t.Fatal(err)
		}
		folderPaths = append(folderPaths, fp)
	}

	hostKey := types.PublicKey(siad.Host().PublicKey().Key)
	hostUC, err := siad.Wallet().NextAddress()
	if err != nil {
		t.Fatal(err)
	}
	hostWalletAddr := hostUC.UnlockHash()
	hostAddr := string(siad.Host().ExternalSettings().NetAddress)

	// create a renter
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterNode, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renterNode.Close()

	renter, err := test.NewRenter(renterKey, t.TempDir(), renterNode, log.Named("renter"))
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()

	if err := siad.Gateway().Connect(modules.NetAddress(renterNode.GatewayAddr())); err != nil {
		t.Fatal(err)
	}

	miner := test.NewMiner(renterNode.ChainManager())
	if err := renterNode.ChainManager().Subscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	renterNode.TPool().Subscribe(miner)

	// mine enough blocks to fund both wallets
	if err := miner.Mine(types.Address(hostWalletAddr), 20); err != nil {
		t.Fatal(err)
	} else if err := miner.Mine(types.Address(renter.Wallet().Address()), 20); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	// form a few contracts with the host and upload some data with them
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// mine until the contracts have expired
	for i := 0; i < 50; i++ {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)
	}

	var activeContracts []types.FileContractID
	var activeSectors []types.Hash256

	// form additional contracts with the host and upload some data with them
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		revision, err := renter.FormContract(ctx, hostAddr, hostKey, types.Siacoins(1000), types.Siacoins(1000), 20)
		if err != nil {
			t.Fatal(err)
		}
		activeContracts = append(activeContracts, revision.ID())

		err = func() error {
			session, err := renter.NewRHP2Session(ctx, hostAddr, hostKey, revision.ID())
			if err != nil {
				return fmt.Errorf("failed to create rhp2 session: %w", err)
			}
			defer session.Close()

			var sector [rhp2.SectorSize]byte
			for j := 0; j < 10; j++ {
				frand.Read(sector[:])

				root := rhp2.SectorRoot(&sector)

				returnedRoot, err := session.Append(ctx, &sector, types.Siacoins(1), types.ZeroCurrency)
				if err != nil {
					return fmt.Errorf("failed to upload sector: %w", err)
				} else if root != returnedRoot {
					return fmt.Errorf("root mismatch: expected %v, got %v", root, returnedRoot)
				}
				activeSectors = append(activeSectors, root)
			}
			return nil
		}()
		if err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to confirm all the contracts
	if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// shutdown the siad node
	if err := siad.Close(); err != nil {
		t.Fatal(err)
	}

	// rename one of the storage folders so it can't be found
	oldPath := folderPaths[2]
	newPath := oldPath + "missing"
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}

	// migrate the host
	db, err := sqlite.OpenDatabase(filepath.Join(hostDir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// migration will fail
	if err := migrate.Siad(context.Background(), db, hostDir, true, log.Named("migrate")); err == nil {
		t.Fatalf("migration should fail due to missing folder")
	}

	// move the folder back
	if err := os.Rename(newPath, oldPath); err != nil {
		t.Fatal(err)
	}

	dir, err := os.ReadDir(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, fi := range dir {
		log.Info(fi.Name())
	}

	// try the migration again, should succeed
	if err := migrate.Siad(context.Background(), db, hostDir, true, log.Named("migrate")); err != nil {
		t.Fatal(err)
	}

	// start a hostd node using the renter's wallet key so it has existing funds
	hostNode, err := test.NewEmptyHost(renterKey, hostDir, renterNode, log.Named("hostd"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostNode.Close()

	settings, err := hostNode.RHP2Settings()
	if err != nil {
		t.Fatal(err)
	} else if settings.TotalStorage != 3*rhp2.SectorSize*64 {
		t.Fatalf("expected host to have %d total storage, got %d", 3*rhp2.SectorSize*64, settings.TotalStorage)
	} else if settings.AcceptingContracts != true {
		t.Fatal("expected host to be accepting contracts")
	} else if settings.MaxDuration != 100 {
		t.Fatalf("expected host to have max duration of 100, got %v", settings.MaxDuration)
	}

	expectedUsage := uint64(10 * 10) // 10 contracts * 10 sectors
	expectedTotal := uint64(3 * 64)  // 3 folder * 64 sectors

	usedSectors, totalSectors, err := hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != expectedUsage {
		t.Fatalf("expected host to have %d used sectors, got %v", expectedUsage, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err := hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != expectedUsage {
		t.Fatalf("expected host to have %d contract sectors, got %v", expectedUsage, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != expectedUsage {
		t.Fatalf("expected host to have %d physical sectors, got %v", expectedUsage, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 10 {
		t.Fatalf("expected host to have 10 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 0 {
		t.Fatalf("expected host to have 0 successful contracts, got %v", metrics.Contracts.Successful)
	}

	if hostNode.Store().HostKey().PublicKey() != hostKey {
		t.Fatal("host key mismatch")
	}

	for _, root := range activeSectors {
		data, err := hostNode.Storage().Read(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(data) != root {
			t.Fatalf("root mismatch: expected %v, got %v", root, rhp2.SectorRoot(data))
		}
	}

	var maxWindowEnd uint64
	for _, contractID := range activeContracts {
		fc, err := hostNode.Store().Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if fc.Revision.Filesize != 10*rhp2.SectorSize {
			t.Fatalf("expected contract to have 10 sectors, got %v", fc.Revision.Filesize/rhp2.SectorSize)
		}

		if fc.Revision.WindowEnd > maxWindowEnd {
			maxWindowEnd = fc.Revision.WindowEnd
		}
	}

	waitForSync := func() {
		// wait for the host node to sync
		for {
			if hostNode.Contracts().ScanHeight() == hostNode.ChainManager().TipState().Index.Height {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	waitForSync()

	// mine a block and wait for it to be processed
	for {
		if err := miner.Mine(types.Address(renter.Wallet().Address()), 1); err != nil {
			t.Fatal(err)
		}

		waitForSync()

		if hostNode.Contracts().ScanHeight() >= maxWindowEnd+10 {
			break
		}
	}

	time.Sleep(time.Second)

	// check that all contracts are now successful
	c, _, err := hostNode.Store().Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	}

	for _, contract := range c {
		if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatalf("expected contract %v to be successful, got %v", contract.Revision.ParentID, contract.Status)
		}
	}

	// check that everything was cleaned up
	usedSectors, totalSectors, err = hostNode.Storage().Usage()
	if err != nil {
		t.Fatal(err)
	} else if usedSectors != 0 {
		t.Fatalf("expected host to have %d used sectors, got %v", 0, usedSectors)
	} else if totalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, totalSectors)
	}

	// check the host's metrics
	metrics, err = hostNode.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if metrics.Storage.ContractSectors != 0 {
		t.Fatalf("expected host to have %d contract sectors, got %v", 0, metrics.Storage.ContractSectors)
	} else if metrics.Storage.TotalSectors != expectedTotal {
		t.Fatalf("expected host to have %d total sectors, got %v", expectedTotal, metrics.Storage.TotalSectors)
	} else if metrics.Storage.PhysicalSectors != 0 {
		t.Fatalf("expected host to have %d physical sectors, got %v", 0, metrics.Storage.PhysicalSectors)
	} else if metrics.Contracts.Active != 0 {
		t.Fatalf("expected host to have 0 active contracts, got %v", metrics.Contracts.Active)
	} else if metrics.Contracts.Pending != 0 {
		t.Fatalf("expected host to have 0 total contracts, got %v", metrics.Contracts.Pending)
	} else if metrics.Contracts.Successful != 10 {
		t.Fatalf("expected host to have 10 successful contracts, got %v", metrics.Contracts.Successful)
	}
}
