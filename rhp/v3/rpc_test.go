package rhp_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	crhp2 "go.sia.tech/core/rhp/v2"
	crhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/testutil"
	proto2 "go.sia.tech/hostd/internal/testutil/rhp/v2"
	proto3 "go.sia.tech/hostd/internal/testutil/rhp/v3"
	"go.sia.tech/hostd/rhp"
	rhp2 "go.sia.tech/hostd/rhp/v2"
	rhp3 "go.sia.tech/hostd/rhp/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func formContract(t *testing.T, cm *chain.Manager, wm *wallet.SingleAddressWallet, hostAddr string, renterKey types.PrivateKey, hostKey types.PublicKey, duration uint64) crhp2.ContractRevision {
	t.Helper()

	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		t.Fatal("failed to dial host", err)
	}
	defer conn.Close()

	transport, err := crhp2.NewRenterTransport(conn, hostKey)
	if err != nil {
		t.Fatal("failed to create transport", err)
	}
	defer transport.Close()

	settings, err := proto2.RPCSettings(transport)
	if err != nil {
		t.Fatal("failed to get settings", err)
	}

	fc := crhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey, types.Siacoins(1000), types.Siacoins(1000), cm.Tip().Height+duration, settings, wm.Address())
	formationCost := crhp2.ContractFormationCost(cm.TipState(), fc, settings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	toSign, err := wm.FundTransaction(&txn, formationCost, true)
	if err != nil {
		t.Fatal("failed to fund formation txn:", err)
	}
	wm.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))
	formationSet := append(cm.UnconfirmedParents(txn), txn)

	revision, _, err := proto2.RPCFormContract(transport, renterKey, formationSet)
	if err != nil {
		t.Fatal("failed to form contract:", err)
	}
	return revision
}

func setupRHP3Host(t *testing.T, node *testutil.HostNode, hostKey types.PrivateKey, maxStorage uint64, log *zap.Logger) (*rhp2.SessionHandler, *rhp3.SessionHandler) {
	// start the RHP2 listener for forming contracts
	rhp2Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rhp2Listener.Close() })

	// start the RHP3 listener for the actual test
	rhp3Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rhp3Listener.Close() })

	// set the host to accept contracts
	s := node.Settings.Settings()
	s.AcceptingContracts = true
	s.MaxCollateral = types.Siacoins(100000)
	s.MaxAccountBalance = types.Siacoins(100000)
	s.StoragePrice = types.NewCurrency64(1)
	s.ContractPrice = types.NewCurrency64(1)
	s.EgressPrice = types.NewCurrency64(1)
	s.IngressPrice = types.NewCurrency64(1)
	s.BaseRPCPrice = types.NewCurrency64(1)
	s.NetAddress = rhp3Listener.Addr().String()
	if err := node.Settings.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}

	// initialize a storage volume
	res := make(chan error)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "storage.dat"), maxStorage, res); err != nil {
		t.Fatal(err)
	} else if err := <-res; err != nil {
		t.Fatal(err)
	}

	sh2 := rhp2.NewSessionHandler(rhp2Listener, hostKey, node.Chain, node.Syncer, node.Wallet, node.Contracts, node.Settings, node.Volumes, log.Named("rhp2"))
	t.Cleanup(func() { sh2.Close() })
	go sh2.Serve()

	sh3 := rhp3.NewSessionHandler(rhp3Listener, hostKey, node.Chain, node.Syncer, node.Wallet, node.Accounts, node.Contracts, node.Registry, node.Volumes, node.Settings, log.Named("rhp3"))
	t.Cleanup(func() { sh3.Close() })
	go sh3.Serve()

	return sh2, sh3
}

func TestPriceTable(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	pt, err := node.Settings.RHP3PriceTable()
	if err != nil {
		t.Fatal(err)
	}

	retrieved, err := session.ScanPriceTable()
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	// check that the price tables match
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}

	// form a contract
	revision := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

	account := crhp3.Account(renterKey.PublicKey())
	payment := proto3.ContractPayment(&revision, renterKey, account)

	retrieved, err = session.RegisterPriceTable(payment)
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}

	// fund an account
	_, err = session.FundAccount(account, payment, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}

	payment = proto3.AccountPayment(account, renterKey)
	// pay for a price table using an account
	retrieved, err = session.RegisterPriceTable(payment)
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}
}

func TestAppendSector(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// form a contract to upload sectors
	revision := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

	// register the price table
	account := crhp3.Account(renterKey.PublicKey())
	payment := proto3.ContractPayment(&revision, renterKey, account)
	pt, err := session.RegisterPriceTable(payment)
	if err != nil {
		t.Fatal(err)
	}

	// fund an account
	_, err = session.FundAccount(account, payment, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}

	var roots []types.Hash256
	for i := 0; i < 10; i++ {
		// calculate the cost of the upload
		cost, _ := pt.BaseCost().Add(pt.AppendSectorCost(revision.Revision.WindowEnd - node.Chain.Tip().Height)).Total()
		if cost.IsZero() {
			t.Fatal("cost is zero")
		}
		var sector [crhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := crhp2.SectorRoot(&sector)
		roots = append(roots, root)

		if _, err = session.AppendSector(&sector, &revision, renterKey, payment, cost); err != nil {
			t.Fatal(err)
		}

		// check that the contract merkle root matches
		if revision.Revision.FileMerkleRoot != crhp2.MetaRoot(roots) {
			t.Fatal("contract merkle root doesn't match")
		}

		// download the sector
		cost, _ = pt.BaseCost().Add(pt.ReadSectorCost(crhp2.SectorSize)).Total()
		downloaded, _, err := session.ReadSector(root, 0, crhp2.SectorSize, payment, cost)
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(downloaded, sector[:]) {
			t.Fatal("downloaded sector doesn't match")
		}
	}

	// assert ReadSector exposes ErrSectorNotFound
	cost, _ := pt.BaseCost().Add(pt.ReadSectorCost(crhp2.SectorSize)).Total()
	_, _, err = session.ReadSector(types.Hash256{}, 0, crhp2.SectorSize, payment, cost)
	if err == nil {
		t.Fatal("expected error when reading nil sector")
	} else if !strings.Contains(err.Error(), storage.ErrSectorNotFound.Error()) {
		t.Fatal("expected storage.ErrSectorNotFound", err)
	}
}

func TestStoreSector(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// form a contract to upload sectors
	revision := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

	account := crhp3.Account(renterKey.PublicKey())
	// register the price table
	payment := proto3.ContractPayment(&revision, renterKey, account)
	pt, err := session.RegisterPriceTable(payment)
	if err != nil {
		t.Fatal(err)
	}

	// fund an account
	_, err = session.FundAccount(account, payment, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}

	// upload a sector
	payment = proto3.AccountPayment(account, renterKey)
	// calculate the cost of the upload
	usage := pt.StoreSectorCost(10)
	cost, _ := usage.Total()
	var sector [crhp2.SectorSize]byte
	frand.Read(sector[:256])
	root := crhp2.SectorRoot(&sector)
	if err = session.StoreSector(&sector, 10, payment, cost); err != nil {
		t.Fatal(err)
	}

	// download the sector
	usage = pt.ReadSectorCost(crhp2.SectorSize)
	cost, _ = usage.Total()
	downloaded, _, err := session.ReadSector(root, 0, crhp2.SectorSize, payment, cost)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(downloaded, sector[:]) {
		t.Fatal("downloaded sector doesn't match")
	}

	// mine until the sector expires
	testutil.MineAndSync(t, node, node.Wallet.Address(), 10)
	// ensure the dereferenced sector has been pruned
	if err := node.Store.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// check that the sector was deleted
	usage = pt.ReadSectorCost(crhp2.SectorSize)
	cost, _ = usage.Total()
	_, _, err = session.ReadSector(root, 0, crhp2.SectorSize, payment, cost)
	if err == nil {
		t.Fatal("expected error when reading sector")
	}
}

func TestReadSectorOffset(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// form a contract to upload sectors
	revision := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

	account := crhp3.Account(renterKey.PublicKey())
	payment := proto3.ContractPayment(&revision, renterKey, account)
	// register the price table
	pt, err := session.RegisterPriceTable(payment)
	if err != nil {
		t.Fatal(err)
	}

	// fund an account
	_, err = session.FundAccount(account, payment, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}

	cost, _ := pt.BaseCost().Add(pt.AppendSectorCost(revision.Revision.WindowEnd - node.Chain.Tip().Height)).Total()
	var sectors [][crhp2.SectorSize]byte
	for i := 0; i < 5; i++ {
		// upload a few sectors
		payment = proto3.AccountPayment(account, renterKey)
		// calculate the cost of the upload
		if cost.IsZero() {
			t.Fatal("cost is zero")
		}
		var sector [crhp2.SectorSize]byte
		frand.Read(sector[:256])
		_, err = session.AppendSector(&sector, &revision, renterKey, payment, cost)
		if err != nil {
			t.Fatal(err)
		}
		sectors = append(sectors, sector)
	}

	// download the sector
	cost, _ = pt.BaseCost().Add(pt.ReadOffsetCost(256)).Total()
	downloaded, _, err := session.ReadOffset(crhp2.SectorSize*3+64, 256, revision.ID(), payment, cost)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(downloaded, sectors[3][64:64+256]) {
		t.Fatal("downloaded sector doesn't match")
	}
}

func TestRenew(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	account := crhp3.Account(renterKey.PublicKey())

	assertContractUsage := func(t *testing.T, id types.FileContractID, lockedCollateral types.Currency, expected contracts.Usage) {
		t.Helper()

		contract, err := node.Contracts.Contract(id)
		if err != nil {
			t.Fatal(err)
		} else if !contract.LockedCollateral.Equals(lockedCollateral) {
			t.Fatalf("expected locked collateral %v, got %v", lockedCollateral, contract.LockedCollateral)
		}

		uv := reflect.ValueOf(&contract.Usage).Elem()
		ev := reflect.ValueOf(&expected).Elem()

		for i := 0; i < uv.NumField(); i++ {
			va := ev.Field(i).Interface().(types.Currency)
			vb := uv.Field(i).Interface().(types.Currency)
			if !va.Equals(vb) {
				t.Fatalf("field %v: expected %v, got %v", uv.Type().Field(i).Name, va, vb)
			}
		}
	}

	assertRenewal := func(t *testing.T, parentID types.FileContractID, renewal crhp2.ContractRevision, expectedRevisionNumber uint64, expectedRoot types.Hash256, expectedFilesize uint64) {
		t.Helper()

		if renewal.Revision.FileMerkleRoot != expectedRoot {
			t.Fatalf("expected root %v, got %v", expectedRoot, renewal.Revision.FileMerkleRoot)
		} else if renewal.Revision.Filesize != expectedFilesize {
			t.Fatalf("expected filesize %d, got %d", expectedFilesize, renewal.Revision.Filesize)
		} else if renewal.Revision.RevisionNumber != expectedRevisionNumber {
			t.Fatalf("expected revision number %d, got %d", expectedRevisionNumber, renewal.Revision.RevisionNumber)
		}

		sigHash := rhp.HashRevision(renewal.Revision)
		if !hostKey.PublicKey().VerifyHash(sigHash, types.Signature(renewal.Signatures[1].Signature)) {
			t.Fatal("host signature invalid")
		} else if !renterKey.PublicKey().VerifyHash(sigHash, types.Signature(renewal.Signatures[0].Signature)) {
			t.Fatal("renter signature invalid")
		}

		old, err := node.Contracts.Contract(parentID)
		if err != nil {
			t.Fatal(err)
		} else if old.Revision.Filesize != 0 {
			t.Fatal("filesize mismatch")
		} else if old.Revision.FileMerkleRoot != (types.Hash256{}) {
			t.Fatal("merkle root mismatch")
		} else if old.RenewedTo != renewal.ID() {
			t.Fatal("renewed to mismatch")
		}

		renewed, err := node.Contracts.Contract(renewal.ID())
		if err != nil {
			t.Fatal(err)
		} else if renewed.Revision.Filesize != expectedFilesize {
			t.Fatal("filesize mismatch")
		} else if renewed.Revision.FileMerkleRoot != expectedRoot {
			t.Fatal("merkle root mismatch")
		} else if renewed.RenewedFrom != parentID {
			t.Fatalf("expected renewed from %s, got %s", parentID, renewed.RenewedFrom)
		}
	}

	t.Run("empty contract", func(t *testing.T) {
		// form a contract
		origin := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)

		payment := proto3.ContractPayment(&origin, renterKey, account)

		// register a price table to use for the renewal
		pt, err := session.RegisterPriceTable(payment)
		if err != nil {
			t.Fatal(err)
		}

		renewHeight := origin.Revision.WindowEnd + 10
		renterFunds := types.Siacoins(10)
		additionalCollateral := types.Siacoins(20)
		renewal, _, err := session.RenewContract(&origin, node.Wallet.Address(), renterKey, renterFunds, additionalCollateral, renewHeight)
		if err != nil {
			t.Fatal(err)
		}

		// mine a block to confirm the revision
		testutil.MineAndSync(t, node, node.Wallet.Address(), 1)

		assertRenewal(t, origin.ID(), renewal, 1, origin.Revision.FileMerkleRoot, origin.Revision.Filesize)
		assertContractUsage(t, renewal.ID(), additionalCollateral, contracts.Usage{
			RPCRevenue:     pt.ContractPrice,
			StorageRevenue: pt.RenewContractCost, // renew contract cost is included because it is burned on failure
		})
	})

	t.Run("drained contract", func(t *testing.T) {
		// form a contract
		origin := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 200)

		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)

		payment := proto3.ContractPayment(&origin, renterKey, account)

		// register a price table to use for the renewal
		pt, err := session.RegisterPriceTable(payment)
		if err != nil {
			t.Fatal(err)
		}

		// upload a sector
		var remainingDuration uint64
		contractExpiration := uint64(origin.Revision.WindowEnd)
		currentHeight := node.Chain.Tip().Height
		if contractExpiration < currentHeight {
			t.Fatal("contract expired")
		}

		// generate a sector
		var sector [crhp2.SectorSize]byte
		frand.Read(sector[:256])

		remainingDuration = contractExpiration - currentHeight
		usage := pt.BaseCost().Add(pt.AppendSectorCost(remainingDuration))
		cost, _ := usage.Total()
		if _, err := session.AppendSector(&sector, &origin, renterKey, payment, cost); err != nil {
			t.Fatal(err)
		}

		// fund the account leaving no funds for the renewal
		if _, err := session.FundAccount(account, payment, origin.Revision.ValidRenterPayout().Sub(pt.FundAccountCost)); err != nil {
			t.Fatal(err)
		}

		// mine a few blocks into the contract
		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)

		renewHeight := origin.Revision.WindowEnd + 10
		renterFunds := types.Siacoins(10)
		additionalCollateral := types.Siacoins(20)
		renewal, _, err := session.RenewContract(&origin, node.Wallet.Address(), renterKey, renterFunds, additionalCollateral, renewHeight)
		if err != nil {
			t.Fatal(err)
		}

		extension := renewal.Revision.WindowEnd - origin.Revision.WindowEnd
		baseRiskedCollateral := pt.CollateralCost.Mul64(extension).Mul64(origin.Revision.Filesize)
		assertContractUsage(t, renewal.Revision.ParentID, additionalCollateral.Add(baseRiskedCollateral), contracts.Usage{
			RPCRevenue:       pt.ContractPrice,
			RiskedCollateral: baseRiskedCollateral,
			StorageRevenue:   pt.RenewContractCost.Add(pt.WriteStoreCost.Mul64(origin.Revision.Filesize).Mul64(extension)), // renew contract cost is included because it is burned on failure
		})
		assertRenewal(t, origin.ID(), renewal, 1, origin.Revision.FileMerkleRoot, origin.Revision.Filesize)
	})
}

func TestRPCV2(t *testing.T) {
	log := zaptest.NewLogger(t)
	renterKey, hostKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V2Network()
	network.HardforkV2.AllowHeight = 180
	network.HardforkV2.RequireHeight = 200
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// set the host to accept contracts
	s := node.Settings.Settings()
	s.AcceptingContracts = true
	s.NetAddress = "localhost:9983"
	if err := node.Settings.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}

	// initialize a storage volume
	res := make(chan error)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "storage.dat"), 10, res); err != nil {
		t.Fatal(err)
	} else if err := <-res; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// start the node
	sh2, sh3 := setupRHP3Host(t, node, hostKey, 10, log)

	// form a contract that expires before the hardfork
	origin := formContract(t, node.Chain, node.Wallet, sh2.LocalAddr(), renterKey, hostKey.PublicKey(), 20)
	// mine a block to confirm the contract
	testutil.MineAndSync(t, node, node.Wallet.Address(), 1)

	// create a RHP3 session
	session, err := proto3.NewSession(context.Background(), hostKey.PublicKey(), sh3.LocalAddr(), node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// try to renew the contract with an ending after the hardfork
	renewHeight := network.HardforkV2.RequireHeight
	renterFunds := types.Siacoins(10)
	additionalCollateral := types.Siacoins(20)
	_, _, err = session.RenewContract(&origin, node.Wallet.Address(), renterKey, renterFunds, additionalCollateral, renewHeight)
	if !errors.Is(err, rhp3.ErrAfterV2Hardfork) {
		t.Fatalf("expected after v2 hardfork error, got %v", err)
	}
}
