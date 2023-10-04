package contracts_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

const sectorCacheSize = 64

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func formContract(renterKey, hostKey types.PrivateKey, start, end uint64, renterPayout, hostPayout types.Currency, c *contracts.ContractManager, w contracts.Wallet, cm contracts.ChainManager, tp contracts.TransactionPool) (contracts.SignedRevision, error) {
	contract := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), renterPayout, hostPayout, start, rhp2.HostSettings{WindowSize: end - start}, w.Address())
	state := cm.TipState()
	formationCost := rhp2.ContractFormationCost(state, contract, types.ZeroCurrency)
	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	txn := types.Transaction{
		FileContracts: []types.FileContract{contract},
	}
	toSign, discard, err := w.FundTransaction(&txn, formationCost.Add(hostPayout)) // we're funding both sides of the payout
	if err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer discard()
	if err := w.SignTransaction(state, &txn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to sign transaction: %w", err)
	} else if err := tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to accept transaction set: %w", err)
	}
	revision := types.FileContractRevision{
		ParentID:         txn.FileContractID(0),
		UnlockConditions: contractUnlockConditions,
		FileContract:     txn.FileContracts[0],
	}
	revision.RevisionNumber = 1
	sigHash := hashRevision(revision)
	rev := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostKey.SignHash(sigHash),
		RenterSignature: renterKey.SignHash(sigHash),
	}

	if err := c.AddContract(rev, []types.Transaction{}, hostPayout, contracts.Usage{}); err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to add contract: %w", err)
	}
	return rev, nil
}

func TestContractLockUnlock(t *testing.T) {
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()

	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node, err := test.NewWallet(hostKey, t.TempDir(), log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	am := alerts.NewManager()
	s, err := storage.NewVolumeManager(db, am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c, err := contracts.NewManager(db, am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	rev := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			FileContract: types.FileContract{
				UnlockHash:  types.Hash256(contractUnlockConditions.UnlockHash()),
				WindowStart: 100,
				WindowEnd:   200,
			},
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
		},
	}

	if err := c.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Lock(context.Background(), rev.Revision.ParentID); err != nil {
		t.Fatal(err)
	}

	err = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.Lock(ctx, rev.Revision.ParentID)
		return err
	}()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected context deadline exceeded, got", err)
	}

	c.Unlock(rev.Revision.ParentID)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Lock(context.Background(), rev.Revision.ParentID); err != nil {
				t.Error(err)
			}
			time.Sleep(100 * time.Millisecond)
			c.Unlock(rev.Revision.ParentID)
		}()
	}
	wg.Wait()
}

func TestContractLifecycle(t *testing.T) {
	t.Run("successful with proof", func(t *testing.T) {
		hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

		dir := t.TempDir()
		log := zaptest.NewLogger(t)
		node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()

		am := alerts.NewManager()
		s, err := storage.NewVolumeManager(node.Store(), am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		result := make(chan error, 1)
		if _, err := s.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		c, err := contracts.NewManager(node.Store(), am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// note: many more blocks than necessary are mined to ensure all forks have activated
		if err := node.MineBlocks(node.Address(), int(stypes.MaturityDelay*4)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev, err := formContract(renterKey, hostKey, 50, 60, renterFunds, hostCollateral, c, node, node.ChainManager(), node.TPool())
		if err != nil {
			t.Fatal(err)
		}

		contract, err := c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusPending {
			t.Fatal("expected contract to be pending")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 1 {
			t.Fatal("expected 1 pending contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 0 {
			t.Fatal("expected 0 pending contracts")
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		var roots []types.Hash256
		for i := 0; i < 5; i++ {
			var sector [rhp2.SectorSize]byte
			frand.Read(sector[:256])
			root := rhp2.SectorRoot(&sector)
			release, err := s.Write(root, &sector)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			roots = append(roots, root)
		}

		// create a revision that adds sectors and transfers funds to the host
		amount := types.NewCurrency64(100)
		collateral := types.NewCurrency64(200)
		rev.Revision.RevisionNumber++
		rev.Revision.Filesize = rhp2.SectorSize * uint64(len(roots))
		rev.Revision.FileMerkleRoot = rhp2.MetaRoot(roots)
		rev.Revision.ValidProofOutputs[0].Value = rev.Revision.ValidProofOutputs[0].Value.Sub(amount)
		rev.Revision.ValidProofOutputs[1].Value = rev.Revision.ValidProofOutputs[1].Value.Add(amount)
		rev.Revision.MissedProofOutputs[0].Value = rev.Revision.MissedProofOutputs[0].Value.Sub(amount)
		rev.Revision.MissedProofOutputs[1].Value = rev.Revision.MissedProofOutputs[1].Value.Sub(collateral)
		rev.Revision.MissedProofOutputs[2].Value = rev.Revision.MissedProofOutputs[2].Value.Add(collateral.Add(amount))
		sigHash := hashRevision(rev.Revision)
		rev.HostSignature = hostKey.SignHash(sigHash)
		rev.RenterSignature = renterKey.SignHash(sigHash)

		updater, err := c.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		for _, root := range roots {
			updater.AppendSector(root)
		}

		err = updater.Commit(rev, contracts.Usage{
			StorageRevenue:   amount,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(collateral) {
			t.Fatalf("expected %v risked collateral, got %v", collateral, m.Contracts.RiskedCollateral)
		}

		// mine until the revision is broadcast
		remainingBlocks := rev.Revision.WindowStart - node.TipState().Index.Height - contracts.RevisionSubmissionBuffer
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time
		// confirm the revision
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until the proof window
		remainingBlocks = rev.Revision.WindowStart - node.TipState().Index.Height
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time
		// confirm the proof
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time
		proofHeight := rev.Revision.WindowStart + 1

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if contract.ResolutionHeight != proofHeight {
			t.Fatalf("expected resolution height %v, got %v", proofHeight, contract.ResolutionHeight)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contracts")
		} else if m.Contracts.Successful != 0 {
			t.Fatal("expected 0 successful contract")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(collateral) {
			t.Fatalf("expected %v risked collateral, got %v", collateral, m.Contracts.RiskedCollateral)
		}

		// mine until the end of the proof window
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height + 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// check that the contract was marked successful
		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatal("expected contract to be successful")
		} else if contract.ResolutionHeight != proofHeight {
			t.Fatalf("expected resolution height %v, got %v", proofHeight, contract.ResolutionHeight)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 0 {
			t.Fatal("expected 0 active contracts")
		} else if m.Contracts.Successful != 1 {
			t.Fatal("expected 1 successful contract")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.IsZero() {
			t.Fatalf("expected %v locked collateral, got %v", types.ZeroCurrency, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected %v risked collateral, got %v", types.ZeroCurrency, m.Contracts.RiskedCollateral)
		}
	})

	t.Run("successful no proof", func(t *testing.T) {
		hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

		dir := t.TempDir()
		log := zaptest.NewLogger(t)
		node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()

		am := alerts.NewManager()
		s, err := storage.NewVolumeManager(node.Store(), am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		result := make(chan error, 1)
		if _, err := s.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		c, err := contracts.NewManager(node.Store(), am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// note: many more blocks than necessary are mined to ensure all forks have activated
		if err := node.MineBlocks(node.Address(), int(stypes.MaturityDelay*4)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev, err := formContract(renterKey, hostKey, 50, 60, renterFunds, hostCollateral, c, node, node.ChainManager(), node.TPool())
		if err != nil {
			t.Fatal(err)
		}

		contract, err := c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusPending {
			t.Fatal("expected contract to be pending")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 1 {
			t.Fatal("expected 1 pending contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 0 {
			t.Fatal("expected 0 pending contracts")
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		// create a revision that transfers funds to the host, simulating
		// account funding
		amount := types.NewCurrency64(100)
		rev.Revision.RevisionNumber += 10
		rev.Revision.ValidProofOutputs[0].Value = rev.Revision.ValidProofOutputs[0].Value.Sub(amount)
		rev.Revision.ValidProofOutputs[1].Value = rev.Revision.ValidProofOutputs[1].Value.Add(amount)
		rev.Revision.MissedProofOutputs[0].Value = rev.Revision.MissedProofOutputs[0].Value.Sub(amount)
		rev.Revision.MissedProofOutputs[1].Value = rev.Revision.MissedProofOutputs[1].Value.Add(amount)
		sigHash := hashRevision(rev.Revision)
		rev.HostSignature = hostKey.SignHash(sigHash)
		rev.RenterSignature = renterKey.SignHash(sigHash)

		updater, err := c.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		err = updater.Commit(rev, contracts.Usage{
			AccountFunding: amount,
		})
		if err != nil {
			t.Fatal(err)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(types.ZeroCurrency) {
			t.Fatalf("expected %v risked collateral, got %v", types.ZeroCurrency, m.Contracts.RiskedCollateral)
		}

		// mine until the revision is broadcast
		remainingBlocks := rev.Revision.WindowStart - node.TipState().Index.Height - contracts.RevisionSubmissionBuffer
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time
		// confirm the revision
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until the end of the proof window -- contract should still be
		// active since no proof is required.
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height - 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 0 {
			t.Fatal("expected 0 pending contracts")
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		// mine until after the proof window -- contract should be successful
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height + 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatal("expected contract to be successful")
		} else if contract.ResolutionHeight != 0 {
			t.Fatalf("expected resolution height %v, got %v", 0, contract.ResolutionHeight)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 0 {
			t.Fatal("expected 0 active contracts")
		} else if m.Contracts.Successful != 1 {
			t.Fatal("expected 1 successful contract")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.IsZero() {
			t.Fatalf("expected %v locked collateral, got %v", types.ZeroCurrency, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected %v risked collateral, got %v", types.ZeroCurrency, m.Contracts.RiskedCollateral)
		}
	})

	t.Run("0 filesize contract", func(t *testing.T) {
		hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

		dir := t.TempDir()
		log := zaptest.NewLogger(t)
		node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()

		am := alerts.NewManager()
		s, err := storage.NewVolumeManager(node.Store(), am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		result := make(chan error, 1)
		if _, err := s.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		c, err := contracts.NewManager(node.Store(), am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// note: mine enough blocks to ensure all forks have activated
		if err := node.MineBlocks(node.Address(), int(stypes.MaturityDelay*4)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		rev, err := formContract(renterKey, hostKey, 50, 60, types.Siacoins(500), types.Siacoins(1000), c, node, node.ChainManager(), node.TPool())
		if err != nil {
			t.Fatal(err)
		}

		contract, err := c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusPending {
			t.Fatal("expected contract to be pending")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 1 {
			t.Fatal("expected 1 pending contract")
		}

		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 0 {
			t.Fatal("expected 0 pending contracts")
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contract")
		}

		// create a revision that adds sectors and transfers funds to the host
		amount := types.NewCurrency64(100)
		rev.Revision.RevisionNumber++
		rev.Revision.ValidProofOutputs[0].Value = rev.Revision.ValidProofOutputs[0].Value.Sub(amount)
		rev.Revision.ValidProofOutputs[1].Value = rev.Revision.ValidProofOutputs[1].Value.Add(amount)
		sigHash := hashRevision(rev.Revision)
		rev.HostSignature = hostKey.SignHash(sigHash)
		rev.RenterSignature = renterKey.SignHash(sigHash)

		updater, err := c.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		if err := updater.Commit(rev, contracts.Usage{}); err != nil {
			t.Fatal(err)
		}

		// mine until the revision is broadcast
		remainingBlocks := rev.Revision.WindowStart - node.TipState().Index.Height - contracts.RevisionSubmissionBuffer
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time
		// confirm the revision
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until the proof window
		remainingBlocks = rev.Revision.WindowStart - node.TipState().Index.Height + 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time
		// confirm the proof
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatalf("expected contract to be active, got %v", contract.Status)
		} else if contract.ResolutionHeight == 0 {
			t.Fatal("expected contract to have resolution")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contracts")
		} else if m.Contracts.Successful != 0 {
			t.Fatal("expected 0 successful contracts")
		}

		// mine until the proof window ends -- contract should be successful
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height + 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatalf("expected contract to be active, got %v", contract.Status)
		} else if contract.ResolutionHeight == 0 {
			t.Fatal("expected contract to have resolution")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 0 {
			t.Fatal("expected 0 active contracts")
		} else if m.Contracts.Successful != 1 {
			t.Fatal("expected 1 successful contracts")
		}
	})

	t.Run("failed corrupt sector", func(t *testing.T) {
		hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

		dir := t.TempDir()
		log := zaptest.NewLogger(t)
		node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()

		am := alerts.NewManager()
		s, err := storage.NewVolumeManager(node.Store(), am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		result := make(chan error, 1)
		if _, err := s.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		c, err := contracts.NewManager(node.Store(), am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// note: many more blocks than necessary are mined to ensure all forks have activated
		if err := node.MineBlocks(node.Address(), int(stypes.MaturityDelay*4)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev, err := formContract(renterKey, hostKey, 50, 60, renterFunds, hostCollateral, c, node, node.ChainManager(), node.TPool())
		if err != nil {
			t.Fatal(err)
		}

		contract, err := c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusPending {
			t.Fatal("expected contract to be pending")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 1 {
			t.Fatal("expected 1 pending contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Pending != 0 {
			t.Fatal("expected 0 pending contracts")
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contract")
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected 0 risked collateral, got %v", m.Contracts.RiskedCollateral)
		}

		var roots []types.Hash256
		for i := 0; i < 5; i++ {
			var sector [rhp2.SectorSize]byte
			frand.Read(sector[:256])
			root := rhp2.SectorRoot(&sector)
			release, err := s.Write(root, &sector)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			roots = append(roots, root)
		}

		// create a revision that adds sectors and transfers funds to the host
		amount := types.NewCurrency64(100)
		collateral := types.NewCurrency64(200)
		rev.Revision.RevisionNumber++
		rev.Revision.Filesize = rhp2.SectorSize * uint64(len(roots))
		rev.Revision.FileMerkleRoot = frand.Entropy256() // corrupt the file merkle root so the blockchain rejects the proof
		rev.Revision.ValidProofOutputs[0].Value = rev.Revision.ValidProofOutputs[0].Value.Sub(amount)
		rev.Revision.ValidProofOutputs[1].Value = rev.Revision.ValidProofOutputs[1].Value.Add(amount)
		rev.Revision.MissedProofOutputs[0].Value = rev.Revision.MissedProofOutputs[0].Value.Sub(amount)
		rev.Revision.MissedProofOutputs[1].Value = rev.Revision.MissedProofOutputs[1].Value.Sub(collateral)
		rev.Revision.MissedProofOutputs[2].Value = rev.Revision.MissedProofOutputs[2].Value.Add(collateral.Add(amount))
		sigHash := hashRevision(rev.Revision)
		rev.HostSignature = hostKey.SignHash(sigHash)
		rev.RenterSignature = renterKey.SignHash(sigHash)

		updater, err := c.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		for _, root := range roots {
			updater.AppendSector(root)
		}

		err = updater.Commit(rev, contracts.Usage{
			StorageRevenue:   amount,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", hostCollateral, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(collateral) {
			t.Fatalf("expected %v risked collateral, got %v", collateral, m.Contracts.RiskedCollateral)
		}

		// mine until the revision is broadcast
		remainingBlocks := rev.Revision.WindowStart - node.TipState().Index.Height - contracts.RevisionSubmissionBuffer
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time
		// confirm the revision
		if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until the end proof window
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height - 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// check that the contract is still active
		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be successful")
		} else if contract.ResolutionHeight != 0 {
			t.Fatalf("expected resolution height %v, got %v", 0, contract.ResolutionHeight)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 1 {
			t.Fatal("expected 1 active contracts")
		} else if m.Contracts.Successful != 0 {
			t.Fatal("expected 0 successful contracts")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.Equals(hostCollateral) {
			t.Fatalf("expected %v locked collateral, got %v", types.ZeroCurrency, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(collateral) {
			t.Fatalf("expected %v risked collateral, got %v", types.ZeroCurrency, m.Contracts.RiskedCollateral)
		}

		// mine until after the proof window
		remainingBlocks = rev.Revision.WindowEnd - node.TipState().Index.Height + 1
		if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// check that the contract is now failed
		contract, err = c.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusFailed {
			t.Fatal("expected contract to be successful")
		} else if contract.ResolutionHeight != 0 {
			t.Fatalf("expected resolution height %v, got %v", 0, contract.ResolutionHeight)
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != 0 {
			t.Fatal("expected 0 active contracts")
		} else if m.Contracts.Failed != 1 {
			t.Fatal("expected 1 failed contract")
		} else if m, err := node.Store().Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if !m.Contracts.LockedCollateral.IsZero() {
			t.Fatalf("expected %v locked collateral, got %v", types.ZeroCurrency, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.IsZero() {
			t.Fatalf("expected %v risked collateral, got %v", types.ZeroCurrency, m.Contracts.RiskedCollateral)
		}
	})
}

func TestSectorRoots(t *testing.T) {
	const sectors = 256
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()

	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node, err := test.NewWallet(hostKey, t.TempDir(), log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	am := alerts.NewManager()
	s, err := storage.NewVolumeManager(db, am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// create a fake volume so disk space is not used
	id, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

	c, err := contracts.NewManager(db, am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	rev := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			FileContract: types.FileContract{
				UnlockHash:  types.Hash256(contractUnlockConditions.UnlockHash()),
				WindowStart: 100,
				WindowEnd:   200,
			},
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
		},
	}

	if err := c.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	var roots []types.Hash256
	for i := 0; i < sectors; i++ {
		root := frand.Entropy256()
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		// use the database method directly to avoid the sector cache
		err = db.ReviseContract(rev, contracts.Usage{}, []contracts.SectorChange{
			{Action: contracts.SectorActionAppend, Root: root},
		})
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}

	// check that the sector roots are correct
	check, err := c.SectorRoots(rev.Revision.ParentID, 0, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(check) != len(roots) {
		t.Fatalf("expected %v sector roots, got %v", len(roots), len(check))
	}
	for i := range check {
		if check[i] != roots[i] {
			t.Fatalf("expected sector root %v to be %v, got %v", i, roots[i], check[i])
		}
	}

	// check that the cached sector roots are correct
	check, err = c.SectorRoots(rev.Revision.ParentID, 0, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(check) != len(roots) {
		t.Fatalf("expected %v sector roots, got %v", len(roots), len(check))
	}
	for i := range check {
		if check[i] != roots[i] {
			t.Fatalf("expected sector root %v to be %v, got %v", i, roots[i], check[i])
		}
	}
}
