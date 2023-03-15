package contracts_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.sia.tech/hostd/internal/test"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func formContract(renterKey, hostKey types.PrivateKey, c *contracts.ContractManager, w contracts.Wallet, cm contracts.ChainManager, tp contracts.TransactionPool) (contracts.SignedRevision, error) {
	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	txn := types.Transaction{
		FileContracts: []types.FileContract{{
			UnlockHash:  types.Hash256(contractUnlockConditions.UnlockHash()),
			WindowStart: 50,
			WindowEnd:   60,
			ValidProofOutputs: []types.SiacoinOutput{
				{Value: types.NewCurrency64(500), Address: w.Address()},
				{Address: w.Address()},
			},
			MissedProofOutputs: []types.SiacoinOutput{
				{Value: types.NewCurrency64(500), Address: w.Address()},
				{Address: w.Address()},
				{Address: types.VoidAddress},
			},
		}},
	}
	state := cm.TipState()
	txn.FileContracts[0].Payout = txn.FileContracts[0].ValidProofOutputs[0].Value.Add(state.FileContractTax(txn.FileContracts[0]))
	toSign, discard, err := w.FundTransaction(&txn, txn.FileContracts[0].Payout)
	if err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer discard()
	if err := w.SignTransaction(state, &txn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	if err := tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
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

	if err := c.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}, 100); err != nil {
		return contracts.SignedRevision{}, fmt.Errorf("failed to add contract: %w", err)
	}
	return rev, nil
}

func TestContractLockUnlock(t *testing.T) {
	privKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()
	opt := zap.NewDevelopmentConfig()
	opt.OutputPaths = []string{filepath.Join(dir, "hostd.log")}
	log, err := opt.Build()
	if err != nil {
		t.Fatal(err)
	}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node, err := test.NewWallet(privKey, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	s, err := storage.NewVolumeManager(db, node.ChainManager(), log.Named("storage"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c, err := contracts.NewManager(db, s, node.ChainManager(), node.TPool(), node.Wallet(), log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	rev := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			FileContract: types.FileContract{
				WindowStart: 100,
				WindowEnd:   200,
			},
			ParentID: frand.Entropy256(),
		},
	}

	if err := c.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}, 100); err != nil {
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
	hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

	dir := t.TempDir()
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}

	node, err := test.NewWallet(hostKey, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	s, err := storage.NewVolumeManager(node.Store(), node.ChainManager(), log.Named("storage"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.AddVolume(filepath.Join(dir, "data.dat"), 10); err != nil {
		t.Fatal(err)
	}

	c, err := contracts.NewManager(node.Store(), s, node.ChainManager(), node.TPool(), node.Wallet(), log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := node.MineBlocks(node.Wallet().Address(), int(stypes.MaturityDelay+2)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time

	rev, err := formContract(renterKey, hostKey, c, node.Wallet(), node.ChainManager(), node.TPool())
	if err != nil {
		t.Fatal(err)
	}

	contract, err := c.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.ContractStatusPending {
		t.Fatal("expected contract to be pending")
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
	}

	var roots []types.Hash256
	for i := 0; i < 5; i++ {
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhpv2.SectorRoot(&sector)
		release, err := s.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		roots = append(roots, root)
	}

	// create a revision that adds sectors and transfers funds to the host
	amount := types.NewCurrency64(100)
	rev.Revision.RevisionNumber++
	rev.Revision.Filesize = rhpv2.SectorSize * uint64(len(roots))
	rev.Revision.FileMerkleRoot = rhpv2.MetaRoot(roots)
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

	for _, root := range roots {
		updater.AppendSector(root)
	}

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
	remainingBlocks = rev.Revision.WindowStart - node.TipState().Index.Height
	if err := node.MineBlocks(types.VoidAddress, int(remainingBlocks)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time
	// confirm the proof
	if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time

	contract, err = c.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.ContractStatusSuccessful {
		t.Fatal("expected contract to be successful")
	} else if !contract.ResolutionConfirmed {
		t.Fatal("expected resolution to be confirmed")
	}
}
