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
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/testutil"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func formV2Contract(t *testing.T, cm *chain.Manager, c *contracts.Manager, w *wallet.SingleAddressWallet, s *syncer.Syncer, renterKey, hostKey types.PrivateKey, renterFunds, hostFunds types.Currency, duration uint64, broadcast bool) (types.FileContractID, types.V2FileContract) {
	t.Helper()

	cs := cm.TipState()
	fc := types.V2FileContract{
		RevisionNumber:   0,
		Filesize:         0,
		FileMerkleRoot:   types.Hash256{},
		ProofHeight:      cs.Index.Height + duration,
		ExpirationHeight: cs.Index.Height + duration + 10,
		RenterOutput: types.SiacoinOutput{
			Value:   renterFunds,
			Address: w.Address(),
		},
		HostOutput: types.SiacoinOutput{
			Value:   hostFunds,
			Address: w.Address(),
		},
		MissedHostValue: hostFunds,
		TotalCollateral: hostFunds,
		RenterPublicKey: renterKey.PublicKey(),
		HostPublicKey:   hostKey.PublicKey(),
	}
	fundAmount := cs.V2FileContractTax(fc).Add(hostFunds).Add(renterFunds)
	sigHash := cs.ContractSigHash(fc)
	fc.HostSignature = hostKey.SignHash(sigHash)
	fc.RenterSignature = renterKey.SignHash(sigHash)

	txn := types.V2Transaction{
		FileContracts: []types.V2FileContract{fc},
	}

	cs, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
	if err != nil {
		t.Fatal("failed to fund transaction:", err)
	}
	w.SignV2Inputs(cs, &txn, toSign)
	formationSet := contracts.V2FormationTransactionSet{
		TransactionSet: []types.V2Transaction{txn},
		Basis:          cs.Index,
	}

	if broadcast {
		if _, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.TransactionSet); err != nil {
			t.Fatal("failed to add formation set to pool:", err)
		}
		s.BroadcastV2TransactionSet(formationSet.Basis, formationSet.TransactionSet)
	}

	if err := c.AddV2Contract(formationSet, contracts.Usage{}); err != nil {
		t.Fatal("failed to add contract:", err)
	}
	return txn.V2FileContractID(txn.ID(), 0), fc
}

func formContract(t *testing.T, cm *chain.Manager, c *contracts.Manager, w *wallet.SingleAddressWallet, s *syncer.Syncer, renterKey, hostKey types.PrivateKey, renterFunds, hostFunds types.Currency, duration uint64, broadcast bool) contracts.SignedRevision {
	t.Helper()

	contract := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), renterFunds, hostFunds, cm.Tip().Height+duration, rhp2.HostSettings{WindowSize: 10}, w.Address())
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
	toSign, err := w.FundTransaction(&txn, formationCost.Add(hostFunds), true) // we're funding both sides of the payout
	if err != nil {
		t.Fatal("failed to fund transaction:", err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
	formationSet := append(cm.UnconfirmedParents(txn), txn)
	if broadcast {
		if _, err := cm.AddPoolTransactions(formationSet); err != nil {
			t.Fatal("failed to add formation set to pool:", err)
		}
		s.BroadcastTransactionSet(formationSet)
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
	if err := c.AddContract(rev, formationSet, hostFunds, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}
	return rev
}

func TestContractLockUnlock(t *testing.T) {
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	log := zaptest.NewLogger(t)
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

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

	if err := node.Contracts.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	if _, err := node.Contracts.Lock(context.Background(), rev.Revision.ParentID); err != nil {
		t.Fatal(err)
	}

	err := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := node.Contracts.Lock(ctx, rev.Revision.ParentID)
		return err
	}()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected context deadline exceeded, got", err)
	}

	node.Contracts.Unlock(rev.Revision.ParentID)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := node.Contracts.Lock(context.Background(), rev.Revision.ParentID); err != nil {
				t.Error(err)
			}
			time.Sleep(100 * time.Millisecond)
			node.Contracts.Unlock(rev.Revision.ParentID)
		}()
	}
	wg.Wait()
}

func TestContractLifecycle(t *testing.T) {
	assertContractStatus := func(t *testing.T, c *contracts.Manager, contractID types.FileContractID, status contracts.ContractStatus) {
		t.Helper()

		contract, err := c.Contract(contractID)
		if err != nil {
			t.Fatal("failed to get contract", err)
		} else if contract.Status != status {
			t.Fatalf("expected contract to be %v, got %v", status, contract.Status)
		}
	}

	assertContractMetrics := func(t *testing.T, s *sqlite.Store, active, successful uint64, locked, risked types.Currency) {
		t.Helper()

		m, err := s.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != active {
			t.Fatalf("expected %v active contracts, got %v", active, m.Contracts.Active)
		} else if m.Contracts.Successful != successful {
			t.Fatalf("expected %v successful contracts, got %v", successful, m.Contracts.Successful)
		} else if !m.Contracts.LockedCollateral.Equals(locked) {
			t.Fatalf("expected %v locked collateral, got %v", locked, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(risked) {
			t.Fatalf("expected %v risked collateral, got %v", risked, m.Contracts.RiskedCollateral)
		}
	}

	t.Run("reject", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)
		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds := types.Siacoins(10)
		hostFunds := types.Siacoins(20)
		contract := rhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), renterFunds, hostFunds, cm.Tip().Height+10, rhp2.HostSettings{WindowSize: 10}, w.Address())
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
		toSign, err := w.FundTransaction(&txn, formationCost.Add(hostFunds), true) // we're funding both sides of the payout
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
		formationSet := append(cm.UnconfirmedParents(txn), txn)
		revision := types.FileContractRevision{
			ParentID:         txn.FileContractID(0),
			UnlockConditions: contractUnlockConditions,
			FileContract:     txn.FileContracts[0],
		}
		// corrupt the transaction set to simulate a rejected contract
		formationSet[len(formationSet)-1].Signatures = nil
		revision.RevisionNumber = 1
		sigHash := hashRevision(revision)
		rev := contracts.SignedRevision{
			Revision:        revision,
			HostSignature:   hostKey.SignHash(sigHash),
			RenterSignature: renterKey.SignHash(sigHash),
		}
		if err := c.AddContract(rev, formationSet, hostFunds, contracts.Usage{}); err != nil {
			t.Fatal(err)
		}

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// mine until the contract is rejected
		testutil.MineAndSync(t, node, types.VoidAddress, 20)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusRejected)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("rebroadcast", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)
		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		rev := formContract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, false)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// mine a block to rebroadcast the formation set
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// mine another block to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, types.Siacoins(20), types.ZeroCurrency)

		// mine until the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(rev.Revision.WindowEnd-node.Chain.Tip().Height)+1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusSuccessful)
		assertContractMetrics(t, node.Store, 0, 1, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("successful with proof", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		dir := t.TempDir()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)

		result := make(chan error, 1)
		if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev := formContract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, renterFunds, hostCollateral, 10, true)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		// pending contracts do not contribute to metrics
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

		var releaseFuncs []func() error
		defer func() {
			for _, release := range releaseFuncs {
				if err := release(); err != nil {
					t.Fatal(err)
				}
			}
		}()

		var roots []types.Hash256
		for i := 0; i < 5; i++ {
			var sector [rhp2.SectorSize]byte
			frand.Read(sector[:256])
			root := rhp2.SectorRoot(&sector)
			release, err := node.Volumes.Write(root, &sector)
			if err != nil {
				t.Fatal(err)
			}
			releaseFuncs = append(releaseFuncs, release)
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

		updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
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
		}
		for _, release := range releaseFuncs {
			if err := release(); err != nil {
				t.Fatal(err)
			}
		}

		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, collateral)

		// mine until right before the proof window so the revision is broadcast
		// and confirmed
		remainingBlocks := rev.Revision.WindowStart - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		contract, err := node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine into the proof window
		testutil.MineAndSync(t, node, types.VoidAddress, 2)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusSuccessful)
		assertContractMetrics(t, node.Store, 0, 1, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("successful no proof", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		dir := t.TempDir()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)

		result := make(chan error, 1)
		if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev := formContract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, renterFunds, hostCollateral, 10, true)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

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

		updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		err = updater.Commit(rev, contracts.Usage{
			AccountFunding: amount,
		})
		if err != nil {
			t.Fatal(err)
		}

		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

		// mine until right before the proof window so the revision is broadcast
		remainingBlocks := rev.Revision.WindowStart - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		contract, err := node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until the end of the proof window -- contract should still be
		// active since no proof is required.
		remainingBlocks = rev.Revision.WindowEnd - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

		// mine after the proof window ends -- contract should be successful
		testutil.MineAndSync(t, node, types.VoidAddress, 10)

		assertContractMetrics(t, node.Store, 0, 1, types.ZeroCurrency, types.ZeroCurrency)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusSuccessful)

		contract, err = node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusSuccessful {
			t.Fatal("expected contract to be successful")
		} else if contract.ResolutionHeight != contract.Revision.WindowEnd {
			t.Fatalf("expected resolution height %v, got %v", contract.Revision.WindowEnd, contract.ResolutionHeight)
		}
	})

	t.Run("0 filesize contract", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		dir := t.TempDir()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)

		result := make(chan error, 1)
		if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev := formContract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, renterFunds, hostCollateral, 10, true)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

		// create a revision that transfers funds to the host with out adding any sectors
		amount := types.NewCurrency64(100)
		rev.Revision.RevisionNumber++
		rev.Revision.ValidProofOutputs[0].Value = rev.Revision.ValidProofOutputs[0].Value.Sub(amount)
		rev.Revision.ValidProofOutputs[1].Value = rev.Revision.ValidProofOutputs[1].Value.Add(amount)
		sigHash := hashRevision(rev.Revision)
		rev.HostSignature = hostKey.SignHash(sigHash)
		rev.RenterSignature = renterKey.SignHash(sigHash)

		updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		defer updater.Close()

		if err := updater.Commit(rev, contracts.Usage{}); err != nil {
			t.Fatal(err)
		}

		// mine until right before the proof window starts to broadcast and
		// confirm the revision
		remainingBlocks := rev.Revision.WindowStart - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		contract, err := node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until just before the end of the proof window to broadcast the
		// proof and confirm the resolution
		remainingBlocks = rev.Revision.WindowEnd - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		contract, err = node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.ResolutionHeight == 0 {
			t.Fatalf("expected contract to have resolution got %v", contract.ResolutionHeight)
		}

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusSuccessful)
		assertContractMetrics(t, node.Store, 0, 1, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("failed corrupt sector", func(t *testing.T) {
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		dir := t.TempDir()
		log := zaptest.NewLogger(t)

		network, genesis := testutil.V1Network()
		node := testutil.NewHostNode(t, hostKey, network, genesis, log)

		result := make(chan error, 1)
		if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
			t.Fatal(err)
		} else if err := <-result; err != nil {
			t.Fatal(err)
		}

		testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

		renterFunds := types.Siacoins(500)
		hostCollateral := types.Siacoins(1000)
		rev := formContract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, renterFunds, hostCollateral, 10, true)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusPending)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)

		// confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, types.ZeroCurrency)

		// add sectors to the volume manager
		var releaseFuncs []func() error
		var roots []types.Hash256
		for i := 0; i < 5; i++ {
			var sector [rhp2.SectorSize]byte
			frand.Read(sector[:])
			root := rhp2.SectorRoot(&sector)
			release, err := node.Volumes.Write(root, &sector)
			if err != nil {
				t.Fatal(err)
			}
			releaseFuncs = append(releaseFuncs, release)
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

		updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
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
		}

		// release the sectors
		for _, release := range releaseFuncs {
			if err := release(); err != nil {
				t.Fatal(err)
			}
		}

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusActive)
		assertContractMetrics(t, node.Store, 1, 0, hostCollateral, collateral)

		// mine until right before the proof window so the revision is broadcast
		// and confirmed
		remainingBlocks := rev.Revision.WindowStart - node.Chain.Tip().Height - 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		contract, err := node.Contracts.Contract(rev.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.ContractStatusActive {
			t.Fatal("expected contract to be active")
		} else if !contract.RevisionConfirmed {
			t.Fatal("expected revision to be confirmed")
		}

		// mine until after the proof window
		remainingBlocks = rev.Revision.WindowEnd - node.Chain.Tip().Height + 1
		testutil.MineAndSync(t, node, types.VoidAddress, int(remainingBlocks))

		assertContractStatus(t, node.Contracts, rev.Revision.ParentID, contracts.ContractStatusFailed)
		assertContractMetrics(t, node.Store, 0, 0, types.ZeroCurrency, types.ZeroCurrency)
	})
}

func TestV2ContractLifecycle(t *testing.T) {
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	dir := t.TempDir()
	log := zaptest.NewLogger(t)

	network, genesis := testutil.V2Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	result := make(chan error, 1)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

	assertContractStatus := func(t *testing.T, contractID types.FileContractID, status contracts.V2ContractStatus) {
		t.Helper()

		contract, err := node.Contracts.V2Contract(contractID)
		if err != nil {
			t.Fatal("failed to get contract:", err)
		} else if contract.Status != status {
			t.Fatalf("expected contract to be %v, got %v", status, contract.Status)
		}
	}

	// tracks statuses between subtests
	expectedStatuses := make(map[contracts.V2ContractStatus]uint64)
	assertContractMetrics := func(t *testing.T, locked, risked types.Currency) {
		t.Helper()

		m, err := node.Store.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != expectedStatuses[contracts.V2ContractStatusActive] {
			t.Fatalf("expected %v active contracts, got %v", expectedStatuses[contracts.V2ContractStatusActive], m.Contracts.Active)
		} else if m.Contracts.Successful != expectedStatuses[contracts.V2ContractStatusSuccessful] {
			t.Fatalf("expected %v successful contracts, got %v", expectedStatuses[contracts.V2ContractStatusSuccessful], m.Contracts.Successful)
		} else if m.Contracts.Renewed != expectedStatuses[contracts.V2ContractStatusRenewed] {
			t.Fatalf("expected %v renewed contracts, got %v", expectedStatuses[contracts.V2ContractStatusRenewed], m.Contracts.Renewed)
		} else if m.Contracts.Finalized != expectedStatuses[contracts.V2ContractStatusFinalized] {
			t.Fatalf("expected %v finalized contracts, got %v", expectedStatuses[contracts.V2ContractStatusFinalized], m.Contracts.Finalized)
		} else if m.Contracts.Failed != expectedStatuses[contracts.V2ContractStatusFailed] {
			t.Fatalf("expected %v failed contracts, got %v", expectedStatuses[contracts.V2ContractStatusFailed], m.Contracts.Failed)
		} else if !m.Contracts.LockedCollateral.Equals(locked) {
			t.Fatalf("expected %v locked collateral, got %v", locked, m.Contracts.LockedCollateral)
		} else if !m.Contracts.RiskedCollateral.Equals(risked) {
			t.Fatalf("expected %v risked collateral, got %v", risked, m.Contracts.RiskedCollateral)
		}
	}

	assertStorageMetrics := func(t *testing.T, contractSectors, physicalSectors uint64) {
		t.Helper()

		m, err := node.Store.Metrics(time.Now())
		if err != nil {
			t.Fatal("failed to get metrics:", err)
		} else if m.Storage.ContractSectors != contractSectors {
			t.Fatalf("expected %v contract sectors, got %v", contractSectors, m.Storage.ContractSectors)
		} else if m.Storage.PhysicalSectors != physicalSectors {
			t.Fatalf("expected %v physical sectors, got %v", physicalSectors, m.Storage.PhysicalSectors)
		}

		vols, err := node.Volumes.Volumes()
		if err != nil {
			t.Fatal("failed to get volumes:", err)
		}
		var volumeSectors uint64
		for _, vol := range vols {
			volumeSectors += vol.UsedSectors
		}
		if volumeSectors != physicalSectors {
			t.Fatalf("expected %v physical sectors, got %v", physicalSectors, volumeSectors)
		}
	}

	t.Run("rebroadcast", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, false)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// mine a block to rebroadcast the formation set
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// mine another block to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), types.ZeroCurrency)

		// mine until the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("successful empty contract", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// mine a block to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), types.ZeroCurrency)

		// mine until the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
	})

	t.Run("storage proof", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:])
		root := rhp2.SectorRoot(&sector)
		roots := []types.Hash256{root}

		release, err := node.Volumes.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		fc.Filesize = rhp2.SectorSize
		fc.FileMerkleRoot = rhp2.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, contracts.Usage{
			StorageRevenue:   cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
		// metrics should not have been updated, contract is still pending
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 1, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), collateral)
		assertStorageMetrics(t, 1, 1)

		// mine through the expiration height
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("failed storage proof", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := frand.Entropy256() // random root
		roots := []types.Hash256{root}

		release, err := node.Volumes.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		fc.Filesize = rhp2.SectorSize
		fc.FileMerkleRoot = rhp2.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, contracts.Usage{
			StorageRevenue:   cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
		// metrics should not have been updated, contract is still pending
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 1, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), collateral)
		assertStorageMetrics(t, 1, 1)

		// mine through the expiration height
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusFailed)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusFailed]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("renewal", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:])
		root := rhp2.SectorRoot(&sector)
		roots := []types.Hash256{root}

		release, err := node.Volumes.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		fc.Filesize = rhp2.SectorSize
		fc.FileMerkleRoot = rhp2.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, contracts.Usage{
			StorageRevenue:   cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		// ensure the metrics were updated
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, types.Siacoins(20), collateral)
		assertStorageMetrics(t, 1, 1)

		// renew the contract
		com := node.Contracts
		cm := node.Chain

		cs := cm.TipState()
		final := fc
		final.RevisionNumber = types.MaxRevisionNumber
		final.FileMerkleRoot = types.Hash256{}
		final.Filesize = 0
		final.HostSignature = types.Signature{}
		final.RenterSignature = types.Signature{}
		final.RevisionNumber = types.MaxRevisionNumber

		additionalCollateral := types.Siacoins(2)
		renewal := types.V2FileContractRenewal{
			FinalRevision: final,
			NewContract: types.V2FileContract{
				RevisionNumber:   0,
				Filesize:         fc.Filesize,
				FileMerkleRoot:   fc.FileMerkleRoot,
				ProofHeight:      final.ProofHeight + 10,
				ExpirationHeight: final.ExpirationHeight + 10,
				RenterOutput:     final.RenterOutput,
				HostOutput: types.SiacoinOutput{
					Address: final.HostOutput.Address,
					Value:   final.HostOutput.Value.Add(additionalCollateral),
				},
				MissedHostValue: final.MissedHostValue.Add(additionalCollateral),
				TotalCollateral: final.TotalCollateral.Add(additionalCollateral),
				RenterPublicKey: renterKey.PublicKey(),
				HostPublicKey:   hostKey.PublicKey(),
			},
			HostRollover:   final.HostOutput.Value,
			RenterRollover: final.RenterOutput.Value,
		}
		renewalSigHash := cs.RenewalSigHash(renewal)
		renewal.HostSignature = hostKey.SignHash(renewalSigHash)
		renewal.RenterSignature = renterKey.SignHash(renewalSigHash)

		fce, err := com.V2ContractElement(contractID)
		if err != nil {
			t.Fatal(err)
		}

		fundAmount := cs.V2FileContractTax(renewal.NewContract).Add(additionalCollateral)
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Value: fundAmount, Address: fc.HostOutput.Address},
			},
		}
		cs, toSign, err := node.Wallet.FundV2Transaction(&setupTxn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		node.Wallet.SignV2Inputs(cs, &setupTxn, toSign)

		renewalTxn := types.V2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{
				{
					Parent: setupTxn.EphemeralSiacoinOutput(0),
				},
			},
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &renewal,
				},
			},
		}
		node.Wallet.SignV2Inputs(cs, &renewalTxn, []int{0})
		renewalTxnSet := contracts.V2FormationTransactionSet{
			Basis:          cs.Index,
			TransactionSet: []types.V2Transaction{setupTxn, renewalTxn},
		}
		if _, err := cm.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.TransactionSet); err != nil {
			t.Fatal("failed to add renewal to pool:", err)
		}
		node.Syncer.BroadcastV2TransactionSet(renewalTxnSet.Basis, renewalTxnSet.TransactionSet)

		err = com.RenewV2Contract(renewalTxnSet, contracts.Usage{}, contracts.Usage{
			RiskedCollateral: renewal.NewContract.TotalCollateral.Sub(renewal.NewContract.MissedHostValue),
		})
		if err != nil {
			t.Fatal(err)
		}

		renewalID := contractID.V2RenewalID()

		// metrics should not have changed
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, types.Siacoins(20), collateral)
		assertStorageMetrics(t, 1, 1)

		// mine to confirm the renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		// new contract pending -> active, old contract active -> renewed
		expectedStatuses[contracts.V2ContractStatusRenewed]++
		expectedStatuses[contracts.V2ContractStatusActive] += 0 // no change
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusActive)
		// metrics should reflect the new contract
		assertContractMetrics(t, types.Siacoins(22), collateral)
		assertStorageMetrics(t, 1, 1)
		// mine until the renewed contract is successful and the sectors have
		// been pruned
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.NewContract.ExpirationHeight-node.Chain.Tip().Height)+1)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("reject", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		duration := uint64(10)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			FileMerkleRoot:   types.Hash256{},
			ProofHeight:      cs.Index.Height + duration,
			ExpirationHeight: cs.Index.Height + duration + 10,
			RenterOutput: types.SiacoinOutput{
				Value:   renterFunds,
				Address: w.Address(),
			},
			HostOutput: types.SiacoinOutput{
				Value:   hostFunds,
				Address: w.Address(),
			},
			MissedHostValue: hostFunds,
			TotalCollateral: hostFunds,
			RenterPublicKey: renterKey.PublicKey(),
			HostPublicKey:   hostKey.PublicKey(),
		}
		fundAmount := cs.V2FileContractTax(fc).Add(hostFunds).Add(renterFunds)
		sigHash := cs.ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		txn := types.V2Transaction{
			FileContracts: []types.V2FileContract{fc},
		}

		cs, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(cs, &txn, toSign)
		formationSet := contracts.V2FormationTransactionSet{
			TransactionSet: []types.V2Transaction{txn},
			Basis:          cs.Index,
		}
		contractID := txn.V2FileContractID(txn.ID(), 0)
		// corrupt the formation set to trigger a rejection
		formationSet.TransactionSet[len(formationSet.TransactionSet)-1].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := c.AddV2Contract(formationSet, contracts.Usage{}); err != nil {
			t.Fatal("failed to add contract:", err)
		}

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// mine until the contract is rejected
		testutil.MineAndSync(t, node, types.VoidAddress, 20)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRejected)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})
}

func TestSectorRoots(t *testing.T) {
	log := zaptest.NewLogger(t)

	const sectors = 256
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	dir := t.TempDir()

	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	result := make(chan error, 1)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, node, node.Wallet.Address(), 150)

	// create a fake volume so disk space is not used
	id, err := node.Store.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := node.Store.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err := node.Store.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

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

	if err := node.Contracts.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	var roots []types.Hash256
	for i := 0; i < sectors; i++ {
		root, err := func() (types.Hash256, error) {
			root := frand.Entropy256()
			release, err := node.Store.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
			if err != nil {
				return types.Hash256{}, fmt.Errorf("failed to store sector: %w", err)
			}
			defer release()

			updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
			if err != nil {
				return types.Hash256{}, fmt.Errorf("failed to revise contract: %w", err)
			}
			defer updater.Close()

			updater.AppendSector(root)

			if err := updater.Commit(rev, contracts.Usage{}); err != nil {
				return types.Hash256{}, fmt.Errorf("failed to commit revision: %w", err)
			}

			return root, nil
		}()
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}

	// check that the cached sector roots are correct
	check := node.Contracts.SectorRoots(rev.Revision.ParentID)
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

	dbRoots, err := node.Store.SectorRoots()
	if err != nil {
		t.Fatal(err)
	}
	check = dbRoots[rev.Revision.ParentID]
	if len(check) != len(roots) {
		t.Fatalf("expected %v sector roots, got %v", len(roots), len(check))
	}
	for i := range check {
		if check[i] != roots[i] {
			t.Fatalf("expected sector root %v to be %v, got %v", i, roots[i], check[i])
		}
	}
}

func TestChainIndexElementsDeepReorg(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesis := testutil.V2Network()
	n1 := testutil.NewConsensusNode(t, network, genesis, log.Named("node1"))

	h1 := testutil.NewHostNode(t, types.GeneratePrivateKey(), network, genesis, log.Named("host"))

	if _, err := h1.Syncer.Connect(context.Background(), n1.Syncer.Addr()); err != nil {
		t.Fatal(err)
	}

	mineAndSync := func(t *testing.T, cn *testutil.ConsensusNode, addr types.Address, n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			testutil.MineBlocks(t, cn, addr, 1)
			testutil.WaitForSync(t, cn.Chain, h1.Indexer)
		}
	}

	mineAndSync(t, n1, types.VoidAddress, 145)
	n2 := testutil.NewConsensusNode(t, network, genesis, log.Named("node2"))
	testutil.MineBlocks(t, n2, types.VoidAddress, 200)

	if _, err := h1.Syncer.Connect(context.Background(), n2.Syncer.Addr()); err != nil {
		t.Fatal(err)
	}
	testutil.WaitForSync(t, n2.Chain, h1.Indexer)
}
