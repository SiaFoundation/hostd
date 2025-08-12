package contracts_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/internal/testutil"
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
		Capacity:         0,
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

	basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
	if err != nil {
		t.Fatal("failed to fund transaction:", err)
	}
	w.SignV2Inputs(&txn, toSign)
	formationSet := rhp4.TransactionSet{
		Transactions: []types.V2Transaction{txn},
		Basis:        basis,
	}

	if broadcast {
		if _, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
			t.Fatal("failed to add formation set to pool:", err)
		}
	}

	if err := c.AddV2Contract(formationSet, proto4.Usage{}); err != nil {
		t.Fatal("failed to add contract:", err)
	}
	return txn.V2FileContractID(txn.ID(), 0), fc
}

func mineEmptyBlock(state consensus.State, minerAddr types.Address) types.Block {
	b := types.Block{
		ParentID:     state.Index.ID,
		Timestamp:    types.CurrentTimestamp(),
		MinerPayouts: []types.SiacoinOutput{{Address: minerAddr, Value: state.BlockReward()}},
	}
	if state.Index.Height >= state.Network.HardforkV2.AllowHeight {
		b.V2 = &types.V2BlockData{Height: state.Index.Height + 1}
		b.V2.Commitment = state.Commitment(minerAddr, b.Transactions, b.V2Transactions())
	}
	if !coreutils.FindBlockNonce(state, &b, 10*time.Second) {
		panic(fmt.Sprintf("failed to mine empty block at height %d", state.Index.Height+1))
	}
	return b
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
				UnlockHash:  contractUnlockConditions.UnlockHash(),
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
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

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

		// ensure any dereferenced sectors have been pruned
		if err := node.Store.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		m, err := node.Store.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if m.Contracts.Active != expectedStatuses[contracts.V2ContractStatusActive] {
			t.Fatalf("expected %v active contracts, got %v", expectedStatuses[contracts.V2ContractStatusActive], m.Contracts.Active)
		} else if m.Contracts.Successful != expectedStatuses[contracts.V2ContractStatusSuccessful] {
			t.Fatalf("expected %v successful contracts, got %v", expectedStatuses[contracts.V2ContractStatusSuccessful], m.Contracts.Successful)
		} else if m.Contracts.Renewed != expectedStatuses[contracts.V2ContractStatusRenewed] {
			t.Fatalf("expected %v renewed contracts, got %v", expectedStatuses[contracts.V2ContractStatusRenewed], m.Contracts.Renewed)
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
		time.Sleep(2 * time.Second) // wait for the volume manager to prune sectors

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
		assertStorageMetrics(t, 0, 0)

		// mine a block to rebroadcast the formation set
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// mine another block to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// mine until the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
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
		assertStorageMetrics(t, 0, 0)

		// mine until the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("storage proof", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)
		roots := []types.Hash256{root}

		if err := node.Volumes.Write(root, &sector); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize
		fc.Capacity = proto4.SectorSize
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err := node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
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

		// mine until the proof window so the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ProofHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// sector metrics should not change due to the reorg buffer
		assertStorageMetrics(t, 1, 1)

		// mine through the reorg buffer so the sectors will be garbage
		// collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("failed storage proof", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		root := frand.Entropy256() // random root
		roots := []types.Hash256{root}

		if err := node.Volumes.Write(root, &sector); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize
		fc.Capacity = proto4.SectorSize
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err := node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
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
		// storage metrics will not change due to the reorg buffer
		assertStorageMetrics(t, 1, 1)

		// mine through the reorg buffer so the sectors will be
		// garbage collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("renewal", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, node.Syncer, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)
		roots := []types.Hash256{root}

		if err := node.Volumes.Write(root, &sector); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize
		fc.Capacity = proto4.SectorSize
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		err := node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
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

		additionalCollateral := types.Siacoins(2)
		renewal := types.V2FileContractRenewal{
			NewContract: types.V2FileContract{
				RevisionNumber:   0,
				Filesize:         fc.Filesize,
				Capacity:         fc.Capacity,
				FileMerkleRoot:   fc.FileMerkleRoot,
				ProofHeight:      fc.ProofHeight + 30,
				ExpirationHeight: fc.ExpirationHeight + 30,
				RenterOutput:     fc.RenterOutput,
				HostOutput: types.SiacoinOutput{
					Address: fc.HostOutput.Address,
					Value:   fc.HostOutput.Value.Add(additionalCollateral),
				},
				MissedHostValue: fc.MissedHostValue.Add(additionalCollateral),
				TotalCollateral: fc.TotalCollateral.Add(additionalCollateral),
				RenterPublicKey: renterKey.PublicKey(),
				HostPublicKey:   hostKey.PublicKey(),
			},
			HostRollover:   fc.HostOutput.Value,
			RenterRollover: fc.RenterOutput.Value,
		}
		renewalSigHash := cs.RenewalSigHash(renewal)
		renewal.HostSignature = hostKey.SignHash(renewalSigHash)
		renewal.RenterSignature = renterKey.SignHash(renewalSigHash)
		contractSigHash := cs.ContractSigHash(renewal.NewContract)
		renewal.NewContract.HostSignature = hostKey.SignHash(contractSigHash)
		renewal.NewContract.RenterSignature = renterKey.SignHash(contractSigHash)

		_, fce, err := com.V2FileContractElement(contractID)
		if err != nil {
			t.Fatal(err)
		}

		fundAmount := cs.V2FileContractTax(renewal.NewContract).Add(additionalCollateral)
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Value: fundAmount, Address: fc.HostOutput.Address},
			},
		}
		basis, toSign, err := node.Wallet.FundV2Transaction(&setupTxn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		node.Wallet.SignV2Inputs(&setupTxn, toSign)

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
		node.Wallet.SignV2Inputs(&renewalTxn, []int{0})
		renewalTxnSet := rhp4.TransactionSet{
			Basis:        basis,
			Transactions: []types.V2Transaction{setupTxn, renewalTxn},
		}
		if _, err := cm.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal("failed to add renewal to pool:", err)
		}

		err = com.RenewV2Contract(renewalTxnSet, proto4.Usage{
			RiskedCollateral: renewal.NewContract.TotalCollateral.Sub(renewal.NewContract.MissedHostValue),
		})
		if err != nil {
			t.Fatal(err)
		}

		renewalID := contractID.V2RenewalID()

		// only contract sectors metric should have changed
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, types.Siacoins(20), collateral)
		// renewed contracts temporarily double the contract sectors
		// until the reorg buffer is hit
		assertStorageMetrics(t, 2, 1)

		// try to revise the original contract before the renewal is confirmed
		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err == nil || !strings.Contains(err.Error(), "renewed contracts cannot be revised") {
			t.Fatalf("expected renewal error, got %v", err)
		}

		// mine to confirm the renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		// new contract pending -> active, old contract active -> renewed
		expectedStatuses[contracts.V2ContractStatusRenewed]++
		expectedStatuses[contracts.V2ContractStatusActive] += 0 // no change
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusActive)
		// metrics should reflect the new contract, but storage should
		// not change due to the reorg buffer
		assertContractMetrics(t, types.Siacoins(22), collateral)
		assertStorageMetrics(t, 2, 1)

		// try to revise the original contract after the renewal is confirmed
		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err == nil || !strings.Contains(err.Error(), "renewed contracts cannot be revised") {
			t.Fatalf("expected renewal error, got %v", err)
		}

		// mine through the reorg buffer so the original contract sectors will be
		// garbage collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 1, 1)
		t.Log("current height", cm.Tip().Height, fc.ProofHeight, fc.ExpirationHeight, renewal.NewContract.ProofHeight, renewal.NewContract.ExpirationHeight)

		// mine until the renewed contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.NewContract.ProofHeight-cm.Tip().Height+1))
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		t.Log("current height", cm.Tip().Height, fc.ProofHeight, fc.ExpirationHeight, renewal.NewContract.ProofHeight, renewal.NewContract.ExpirationHeight)
		// storage metrics will not change due to the reorg buffer
		assertStorageMetrics(t, 1, 1)

		// mine through the reorg buffer so all the storage will be garbage
		// collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)

		// try to revise the original contract after the renewal is successful
		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err == nil || !strings.Contains(err.Error(), "renewed contracts cannot be revised") {
			t.Fatalf("expected renewal error, got %v", err)
		}
	})

	t.Run("rejected no storage", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		duration := uint64(10)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			Capacity:         0,
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

		basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&txn, toSign)
		formationSet := rhp4.TransactionSet{
			Transactions: []types.V2Transaction{txn},
			Basis:        basis,
		}
		contractID := txn.V2FileContractID(txn.ID(), 0)
		// corrupt the formation set to trigger a rejection
		formationSet.Transactions[len(formationSet.Transactions)-1].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := c.AddV2Contract(formationSet, proto4.Usage{}); err != nil {
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

	t.Run("rejected with storage", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		duration := uint64(10)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			Capacity:         0,
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

		basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&txn, toSign)
		formationSet := rhp4.TransactionSet{
			Transactions: []types.V2Transaction{txn},
			Basis:        basis,
		}
		contractID := txn.V2FileContractID(txn.ID(), 0)
		// corrupt the formation set to trigger a rejection
		formationSet.Transactions[len(formationSet.Transactions)-1].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := c.AddV2Contract(formationSet, proto4.Usage{}); err != nil {
			t.Fatal("failed to add contract:", err)
		}

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// add a root to the contract
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)
		roots := []types.Hash256{root}

		if err := node.Volumes.Write(root, &sector); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize
		fc.Capacity = proto4.SectorSize
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		revisionSigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(revisionSigHash)
		fc.RenterSignature = renterKey.SignHash(revisionSigHash)

		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		}

		// only the storage metrics will have changed
		// since revenue/collateral metrics are only applied
		// after confirmation.
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 1, 1)

		// mine until the contract is rejected
		testutil.MineAndSync(t, node, types.VoidAddress, 20)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("revert", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		duration := uint64(10)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			Capacity:         0,
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

		basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&txn, toSign)
		formationSet := rhp4.TransactionSet{
			Transactions: []types.V2Transaction{txn},
			Basis:        basis,
		}

		// broadcast the formation
		if _, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
			t.Fatal(err)
		}

		contractID := txn.V2FileContractID(txn.ID(), 0)
		// corrupt the formation set so the manager cannot rebroadcast it
		corruptTxn := txn.DeepCopy()
		corruptTxn.SiacoinInputs[0].Parent.StateElement.MerkleProof = nil
		corruptedSet := rhp4.TransactionSet{
			Basis:        basis,
			Transactions: []types.V2Transaction{corruptTxn},
		}
		if err := c.AddV2Contract(corruptedSet, proto4.Usage{}); err != nil {
			t.Fatal("failed to add contract:", err)
		}

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// prepare blocks to revert the contract formation
		revertState := node.Chain.TipState()
		var blocks []types.Block
		for range 5 {
			blocks = append(blocks, mineEmptyBlock(revertState, types.VoidAddress))
			revertState, _ = consensus.ApplyBlock(revertState, blocks[len(blocks)-1], consensus.V1BlockSupplement{}, time.Time{})
		}

		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		expectedStatuses[contracts.V2ContractStatusPending]--
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, hostFunds, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		if err := node.Chain.AddBlocks(blocks); err != nil {
			t.Fatal(err)
		}
		testutil.WaitForSync(t, node.Chain, node.Indexer)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("revert with storage", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		duration := uint64(10)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			Capacity:         0,
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

		basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&txn, toSign)
		formationSet := rhp4.TransactionSet{
			Transactions: []types.V2Transaction{txn},
			Basis:        basis,
		}

		// broadcast the formation
		if _, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
			t.Fatal(err)
		}

		contractID := txn.V2FileContractID(txn.ID(), 0)
		// corrupt the formation set so the manager cannot rebroadcast it
		corruptTxn := txn.DeepCopy()
		corruptTxn.SiacoinInputs[0].Parent.StateElement.MerkleProof = nil
		corruptedSet := rhp4.TransactionSet{
			Basis:        basis,
			Transactions: []types.V2Transaction{corruptTxn},
		}
		if err := c.AddV2Contract(corruptedSet, proto4.Usage{}); err != nil {
			t.Fatal("failed to add contract:", err)
		}

		// add a root to the contract
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)
		roots := []types.Hash256{root}

		if err := node.Volumes.Write(root, &sector); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize
		fc.Capacity = proto4.SectorSize
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		// transfer some funds from the renter to the host
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		revisionSigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(revisionSigHash)
		fc.RenterSignature = renterKey.SignHash(revisionSigHash)

		err = node.Contracts.ReviseV2Contract(contractID, fc, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		})
		if err != nil {
			t.Fatal(err)
		}

		// only storage metrics will have changed because
		// revenue metrics are only applied on confirmation
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 1, 1)

		// prepare blocks to revert the contract formation
		revertState := node.Chain.TipState()
		var blocks []types.Block
		for range 5 {
			blocks = append(blocks, mineEmptyBlock(revertState, types.VoidAddress))
			revertState, _ = consensus.ApplyBlock(revertState, blocks[len(blocks)-1], consensus.V1BlockSupplement{}, time.Time{})
		}

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		expectedStatuses[contracts.V2ContractStatusPending]--
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, hostFunds, collateral)
		assertStorageMetrics(t, 1, 1)

		// revert the contract formation
		if err := node.Chain.AddBlocks(blocks); err != nil {
			t.Fatal(err)
		}
		testutil.WaitForSync(t, node.Chain, node.Indexer)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 1, 1)
	})
}

func TestV2SectorRoots(t *testing.T) {
	log := zaptest.NewLogger(t)

	const sectors = 256
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	dir := t.TempDir()

	network, genesis := testutil.V2Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	result := make(chan error, 1)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 10, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	// create a fake volume so disk space is not used
	id, err := node.Store.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := node.Store.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err := node.Store.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

	cs := node.Chain.TipState()
	txn := types.V2Transaction{
		FileContracts: []types.V2FileContract{
			{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				ProofHeight:      100,
				ExpirationHeight: 200,
			},
		},
	}
	sigHash := cs.ContractSigHash(txn.FileContracts[0])
	txn.FileContracts[0].RenterSignature = renterKey.SignHash(sigHash)
	txn.FileContracts[0].HostSignature = hostKey.SignHash(sigHash)

	err = node.Contracts.AddV2Contract(rhp4.TransactionSet{
		Transactions: []types.V2Transaction{txn},
		Basis:        node.Chain.Tip(),
	}, proto4.Usage{})
	if err != nil {
		t.Fatal(err)
	}
	contractID := txn.V2FileContractID(txn.ID(), 0)

	var roots []types.Hash256
	rev := txn.FileContracts[0]
	for range sectors {
		root := frand.Entropy256()
		err := node.Store.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)

		rev.FileMerkleRoot = proto4.MetaRoot(roots)
		rev.Filesize += proto4.SectorSize
		rev.Capacity += proto4.SectorSize
		sigHash := cs.ContractSigHash(rev)
		rev.RenterSignature = renterKey.SignHash(sigHash)
		rev.HostSignature = hostKey.SignHash(sigHash)

		err = node.Contracts.ReviseV2Contract(contractID, rev, roots, proto4.Usage{})
		if err != nil {
			t.Fatal(err)
		}
	}

	assertRoots := func(t *testing.T, roots []types.Hash256) {
		t.Helper()

		// check that the cached sector roots are correct
		check := node.Contracts.SectorRoots(contractID)
		if err != nil {
			t.Fatal(err)
		} else if len(check) != len(roots) {
			t.Fatalf("expected %v cached sector roots, got %v", len(roots), len(check))
		}
		for i := range check {
			if check[i] != roots[i] {
				t.Fatalf("expected sector root %v to be %v, got %v", i, roots[i], check[i])
			}
		}

		dbRoots, err := node.Store.V2SectorRoots()
		if err != nil {
			t.Fatal(err)
		}
		check = dbRoots[contractID]
		if len(check) != len(roots) {
			t.Fatalf("expected %v database sector roots, got %v", len(roots), len(check))
		}
		for i := range check {
			if check[i] != roots[i] {
				t.Fatalf("expected sector root %v to be %v, got %v", i, roots[i], check[i])
			}
		}
	}

	assertRoots(t, roots)

	// reload the contract manager to ensure the roots are persisted
	node.Contracts.Close()
	node.Contracts, err = contracts.NewManager(node.Store, node.Volumes, node.Chain, node.Wallet)
	if err != nil {
		t.Fatal(err)
	}

	assertRoots(t, roots)
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
