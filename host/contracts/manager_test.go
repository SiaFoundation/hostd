package contracts_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func formV2Contract(t *testing.T, cm *chain.Manager, c *contracts.Manager, w *wallet.SingleAddressWallet, renterKey, hostKey types.PrivateKey, renterFunds, hostFunds types.Currency, duration uint64, broadcast bool) (types.FileContractID, types.V2FileContract) {
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

	renewContract := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, renterAllowance, collateral types.Currency) (types.FileContractID, types.V2FileContract, rhp4.TransactionSet, proto4.Usage) {
		t.Helper()

		cm := node.Chain
		con := node.Contracts
		w := node.Wallet
		cs := cm.TipState()

		renewal := types.V2FileContractRenewal{
			NewContract: types.V2FileContract{
				RevisionNumber:   0,
				Filesize:         fc.Filesize,
				Capacity:         fc.Capacity,
				FileMerkleRoot:   fc.FileMerkleRoot,
				ProofHeight:      fc.ProofHeight + 30,
				ExpirationHeight: fc.ExpirationHeight + 30,
				RenterOutput: types.SiacoinOutput{
					Address: fc.RenterOutput.Address,
					Value:   fc.RenterOutput.Value.Add(renterAllowance),
				},
				HostOutput: types.SiacoinOutput{
					Address: fc.HostOutput.Address,
					Value:   fc.HostOutput.Value.Add(collateral),
				},
				MissedHostValue: fc.MissedHostValue.Add(collateral),
				TotalCollateral: fc.TotalCollateral.Add(collateral),
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

		_, fce, err := con.V2FileContractElement(contractID)
		if err != nil {
			t.Fatal(err)
		}

		fundAmount := cs.V2FileContractTax(renewal.NewContract).Add(collateral).Add(renterAllowance)
		renewalTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &renewal,
				},
			},
		}
		basis, toSign, err := w.FundV2Transaction(&renewalTxn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&renewalTxn, toSign)
		return contractID.V2RenewalID(), renewal.NewContract, rhp4.TransactionSet{
				Basis:        basis,
				Transactions: []types.V2Transaction{renewalTxn},
			}, proto4.Usage{
				RiskedCollateral: renewal.NewContract.RiskedCollateral(),
			}
	}

	fundAccount := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, account proto4.Account, amount types.Currency) (types.V2FileContract, proto4.Usage) {
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(amount)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(amount)
		fc.RevisionNumber++
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)
		usage := proto4.Usage{
			AccountFunding: amount,
		}
		_, err := node.Contracts.CreditAccountsWithContract([]proto4.AccountDeposit{
			{Account: account, Amount: amount},
		}, contractID, fc, usage)
		if err != nil {
			t.Fatal(err)
		}
		return fc, usage
	}

	appendSector := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, roots []types.Hash256, corrupt bool) (types.V2FileContract, []types.Hash256, proto4.Usage) {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])

		var root types.Hash256
		if corrupt {
			root = frand.Entropy256()
		} else {
			root = proto4.SectorRoot(&sector)
		}
		roots = append(roots, root)

		if err := node.Volumes.StoreSector(root, &sector, 1); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize * uint64(len(roots))
		fc.Capacity = proto4.SectorSize * uint64(len(roots))
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

		usage := proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		}
		err := node.Contracts.ReviseV2Contract(contractID, fc, roots, usage)
		if err != nil {
			t.Fatal(err)
		}
		return fc, roots, usage
	}

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

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, false)
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

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
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

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		fc, _, usage := appendSector(t, contractID, fc, nil, false)

		// metrics should not have been updated, contract is still pending
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine until the proof window so the contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ProofHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// sector metrics should not change due to the reorg buffer
		assertStorageMetrics(t, 0, 1)

		// mine through the reorg buffer so the sectors will be garbage
		// collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("failed storage proof", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		fc, _, usage := appendSector(t, contractID, fc, nil, true)
		// metrics should not have been updated, contract is still pending
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractMetrics(t, types.Siacoins(20), usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine through the expiration height
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertContractStatus(t, contractID, contracts.V2ContractStatusFailed)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusFailed]++
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// storage metrics will not change due to the reorg buffer
		assertStorageMetrics(t, 0, 1)

		// mine through the reorg buffer so the sectors will be
		// garbage collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("renewal", func(t *testing.T) {
		assertStorageMetrics(t, 0, 0)

		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)

		// add a root to the contract
		fc, _, usage := appendSector(t, contractID, fc, nil, false)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		// ensure the metrics were updated
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, types.Siacoins(20), usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		renewalID, renewal, renewalTxnSet, usage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))

		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal("failed to add renewal to pool:", err)
		}

		err := node.Contracts.RenewV2Contract(renewalTxnSet, proto4.Usage{
			RiskedCollateral: renewal.RiskedCollateral(),
		})
		if err != nil {
			t.Fatal(err)
		}

		// only contract sectors metric should have changed
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, types.Siacoins(20), usage.RiskedCollateral)
		// renewed contracts use the same sectors as the original contract
		assertStorageMetrics(t, 1, 1)

		// try to revise the original contract before the renewal is confirmed
		err = node.Contracts.ReviseV2Contract(contractID, fc, []types.Hash256{}, proto4.Usage{})
		if !errors.Is(err, contracts.ErrAlreadyRenewed) {
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
		assertContractMetrics(t, types.Siacoins(22), renewal.RiskedCollateral())
		assertStorageMetrics(t, 1, 1)

		// try to revise the original contract after the renewal is confirmed
		err = node.Contracts.ReviseV2Contract(contractID, fc, []types.Hash256{}, proto4.Usage{})
		if !errors.Is(err, contracts.ErrAlreadyRenewed) {
			t.Fatalf("expected renewal error, got %v", err)
		}

		// mine through the reorg buffer so the original contract sectors will be
		// garbage collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 1, 1)

		// mine until the renewed contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ProofHeight-node.Chain.Tip().Height+1))
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// storage metrics will not change due to the reorg buffer
		assertStorageMetrics(t, 0, 1)

		// mine through the reorg buffer so all the storage will be garbage
		// collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)

		// try to revise the original contract after the renewal is successful
		err = node.Contracts.ReviseV2Contract(contractID, fc, []types.Hash256{}, proto4.Usage{})
		if !errors.Is(err, contracts.ErrAlreadyRenewed) {
			t.Fatalf("expected renewal error, got %v", err)
		}
	})

	t.Run("renewal with revision in same block", func(t *testing.T) {
		// form a contract
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)

		// revise the contract off-chain to have a valid revision
		fc, _, _ = appendSector(t, contractID, fc, nil, false)

		// build a revision transaction and add it to the pool
		basis, fce, err := node.Contracts.V2FileContractElement(contractID)
		if err != nil {
			t.Fatal(err)
		}
		revisionTxn := types.V2Transaction{
			FileContractRevisions: []types.V2FileContractRevision{
				{Parent: fce, Revision: fc},
			},
		}
		if _, err := node.Chain.AddV2PoolTransactions(basis, []types.V2Transaction{revisionTxn}); err != nil {
			t.Fatal("failed to add revision to pool:", err)
		}

		// build the renewal and add it to the pool as well
		renewalID, renewal, renewalTxnSet, _ := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal("failed to add renewal to pool:", err)
		}

		// renew the contract
		if err := node.Contracts.RenewV2Contract(renewalTxnSet, proto4.Usage{
			RiskedCollateral: renewal.RiskedCollateral(),
		}); err != nil {
			t.Fatal(err)
		}

		// mine a block to confirm the revision and renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		// the old contract should be renewed
		expectedStatuses[contracts.V2ContractStatusRenewed]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusActive)

		// mine until the renewed contract expires
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
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

		if err := node.Volumes.StoreSector(root, &sector, 1); err != nil {
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
		assertStorageMetrics(t, 0, 1)

		// mine until the contract is rejected
		testutil.MineAndSync(t, node, types.VoidAddress, 20)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("rejected renewal with storage", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)

		// form an initial contract with a longer duration so that the renewal can be rejected
		// while this contract is still active
		contractID, fc := formV2Contract(t, cm, c, w, renterKey, hostKey, renterFunds, hostFunds, 30, true)

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// add a root to the contract
		fc, roots, usage := appendSector(t, contractID, fc, nil, false)

		// only the storage metrics will have changed
		// since revenue/collateral metrics are only applied
		// after confirmation.
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusPending]--
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, fc.TotalCollateral, usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// renew the contract
		renewalID, renewal, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		// corrupt the renewalTxnSet so that it cannot be rebroadcast
		renewalTxnSet.Transactions[0].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// renewed contracts share the same sector roots
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, fc.TotalCollateral, usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// append a sector to the renewal
		appendSector(t, renewalID, renewal, roots, false)
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		// collateral metrics won't change since the renewed contract is not
		// active
		assertContractMetrics(t, fc.TotalCollateral, usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 2)

		// mine until the renewed contract is rejected
		// only contract status metrics should change
		// the existing contract should still be active
		testutil.MineAndSync(t, node, types.VoidAddress, 18)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		expectedStatuses[contracts.V2ContractStatusPending]--
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, fc.TotalCollateral, usage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine until the original contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ProofHeight-cm.Tip().Height)+1)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusSuccessful)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// storage metrics won't be garbage collected for 6 blocks
		assertStorageMetrics(t, 0, 1)

		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("rejected renewal with storage re-renewed", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)

		// form an initial contract with a longer duration so that the renewal can be rejected
		// while this contract is still active
		contractID, fc := formV2Contract(t, cm, c, w, renterKey, hostKey, renterFunds, hostFunds, 30, true)

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// add a root to the contract
		fc, _, appendSectorUsage := appendSector(t, contractID, fc, nil, false)

		// only the storage metrics will have changed
		// since revenue/collateral metrics are only applied
		// after confirmation.
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 1)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusPending]--
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, fc.TotalCollateral, appendSectorUsage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// renew the contract
		renewalID, _, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		// corrupt the renewalTxnSet so that it cannot be rebroadcast
		renewalTxnSet.Transactions[0].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// renewed contracts share the same sector roots
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, fc.TotalCollateral, appendSectorUsage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine until the renewed contract is rejected
		// only contract status metrics should change
		// the existing contract should still be active
		testutil.MineAndSync(t, node, types.VoidAddress, 18)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		expectedStatuses[contracts.V2ContractStatusPending]--
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, fc.TotalCollateral, appendSectorUsage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// renew the contract again
		renewalID, renewal, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := cm.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// renewed contracts share the same sector roots
		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, fc.TotalCollateral, appendSectorUsage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine to confirm the renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusPending]--
		expectedStatuses[contracts.V2ContractStatusRenewed]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, renewal.TotalCollateral, renewalUsage.RiskedCollateral)
		assertStorageMetrics(t, 1, 1)

		// mine until the original contract sectors are garbage collected
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 1, 1)

		// mine until the renewed contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ProofHeight-cm.Tip().Height)+1)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		// storage metrics won't be garbage collected for 6 blocks
		assertStorageMetrics(t, 0, 1)

		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertStorageMetrics(t, 0, 0)
	})

	t.Run("rejected renewal with account funding re-renewed", func(t *testing.T) {
		cm := node.Chain
		c := node.Contracts
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)

		// form an initial contract with a longer duration so that the renewal can be rejected
		// while this contract is still active
		contractID, fc := formV2Contract(t, cm, c, w, renterKey, hostKey, renterFunds, hostFunds, 30, true)

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusPending)
		// metrics should not have changed
		assertContractMetrics(t, types.ZeroCurrency, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// mine to confirm the contract
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusPending]--
		expectedStatuses[contracts.V2ContractStatusActive]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, fc.TotalCollateral, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// renew the contract
		renewalID, renewal, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		// corrupt the renewalTxnSet so that it cannot be rebroadcast
		renewalTxnSet.Transactions[0].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}
		if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		assertAccountMetrics := func(t *testing.T, amount types.Currency) {
			t.Helper()

			m, err := node.Store.Metrics(time.Now())
			if err != nil {
				t.Fatal("failed to get metrics", err)
			} else if !m.Accounts.Balance.Equals(amount) {
				t.Fatalf("expected account funding %d, got %d", amount, m.Accounts.Balance)
			}
		}

		assertAccountFunding := func(t *testing.T, account proto4.Account, contractID types.FileContractID, amount types.Currency) {
			t.Helper()

			sources, err := node.Store.RHP4AccountFunding(account)
			if err != nil {
				t.Fatal("failed to get account funding sources", err)
			}

			for _, source := range sources {
				if source.Amount.IsZero() {
					t.Fatal("account funding source is zero", account, contractID, amount)
				} else if source.ContractID == contractID && source.Amount.Equals(amount) {
					return
				} else if source.ContractID == contractID {
					t.Fatalf("expected contract %q to fund %d, got %d", contractID, amount, source.Amount)
				}
			}

			if amount.IsZero() {
				return // zero means no source exists
			}
			t.Fatal("failed to find account funding source", account, contractID, amount)
		}

		// fund an account
		account := proto4.Account(types.GeneratePrivateKey().PublicKey())
		_, usage := fundAccount(t, renewalID, renewal, account, types.Siacoins(2))
		assertAccountFunding(t, account, renewalID, usage.AccountFunding)
		assertAccountMetrics(t, usage.AccountFunding)

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, fc.TotalCollateral, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)

		// mine until the renewed contract is rejected
		// only contract status metrics should change
		// the existing contract should still be active
		testutil.MineAndSync(t, node, types.VoidAddress, 18)
		expectedStatuses[contracts.V2ContractStatusRejected]++
		expectedStatuses[contracts.V2ContractStatusPending]--
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusRejected)
		assertContractMetrics(t, fc.TotalCollateral, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
		// account funding should be reset
		assertAccountFunding(t, account, renewalID, types.ZeroCurrency)
		assertAccountMetrics(t, types.ZeroCurrency)

		// renew the contract again
		renewalID, renewal, renewalTxnSet, renewalUsage = renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := cm.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// spend from the existing account
		err := node.Contracts.DebitAccount(account, proto4.Usage{
			Storage: types.NewCurrency64(100),
		})
		if !errors.Is(err, proto4.ErrNotEnoughFunds) {
			t.Fatalf("expected not enough funds error, got %v", err)
		}

		expectedStatuses[contracts.V2ContractStatusPending]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusActive)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusPending)
		assertContractMetrics(t, fc.TotalCollateral, types.ZeroCurrency)
		assertStorageMetrics(t, 0, 0)
		assertAccountFunding(t, account, renewalID, types.ZeroCurrency) // account funding metrics should be reset
		assertAccountMetrics(t, types.ZeroCurrency)

		// mine to confirm the renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		expectedStatuses[contracts.V2ContractStatusPending]--
		expectedStatuses[contracts.V2ContractStatusRenewed]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusActive)
		assertContractMetrics(t, renewal.TotalCollateral, renewalUsage.RiskedCollateral)
		assertStorageMetrics(t, 0, 0)
		assertAccountFunding(t, account, renewalID, types.ZeroCurrency) // account funding metrics should be reset
		assertAccountMetrics(t, types.ZeroCurrency)

		// mine until the renewed contract is successful
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ProofHeight-cm.Tip().Height)+1)
		expectedStatuses[contracts.V2ContractStatusActive]--
		expectedStatuses[contracts.V2ContractStatusSuccessful]++
		assertContractStatus(t, contractID, contracts.V2ContractStatusRenewed)
		assertContractStatus(t, renewalID, contracts.V2ContractStatusSuccessful)
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

		if err := node.Volumes.StoreSector(root, &sector, 1); err != nil {
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
		assertStorageMetrics(t, 0, 1)

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
		assertStorageMetrics(t, 0, 1)
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
		rev.RevisionNumber++
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

		for range n {
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

func TestV2SectorRootConsistency(t *testing.T) {
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	dir := t.TempDir()
	log := zaptest.NewLogger(t)

	network, genesis := testutil.V2Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	result := make(chan error, 1)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(dir, "data.dat"), 64, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	renewContract := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, renterAllowance, collateral types.Currency) (types.FileContractID, types.V2FileContract, rhp4.TransactionSet, proto4.Usage) {
		t.Helper()

		cm := node.Chain
		con := node.Contracts
		w := node.Wallet
		cs := cm.TipState()

		renewal := types.V2FileContractRenewal{
			NewContract: types.V2FileContract{
				RevisionNumber:   0,
				Filesize:         fc.Filesize,
				Capacity:         fc.Capacity,
				FileMerkleRoot:   fc.FileMerkleRoot,
				ProofHeight:      fc.ProofHeight + 30,
				ExpirationHeight: fc.ExpirationHeight + 30,
				RenterOutput: types.SiacoinOutput{
					Address: fc.RenterOutput.Address,
					Value:   fc.RenterOutput.Value.Add(renterAllowance),
				},
				HostOutput: types.SiacoinOutput{
					Address: fc.HostOutput.Address,
					Value:   fc.HostOutput.Value.Add(collateral),
				},
				MissedHostValue: fc.MissedHostValue.Add(collateral),
				TotalCollateral: fc.TotalCollateral.Add(collateral),
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

		_, fce, err := con.V2FileContractElement(contractID)
		if err != nil {
			t.Fatal(err)
		}

		fundAmount := cs.V2FileContractTax(renewal.NewContract).Add(collateral).Add(renterAllowance)
		renewalTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &renewal,
				},
			},
		}
		basis, toSign, err := w.FundV2Transaction(&renewalTxn, fundAmount, false)
		if err != nil {
			t.Fatal("failed to fund transaction:", err)
		}
		w.SignV2Inputs(&renewalTxn, toSign)
		return contractID.V2RenewalID(), renewal.NewContract, rhp4.TransactionSet{
				Basis:        basis,
				Transactions: []types.V2Transaction{renewalTxn},
			}, proto4.Usage{
				RiskedCollateral: renewal.NewContract.RiskedCollateral(),
			}
	}

	appendSector := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, roots []types.Hash256) (types.V2FileContract, []types.Hash256) {
		t.Helper()

		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)
		roots = append(roots, root)

		if err := node.Volumes.StoreSector(root, &sector, 1); err != nil {
			t.Fatal(err)
		}

		fc.Filesize = proto4.SectorSize * uint64(len(roots))
		fc.Capacity = proto4.SectorSize * uint64(len(roots))
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		usage := proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		}
		if err := node.Contracts.ReviseV2Contract(contractID, fc, roots, usage); err != nil {
			t.Fatal(err)
		}
		return fc, roots
	}

	deleteSectors := func(t *testing.T, contractID types.FileContractID, fc types.V2FileContract, roots []types.Hash256, indices []uint64) (types.V2FileContract, []types.Hash256) {
		t.Helper()

		// normalize indices by sorting and deduplicating them
		// in descending order this mirrors the swap and trim
		// logic used by renters.
		slices.SortFunc(indices, func(a, b uint64) int {
			return cmp.Compare(b, a) // descending
		})
		indices = slices.Compact(indices)

		// ensure the roots at the specified indices are
		// actually removed and the others kept.
		kept := make(map[types.Hash256]struct{})
		deleted := make(map[types.Hash256]struct{})
		roots = slices.Clone(roots)
		for _, n := range indices {
			deleted[roots[n]] = struct{}{}
		}
		for _, root := range roots {
			if _, ok := deleted[root]; ok {
				continue
			}
			kept[root] = struct{}{}
		}
		for i, n := range indices {
			roots[n] = roots[len(roots)-1-i]
		}
		roots = roots[:len(roots)-len(indices)]

		for _, root := range roots {
			if _, ok := deleted[root]; ok {
				t.Fatalf("root %v was supposed to be deleted but is still present", root)
			}
			delete(kept, root)
		}
		if len(kept) != 0 {
			t.Fatalf("some roots were supposed to be kept but are missing: %v", kept)
		}

		fc.Filesize = proto4.SectorSize * uint64(len(roots))
		fc.Capacity = proto4.SectorSize * uint64(len(roots))
		fc.FileMerkleRoot = proto4.MetaRoot(roots)
		fc.RevisionNumber++
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		fc.RenterOutput.Value = fc.RenterOutput.Value.Sub(cost)
		fc.HostOutput.Value = fc.HostOutput.Value.Add(cost)
		fc.MissedHostValue = fc.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		usage := proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		}
		if err := node.Contracts.ReviseV2Contract(contractID, fc, roots, usage); err != nil {
			t.Fatal(err)
		}
		return fc, roots
	}

	assertRoots := func(t *testing.T, contractID types.FileContractID, expected []types.Hash256) {
		t.Helper()

		actual := node.Contracts.SectorRoots(contractID)
		if len(actual) != len(expected) {
			t.Fatalf("expected %v roots, got %v", len(expected), len(actual))
		}
		for i := range expected {
			if actual[i] != expected[i] {
				t.Fatalf("root %v: expected %v, got %v", i, expected[i], actual[i])
			}
		}
	}

	assertDBRoots := func(t *testing.T, contractID types.FileContractID, expected []types.Hash256) {
		t.Helper()

		dbRoots, err := node.Store.V2SectorRoots()
		if err != nil {
			t.Fatal("failed to load sector roots:", err)
		}
		actual := dbRoots[contractID]
		if len(actual) != len(expected) {
			t.Fatalf("expected %v db roots, got %v", len(expected), len(actual))
		}
		for i := range expected {
			if actual[i] != expected[i] {
				t.Fatalf("db root %v: expected %v, got %v", i, expected[i], actual[i])
			}
		}
	}

	t.Run("basic contract", func(t *testing.T) {
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		assertRoots(t, contractID, nil)
		assertDBRoots(t, contractID, nil)

		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		for range 3 {
			fc, roots = appendSector(t, contractID, fc, roots)
			assertRoots(t, contractID, roots)
			assertDBRoots(t, contractID, roots)
		}

		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertRoots(t, contractID, roots)

		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
	})

	t.Run("renewal inherits roots", func(t *testing.T) {
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		for range 2 {
			fc, roots = appendSector(t, contractID, fc, roots)
		}
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		renewalID, _, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)

		renewal, err := node.Contracts.V2Contract(renewalID)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
	})

	t.Run("chained renewals", func(t *testing.T) {
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		fc, roots = appendSector(t, contractID, fc, roots)
		assertRoots(t, contractID, roots)

		renewalID1, renewalFC1, renewalTxnSet1, renewalUsage1 := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet1.Basis, renewalTxnSet1.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet1, renewalUsage1); err != nil {
			t.Fatal(err)
		}
		assertRoots(t, renewalID1, roots)
		assertDBRoots(t, renewalID1, roots)

		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		renewalFC1, roots = appendSector(t, renewalID1, renewalFC1, roots)
		assertRoots(t, renewalID1, roots)
		assertDBRoots(t, renewalID1, roots)

		renewalID2, _, renewalTxnSet2, renewalUsage2 := renewContract(t, renewalID1, renewalFC1, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet2.Basis, renewalTxnSet2.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet2, renewalUsage2); err != nil {
			t.Fatal(err)
		}

		assertRoots(t, renewalID2, roots)
		assertDBRoots(t, renewalID2, roots)
		assertDBRoots(t, renewalID1, roots)

		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		renewal2, err := node.Contracts.V2Contract(renewalID2)
		if err != nil {
			t.Fatal(err)
		}
		assertRoots(t, renewalID2, roots)
		assertDBRoots(t, renewalID2, roots)
		assertDBRoots(t, renewalID1, nil)

		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal2.ExpirationHeight-node.Chain.Tip().Height)+1)
		assertDBRoots(t, renewalID1, nil)
		assertDBRoots(t, renewalID2, nil)
	})

	t.Run("rejected contract", func(t *testing.T) {
		cm := node.Chain
		w := node.Wallet

		renterFunds, hostFunds := types.Siacoins(10), types.Siacoins(20)
		cs := cm.TipState()
		fc := types.V2FileContract{
			RevisionNumber:   0,
			Filesize:         0,
			ProofHeight:      cs.Index.Height + 10,
			ExpirationHeight: cs.Index.Height + 20,
			RenterOutput:     types.SiacoinOutput{Value: renterFunds, Address: w.Address()},
			HostOutput:       types.SiacoinOutput{Value: hostFunds, Address: w.Address()},
			MissedHostValue:  hostFunds,
			TotalCollateral:  hostFunds,
			RenterPublicKey:  renterKey.PublicKey(),
			HostPublicKey:    hostKey.PublicKey(),
		}
		fundAmount := cs.V2FileContractTax(fc).Add(hostFunds).Add(renterFunds)
		sigHash := cs.ContractSigHash(fc)
		fc.HostSignature = hostKey.SignHash(sigHash)
		fc.RenterSignature = renterKey.SignHash(sigHash)

		txn := types.V2Transaction{FileContracts: []types.V2FileContract{fc}}
		basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
		if err != nil {
			t.Fatal(err)
		}
		w.SignV2Inputs(&txn, toSign)
		formationSet := rhp4.TransactionSet{
			Transactions: []types.V2Transaction{txn},
			Basis:        basis,
		}
		contractID := txn.V2FileContractID(txn.ID(), 0)
		formationSet.Transactions[0].SiacoinInputs[0].SatisfiedPolicy.Signatures[0] = types.Signature{}

		if err := node.Contracts.AddV2Contract(formationSet, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		assertRoots(t, contractID, nil)
		assertDBRoots(t, contractID, nil)

		testutil.MineAndSync(t, node, types.VoidAddress, 20)

		contract, err := node.Contracts.V2Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if contract.Status != contracts.V2ContractStatusRejected {
			t.Fatalf("expected rejected, got %v", contract.Status)
		}

		dbRoots, err := node.Store.V2SectorRoots()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := dbRoots[contractID]; ok {
			t.Fatal("rejected contract should not have roots in V2SectorRoots")
		}
	})

	t.Run("renewal with expired original", func(t *testing.T) {
		// form, add sectors, renew, then expire the original through the
		// reorg buffer. The renewal's roots should survive.
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		fc, roots = appendSector(t, contractID, fc, roots)

		renewalID, renewalFC, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// confirm the renewal
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		// mine through the reorg buffer to expire original contract's sectors
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)

		// renewal should still have roots after original is cleaned up
		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)

		// add a new sector to the renewal
		renewalFC, roots = appendSector(t, renewalID, renewalFC, roots)
		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)

		// expire everything
		renewal, err := node.Contracts.V2Contract(renewalID)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)

		_ = renewalFC
	})

	t.Run("renewal adds sectors", func(t *testing.T) {
		// form contract with a sector, renew, then add sectors to the
		// renewal. Both inherited and new sectors should be consistent.
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		fc, roots = appendSector(t, contractID, fc, roots)

		renewalID, renewalFC, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		// add multiple sectors to the renewal
		for range 3 {
			renewalFC, roots = appendSector(t, renewalID, renewalFC, roots)
		}

		// all 4 sectors should be present (1 inherited + 3 new)
		if len(roots) != 4 {
			t.Fatalf("expected 4 roots, got %v", len(roots))
		}
		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)

		// expire everything
		renewal, err := node.Contracts.V2Contract(renewalID)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)

		_ = renewalFC
	})

	t.Run("renewal replaces inherited sector", func(t *testing.T) {
		// form contract with sectors, renew, then replace an inherited
		// sector on the renewal. The replaced sector should be returned
		// correctly, not the old one from the previous revision.
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		for range 3 {
			fc, roots = appendSector(t, contractID, fc, roots)
		}
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		renewalID, renewalFC, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		// replace sector at index 0 on the renewal
		var replacementSector [proto4.SectorSize]byte
		frand.Read(replacementSector[:])
		replacementRoot := proto4.SectorRoot(&replacementSector)
		if err := node.Volumes.StoreSector(replacementRoot, &replacementSector, 1); err != nil {
			t.Fatal(err)
		}

		roots[0] = replacementRoot
		renewalFC.Filesize = proto4.SectorSize * uint64(len(roots))
		renewalFC.Capacity = proto4.SectorSize * uint64(len(roots))
		renewalFC.FileMerkleRoot = proto4.MetaRoot(roots)
		renewalFC.RevisionNumber++
		cost, collateral := types.Siacoins(1), types.Siacoins(2)
		renewalFC.RenterOutput.Value = renewalFC.RenterOutput.Value.Sub(cost)
		renewalFC.HostOutput.Value = renewalFC.HostOutput.Value.Add(cost)
		renewalFC.MissedHostValue = renewalFC.MissedHostValue.Sub(collateral)
		sigHash := node.Chain.TipState().ContractSigHash(renewalFC)
		renewalFC.HostSignature = hostKey.SignHash(sigHash)
		renewalFC.RenterSignature = renterKey.SignHash(sigHash)
		if err := node.Contracts.ReviseV2Contract(renewalID, renewalFC, roots, proto4.Usage{
			Storage:          cost,
			RiskedCollateral: collateral,
		}); err != nil {
			t.Fatal(err)
		}

		assertRoots(t, renewalID, roots)
		assertDBRoots(t, renewalID, roots)

		// expire everything
		renewal, err := node.Contracts.V2Contract(renewalID)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
	})

	t.Run("delete sectors from contract", func(t *testing.T) {
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(100), types.Siacoins(200), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		var roots []types.Hash256
		for range 10 {
			fc, roots = appendSector(t, contractID, fc, roots)
		}
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		fc, roots = deleteSectors(t, contractID, fc, roots, []uint64{1, 3, 5, 7, 9})
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertDBRoots(t, contractID, nil)
		// note: the roots cache is not cleared on expiry so not checked here
	})

	t.Run("delete sectors from renewal chain", func(t *testing.T) {
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(100), types.Siacoins(200), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		// add sectors to the original contract
		var roots []types.Hash256
		for range 10 {
			fc, roots = appendSector(t, contractID, fc, roots)
		}
		assertRoots(t, contractID, roots)

		// delete one sector before renewing
		fc, roots = deleteSectors(t, contractID, fc, roots, []uint64{2})
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)

		// renew the contract — renewal inherits remaining roots
		renewalID, renewalFC, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		// append a sector to the new contract after renewal
		renewalFC, renewalRoots := appendSector(t, renewalID, renewalFC, roots)
		// original contract should still have its roots, renewal should have inherited + new root
		assertRoots(t, contractID, roots)
		assertDBRoots(t, contractID, roots)
		assertRoots(t, renewalID, renewalRoots)
		assertDBRoots(t, renewalID, renewalRoots)

		// original contract's roots should be cleaned up after renewal is confirmed
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		assertDBRoots(t, contractID, nil) // note: roots is not checked because it doesn't get cleared on expiration.
		assertRoots(t, renewalID, renewalRoots)
		assertDBRoots(t, renewalID, renewalRoots)

		for range 3 {
			renewalFC, renewalRoots = appendSector(t, renewalID, renewalFC, renewalRoots)
		}
		assertRoots(t, renewalID, renewalRoots)
		assertDBRoots(t, renewalID, renewalRoots)

		renewalFC, renewalRoots = deleteSectors(t, renewalID, renewalFC, renewalRoots, []uint64{1, 3, 5, 7, 9, 11})
		assertRoots(t, renewalID, renewalRoots)
		assertDBRoots(t, renewalID, renewalRoots)

		// delete all remaining sectors
		indices := make([]uint64, len(renewalRoots))
		for i := range indices {
			indices[i] = uint64(i)
		}
		renewalFC, renewalRoots = deleteSectors(t, renewalID, renewalFC, renewalRoots, indices)
		assertRoots(t, renewalID, nil)
		assertDBRoots(t, renewalID, nil)

		// expire everything
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewalFC.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
		assertDBRoots(t, renewalID, nil)
		assertDBRoots(t, contractID, nil)
	})

	t.Run("empty contract no sectors", func(t *testing.T) {
		// form and confirm a contract with no sectors, then expire it.
		// Roots should always be empty.
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		assertRoots(t, contractID, nil)
		assertDBRoots(t, contractID, nil)

		testutil.MineAndSync(t, node, types.VoidAddress, int(fc.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
	})

	t.Run("empty renewal inherits empty", func(t *testing.T) {
		// renew a contract that has no sectors. Both should have empty roots.
		contractID, fc := formV2Contract(t, node.Chain, node.Contracts, node.Wallet, renterKey, hostKey, types.Siacoins(10), types.Siacoins(20), 10, true)
		testutil.MineAndSync(t, node, types.VoidAddress, 1)

		renewalID, _, renewalTxnSet, renewalUsage := renewContract(t, contractID, fc, types.ZeroCurrency, types.Siacoins(2))
		if _, err := node.Chain.AddV2PoolTransactions(renewalTxnSet.Basis, renewalTxnSet.Transactions); err != nil {
			t.Fatal(err)
		} else if err := node.Contracts.RenewV2Contract(renewalTxnSet, renewalUsage); err != nil {
			t.Fatal(err)
		}

		assertRoots(t, contractID, nil)
		assertRoots(t, renewalID, nil)
		assertDBRoots(t, contractID, nil)
		assertDBRoots(t, renewalID, nil)

		// confirm and expire
		testutil.MineAndSync(t, node, types.VoidAddress, 1)
		renewal, err := node.Contracts.V2Contract(renewalID)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MineAndSync(t, node, types.VoidAddress, int(renewal.ExpirationHeight-node.Chain.Tip().Height)+1)
		testutil.MineAndSync(t, node, types.VoidAddress, contracts.ReorgBuffer+1)
	})
}
