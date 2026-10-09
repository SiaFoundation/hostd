package rhp_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap"
)

// mineEmptyBlock mines a block containing no transactions.
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

// TestV2RejectedRenewalRetainsSectorRoots covers a renewal rejected for
// confirming later than the reject buffer. Its sector roots must survive both
// the rejection and the revival.
func TestV2RejectedRenewalRetainsSectorRoots(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	log := zap.NewNop()
	hn := testutil.NewHostNode(t, hostKey, n, genesis, log)
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}
	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, log)
	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	fs := &fundAndSign{w, renterKey}

	appendSectors := func(t *testing.T, rev rhp4.ContractRevision, count int) rhp4.ContractRevision {
		t.Helper()
		at := proto4.NewAccountToken(renterKey, hostKey.PublicKey())
		at.Signature = renterKey.SignHash(at.SigHash())
		roots := make([]types.Hash256, 0, count)
		for i := range count {
			data := bytes.Repeat([]byte{byte(i + 1)}, proto4.LeafSize)
			wRes, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, at, bytes.NewReader(data), proto4.LeafSize)
			if err != nil {
				t.Fatal(err)
			}
			roots = append(roots, wRes.Root)
		}
		aRes, err := rhp4.RPCAppendSectors(context.Background(), transport, renterKey, cm.TipState(), settings.Prices, rev, roots)
		if err != nil {
			t.Fatal(err)
		}
		rev.Revision = aRes.Revision
		return rev
	}

	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fs, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       types.Siacoins(100),
		Collateral:      types.Siacoins(200),
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	parent := formResult.Contract
	testutil.MineAndSync(t, hn, types.VoidAddress, 10)

	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cm.TipState(), renterKey, parent, []proto4.AccountDeposit{
		{Account: proto4.Account(renterKey.PublicKey()), Amount: types.Siacoins(25)},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent.Revision = fundResult.Revision
	parent = appendSectors(t, parent, 2)

	renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fs, cm.TipState(), settings.Prices, settings.WalletAddress, parent.Revision, proto4.RPCRenewContractParams{
		ContractID:  parent.ID,
		Allowance:   types.Siacoins(150),
		Collateral:  types.Siacoins(300),
		ProofHeight: parent.Revision.ProofHeight + 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	child := appendSectors(t, renewResult.Contract, 3)
	expectedSectors := child.Revision.Filesize / proto4.SectorSize

	// mine past the reject buffer without confirming the renewal
	for range 12 {
		b := mineEmptyBlock(cm.TipState(), types.VoidAddress)
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
		testutil.WaitForSync(t, cm, hn.Indexer)
	}
	if c, err := hn.Contracts.V2Contract(child.ID); err != nil {
		t.Fatal(err)
	} else if c.Status != contracts.V2ContractStatusRejected {
		t.Fatalf("expected the renewal to be rejected, got %q", c.Status)
	}

	testutil.MineAndSync(t, hn, types.VoidAddress, 2)
	c, err := hn.Contracts.V2Contract(child.ID)
	if err != nil {
		t.Fatal(err)
	} else if c.Status != contracts.V2ContractStatusActive {
		t.Fatalf("expected the renewal to be revived, got %q", c.Status)
	}

	// the roots must survive in the store, not just in the cache
	stored, err := hn.Store.V2SectorRoots(0)
	if err != nil {
		t.Fatal(err)
	} else if uint64(len(stored[child.ID])) != expectedSectors {
		t.Fatalf("expected %d sector roots in the store, got %d", expectedSectors, len(stored[child.ID]))
	}

	// restarting reloads the roots from the store
	restartHostWithWalletKey(t, hn, hostKey, hostKey, log)
	transport = testRenterHostPair(t, hostKey, hn, log)

	if roots := hn.Contracts.SectorRoots(child.ID); uint64(len(roots)) != expectedSectors {
		t.Fatalf("expected %d sector roots after restart, got %d", expectedSectors, len(roots))
	}

	latest, err := rhp4.RPCLatestRevision(context.Background(), transport, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	settings, err = rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	appendSectors(t, rhp4.ContractRevision{ID: child.ID, Revision: latest.Contract}, 1)
}
