package sqlite_test

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/testutil"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap/zaptest"
)

func TestWalletMetrics(t *testing.T) {
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

	assertWalletMetrics := func(t *testing.T, db *sqlite.Store, mature types.Currency, immature types.Currency) {
		t.Helper()

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if !m.Wallet.Balance.Equals(mature) {
			t.Fatalf("expected mature balance %v, got %v", mature, m.Wallet.Balance)
		} else if !m.Wallet.ImmatureBalance.Equals(immature) {
			t.Fatalf("expected immature balance %v, got %v", immature, m.Wallet.ImmatureBalance)
		}
	}

	var expectedMature types.Currency
	expectedImmature := n1.Chain.TipState().BlockReward()

	// mine a single block to get the first block reward
	mineAndSync(t, n1, h1.Wallet.Address(), 1)
	assertWalletMetrics(t, h1.Store, expectedMature, expectedImmature)

	// mine until the first block reward matures
	mineAndSync(t, n1, types.VoidAddress, int(network.MaturityDelay))
	expectedMature = expectedImmature
	expectedImmature = types.ZeroCurrency
	assertWalletMetrics(t, h1.Store, expectedMature, expectedImmature)

	// mine a secondary chain to reorg the first chain
	n2 := testutil.NewConsensusNode(t, network, genesis, log.Named("node2"))
	testutil.MineBlocks(t, n2, types.VoidAddress, int(network.MaturityDelay*4))

	t.Log("connecting peer 2")
	if _, err := h1.Syncer.Connect(context.Background(), n2.Syncer.Addr()); err != nil {
		t.Fatal(err)
	}
	testutil.WaitForSync(t, n2.Chain, h1.Indexer)
	assertWalletMetrics(t, h1.Store, types.ZeroCurrency, types.ZeroCurrency)
}
