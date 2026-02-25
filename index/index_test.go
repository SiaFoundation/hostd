package index_test

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap/zaptest"
)

func TestPruneTarget(t *testing.T) {
	const pruneTarget = 10
	log := zaptest.NewLogger(t)
	network, genesis := testutil.V2Network()

	hn := testutil.NewHostNode(t, types.GeneratePrivateKey(), network, genesis, log,
		index.WithPruneTarget(pruneTarget),
	)

	// mine enough blocks to trigger pruning
	totalBlocks := pruneTarget + 20
	testutil.MineAndSync(t, hn, types.VoidAddress, int(totalBlocks))
	time.Sleep(time.Second) // allow time for pruning to occur
	tip := hn.Chain.Tip()

	// blocks below (tip.Height - pruneTarget) should be pruned
	pruneHeight := tip.Height - pruneTarget
	for h := range pruneHeight {
		idx, ok := hn.Chain.BestIndex(h)
		if !ok {
			t.Fatalf("expected index at height %d", h)
		}
		if _, ok := hn.Chain.Block(idx.ID); ok {
			t.Fatalf("expected block at height %d to be pruned", h)
		}
	}

	// blocks at or above (tip.Height - pruneTarget) should still exist
	for h := pruneHeight; h <= tip.Height; h++ {
		idx, ok := hn.Chain.BestIndex(h)
		if !ok {
			t.Fatalf("expected index at height %d", h)
		}
		if _, ok := hn.Chain.Block(idx.ID); !ok {
			t.Fatalf("expected block at height %d to exist", h)
		}
	}
}
