package settings_test

import (
	"net"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap/zaptest"
)

func TestAutoAnnounce(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesisBlock := testutil.V1Network()
	hostKey := types.GeneratePrivateKey()

	node := testutil.NewConsensusNode(t, network, genesisBlock, log)

	// TODO: its unfortunate that all these managers need to be created just to
	// test the auto-announce feature.
	wm, err := wallet.NewSingleAddressWallet(hostKey, node.Chain, node.Store, &testutil.MockSyncer{})
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	defer wm.Close()

	vm, err := storage.NewVolumeManager(node.Store, storage.WithLogger(log.Named("storage")))
	if err != nil {
		t.Fatal("failed to create volume manager:", err)
	}
	defer vm.Close()

	contracts, err := contracts.NewManager(node.Store, vm, node.Chain, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	defer contracts.Close()

	storage, err := storage.NewVolumeManager(node.Store)
	if err != nil {
		t.Fatal("failed to create storage manager:", err)
	}
	defer storage.Close()

	sm, err := settings.NewConfigManager(hostKey, node.Store, node.Chain, vm, wm, settings.WithLog(log.Named("settings")), settings.WithAnnounceInterval(50))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	idx, err := index.NewManager(node.Store, node.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(1))
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	defer idx.Close()

	settings := settings.DefaultSettings
	settings.NetAddress = "foo.bar"
	sm.UpdateSettings(settings)

	assertAnnouncement := func(t *testing.T, expectedHost string, height uint64) {
		t.Helper()

		index, ok := node.Chain.BestIndex(height)
		if !ok {
			t.Fatalf("failed to get index at height %v (%s)", height, node.Chain.Tip())
		}

		netaddress := net.JoinHostPort(expectedHost, "9982")
		ann, err := sm.LastAnnouncement()
		if err != nil {
			t.Fatal(err)
		} else if ann.Address != netaddress {
			t.Fatalf("expected address %q, got %q", netaddress, ann.Address)
		} else if ann.Index != index {
			t.Fatalf("expected index %q, got %q", index, ann.Index)
		}
	}

	assertV2Announcement := func(t *testing.T, expectedHost string, height uint64) {
		t.Helper()

		index, ok := node.Chain.BestIndex(height)
		if !ok {
			t.Fatal("failed to get index")
		}

		hash, announceIndex, err := node.Store.LastV2AnnouncementHash()
		if err != nil {
			t.Fatal(err)
		}

		netaddress := net.JoinHostPort(expectedHost, "9984")
		h := types.NewHasher()
		types.EncodeSlice(h.E, chain.V2HostAnnouncement{{Protocol: siamux.Protocol, Address: netaddress}})
		if err := h.E.Flush(); err != nil {
			t.Fatal(err)
		}
		expectedHash := h.Sum()

		if hash != expectedHash {
			t.Fatalf("expected hash %v, got %v", expectedHash, hash)
		} else if announceIndex != index {
			t.Fatalf("expected index %v, got %v", index, announceIndex)
		}
	}

	// helper that mines blocks and waits for them to be processed before mining
	// the next one. This is necessary because test blocks can be extremely fast
	// and the host may not have time to process the broadcast before the next
	// block is mined.
	mineAndSync := func(t *testing.T, numBlocks uint64) {
		t.Helper()

		// waits for each block to be processed before mining the next one
		for i := uint64(0); i < numBlocks; i++ {
			testutil.MineBlocks(t, node, wm.Address(), 1)
			testutil.WaitForSync(t, node.Chain, idx)
		}
	}

	// fund the wallet and trigger the first auto-announce
	mineAndSync(t, network.MaturityDelay+1+1)
	assertAnnouncement(t, "foo.bar", network.MaturityDelay+1+1) // first maturity height + funds available + confirmation
	// mine until the next announcement and confirm it
	lastHeight := node.Chain.Tip().Height
	mineAndSync(t, 51)
	assertAnnouncement(t, "foo.bar", lastHeight+50+1) // first confirm + interval + confirmation

	// change the address
	settings.NetAddress = "baz.qux"
	sm.UpdateSettings(settings)

	// trigger and confirm the new announcement
	lastHeight = node.Chain.Tip().Height
	mineAndSync(t, 2)
	assertAnnouncement(t, "baz.qux", lastHeight+2)

	// mine until the v2 hardfork activates. The host should re-announce with a
	// v2 attestation.
	n := node.Chain.TipState().Network
	mineAndSync(t, n.HardforkV2.AllowHeight-node.Chain.Tip().Height+1)
	assertV2Announcement(t, "baz.qux", n.HardforkV2.AllowHeight+1)

	// mine a few more blocks to ensure the host doesn't re-announce
	mineAndSync(t, 10)
	assertV2Announcement(t, "baz.qux", n.HardforkV2.AllowHeight+1)
}

func TestAutoAnnounceV2(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesisBlock := testutil.V2Network()
	network.HardforkV2.AllowHeight = 2
	network.HardforkV2.RequireHeight = 3
	hostKey := types.GeneratePrivateKey()

	node := testutil.NewConsensusNode(t, network, genesisBlock, log)

	// TODO: its unfortunate that all these managers need to be created just to
	// test the auto-announce feature.
	wm, err := wallet.NewSingleAddressWallet(hostKey, node.Chain, node.Store, &testutil.MockSyncer{})
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	defer wm.Close()

	vm, err := storage.NewVolumeManager(node.Store, storage.WithLogger(log.Named("storage")))
	if err != nil {
		t.Fatal("failed to create volume manager:", err)
	}
	defer vm.Close()

	contracts, err := contracts.NewManager(node.Store, vm, node.Chain, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	defer contracts.Close()

	storage, err := storage.NewVolumeManager(node.Store)
	if err != nil {
		t.Fatal("failed to create storage manager:", err)
	}
	defer storage.Close()

	sm, err := settings.NewConfigManager(hostKey, node.Store, node.Chain, vm, wm, settings.WithLog(log.Named("settings")), settings.WithAnnounceInterval(50))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	idx, err := index.NewManager(node.Store, node.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(1))
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	defer idx.Close()

	// helper that mines blocks and waits for them to be processed before mining
	// the next one. This is necessary because test blocks can be extremely fast
	// and the host may not have time to process the broadcast before the next
	// block is mined.
	mineAndSync := func(t *testing.T, numBlocks uint64) {
		t.Helper()

		// waits for each block to be processed before mining the next one
		for i := uint64(0); i < numBlocks; i++ {
			testutil.MineBlocks(t, node, wm.Address(), 1)
			testutil.WaitForSync(t, node.Chain, idx)
		}
	}

	assertV2Announcement := func(t *testing.T, expectedHost string, height uint64) {
		t.Helper()

		index, ok := node.Chain.BestIndex(height)
		if !ok {
			t.Fatal("failed to get index")
		}

		hash, announceIndex, err := node.Store.LastV2AnnouncementHash()
		if err != nil {
			t.Fatal(err)
		}

		h := types.NewHasher()
		types.EncodeSlice(h.E, chain.V2HostAnnouncement{{Protocol: siamux.Protocol, Address: net.JoinHostPort(expectedHost, "9984")}})
		if err := h.E.Flush(); err != nil {
			t.Fatal(err)
		}
		expectedHash := h.Sum()

		if hash != expectedHash {
			t.Fatalf("expected hash %v, got %v", expectedHash, hash)
		} else if announceIndex != index {
			t.Fatalf("expected index %v, got %v", index, announceIndex)
		}
	}

	settings := settings.DefaultSettings
	settings.NetAddress = "foo.bar"
	sm.UpdateSettings(settings)

	// fund the wallet and trigger the first auto-announce
	mineAndSync(t, network.MaturityDelay+1+1)
	assertV2Announcement(t, "foo.bar", network.MaturityDelay+1+1) // first maturity height + funds available + confirmation
	// mine until the next announcement and confirm it
	lastHeight := node.Chain.Tip().Height
	mineAndSync(t, 51)
	assertV2Announcement(t, "foo.bar", lastHeight+50+1) // first confirm + interval + confirmation

	// change the address
	settings.NetAddress = "baz.qux"
	sm.UpdateSettings(settings)

	// trigger and confirm the new announcement
	lastHeight = node.Chain.Tip().Height
	mineAndSync(t, 2)
	assertV2Announcement(t, "baz.qux", lastHeight+2)
}
