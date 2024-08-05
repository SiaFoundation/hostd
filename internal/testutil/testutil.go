package testutil

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/index"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
)

type (
	// A ConsensusNode is a node with the core consensus components
	ConsensusNode struct {
		Store  *sqlite.Store
		Chain  *chain.Manager
		Syncer *syncer.Syncer
	}

	// A HostNode is a node with the core wallet components and the host
	// components
	HostNode struct {
		ConsensusNode

		Settings  *settings.ConfigManager
		Wallet    *wallet.SingleAddressWallet
		Contracts *contracts.Manager
		Volumes   *storage.VolumeManager
		Indexer   *index.Manager

		Accounts *accounts.AccountManager
		Registry *registry.Manager
	}
)

// V1Network is a test helper that returns a consensus.Network and genesis block
// suited for testing the v1 network
func V1Network() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesisBlock := chain.TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 500 // comfortably above MaturityHeight
	n.HardforkV2.RequireHeight = 600
	return n, genesisBlock
}

// V2Network is a test helper that returns a consensus.Network and genesis block
// suited for testing after the v2 hardfork
func V2Network() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesisBlock := chain.TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 145 // just above the maturity height
	n.HardforkV2.RequireHeight = 180
	return n, genesisBlock
}

// WaitForSync is a helper to wait for the chain and indexer to sync
func WaitForSync(t *testing.T, cm *chain.Manager, idx *index.Manager) {
	t.Helper()

	for {
		if cm.Tip() == idx.Tip() {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

// MineAndSync is a helper to mine blocks and wait for the index to catch up
func MineAndSync(t *testing.T, cm *chain.Manager, idx *index.Manager, addr types.Address, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		b, ok := coreutils.MineBlock(cm, addr, 5*time.Second)
		if !ok {
			t.Fatal("failed to mine block")
		} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}

		WaitForSync(t, cm, idx)
	}
}

// NewConsensusNode initializes all of the consensus components and returns them.
// The function will clean up all resources when the test is done.
func NewConsensusNode(t *testing.T, network *consensus.Network, genesis types.Block, log *zap.Logger) *ConsensusNode {
	t.Helper()

	dir := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.sqlite3"), log.Named("sqlite"))
	if err != nil {
		t.Fatal("failed to open sqlite store:", err)
	}
	t.Cleanup(func() { db.Close() })

	chainDB, err := coreutils.OpenBoltChainDB(filepath.Join(dir, "consensus.db"))
	if err != nil {
		t.Fatal("failed to open chain db:", err)
	}
	t.Cleanup(func() { chainDB.Close() })

	cs, tipState, err := chain.NewDBStore(chainDB, network, genesis)
	if err != nil {
		t.Fatal("failed to create chain store:", err)
	}
	cm := chain.NewManager(cs, tipState)

	syncerListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal("failed to listen:", err)
	}
	t.Cleanup(func() { syncerListener.Close() })

	ps, err := sqlite.NewPeerStore(db)
	if err != nil {
		t.Fatal("failed to create peer store:", err)
	}

	syncer := syncer.New(syncerListener, cm, ps, gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerListener.Addr().String(),
	})
	t.Cleanup(func() { syncer.Close() })

	return &ConsensusNode{
		Store:  db,
		Chain:  cm,
		Syncer: syncer,
	}
}

// NewHostNode initializes all of the hostd components and returns them. The function
// will clean up all resources when the test is done.
func NewHostNode(t *testing.T, pk types.PrivateKey, network *consensus.Network, genesis types.Block, log *zap.Logger) *HostNode {
	t.Helper()

	cn := NewConsensusNode(t, network, genesis, log)

	wm, err := wallet.NewSingleAddressWallet(pk, cn.Chain, cn.Store)
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	t.Cleanup(func() { wm.Close() })

	vm, err := storage.NewVolumeManager(cn.Store, storage.WithLogger(log.Named("storage")))
	if err != nil {
		t.Fatal("failed to create volume manager:", err)
	}
	t.Cleanup(func() { vm.Close() })

	contracts, err := contracts.NewManager(cn.Store, vm, cn.Chain, cn.Syncer, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	t.Cleanup(func() { contracts.Close() })

	sm, err := settings.NewConfigManager(pk, cn.Store, cn.Chain, cn.Syncer, wm)
	if err != nil {
		t.Fatal(err)
	}

	idx, err := index.NewManager(cn.Store, cn.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(0)) // off-by-one
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	t.Cleanup(func() { idx.Close() })

	am := accounts.NewManager(cn.Store, sm)
	rm := registry.NewManager(pk, cn.Store, log.Named("registry"))
	t.Cleanup(func() { rm.Close() })

	return &HostNode{
		ConsensusNode: *cn,

		Settings:  sm,
		Wallet:    wm,
		Contracts: contracts,
		Volumes:   vm,
		Indexer:   idx,

		Accounts: am,
		Registry: rm,
	}
}