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
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/certificates"
	"go.sia.tech/hostd/v2/certificates/providers/selfsigned"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/persist/sqlite"
	"go.uber.org/zap"
)

type (
	// A MockSyncer is a syncer that does nothing. It is used in tests to avoid
	// the peer check
	MockSyncer struct{}

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

		Certs     certificates.Provider
		Settings  *settings.ConfigManager
		Wallet    *wallet.SingleAddressWallet
		Contracts *contracts.Manager
		Volumes   *storage.VolumeManager
		Indexer   *index.Manager
	}
)

// BroadcastV2TransactionSet implements the syncer.Syncer interface
func (MockSyncer) BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction) error {
	return nil
}

// BroadcastTransactionSet implements the syncer.Syncer interface
func (MockSyncer) BroadcastTransactionSet([]types.Transaction) error {
	return nil
}

// V1Network is a test helper that returns a consensus.Network and genesis block
// suited for testing the v1 network
func V1Network() (*consensus.Network, types.Block) {
	return testutil.Network()
}

// V2Network is a test helper that returns a consensus.Network and genesis block
// suited for testing after the v2 hardfork
func V2Network() (*consensus.Network, types.Block) {
	return testutil.V2Network()
}

// WaitForSync is a helper to wait for the chain and indexer to sync
func WaitForSync(t testing.TB, cm *chain.Manager, idx *index.Manager) {
	t.Helper()

	for {
		if cm.Tip() == idx.Tip() {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

// MineBlocks is a helper to mine blocks and broadcast the headers
func MineBlocks(t testing.TB, cn *ConsensusNode, addr types.Address, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		b, ok := coreutils.MineBlock(cn.Chain, addr, 5*time.Second)
		if !ok {
			t.Fatal("failed to mine block")
		} else if err := cn.Chain.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}

		if b.V2 != nil {
			cn.Syncer.BroadcastV2BlockOutline(gateway.OutlineBlock(b, cn.Chain.PoolTransactions(), cn.Chain.V2PoolTransactions()))
		}
	}
}

// MineAndSync is a helper to mine blocks and wait for the index to catch up
// between each block
func MineAndSync(t testing.TB, hn *HostNode, addr types.Address, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		MineBlocks(t, &hn.ConsensusNode, addr, 1)
		WaitForSync(t, hn.Chain, hn.Indexer)
	}
}

// NewConsensusNode initializes all of the consensus components and returns them.
// The function will clean up all resources when the test is done.
func NewConsensusNode(t testing.TB, network *consensus.Network, genesis types.Block, log *zap.Logger) *ConsensusNode {
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

	cs, tipState, err := chain.NewDBStore(chainDB, network, genesis, nil)
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
	go syncer.Run()
	t.Cleanup(func() { syncer.Close() })

	return &ConsensusNode{
		Store:  db,
		Chain:  cm,
		Syncer: syncer,
	}
}

// NewHostNode initializes all of the hostd components and returns them. The function
// will clean up all resources when the test is done.
func NewHostNode(t testing.TB, pk types.PrivateKey, network *consensus.Network, genesis types.Block, log *zap.Logger) *HostNode {
	t.Helper()

	cn := NewConsensusNode(t, network, genesis, log)

	wm, err := wallet.NewSingleAddressWallet(pk, cn.Chain, cn.Store, &MockSyncer{})
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	t.Cleanup(func() { wm.Close() })

	vm, err := storage.NewVolumeManager(cn.Store, storage.WithLogger(log.Named("storage")), storage.WithPruneInterval(30*time.Second))
	if err != nil {
		t.Fatal("failed to create volume manager:", err)
	}
	t.Cleanup(func() { vm.Close() })

	contracts, err := contracts.NewManager(cn.Store, vm, cn.Chain, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	t.Cleanup(func() { contracts.Close() })

	certs, err := selfsigned.New()
	if err != nil {
		t.Fatal("failed to create certificate provider:", err)
	}

	initialSettings := settings.DefaultSettings
	initialSettings.AcceptingContracts = true
	initialSettings.NetAddress = "127.0.0.1"
	initialSettings.WindowSize = 10
	sm, err := settings.NewConfigManager(pk, cn.Store, cn.Chain, vm, wm, settings.WithAnnounceInterval(10), settings.WithValidateNetAddress(false), settings.WithInitialSettings(initialSettings), settings.WithCertificates(certs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sm.Close() })

	idx, err := index.NewManager(cn.Store, cn.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(1))
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	t.Cleanup(func() { idx.Close() })

	return &HostNode{
		ConsensusNode: *cn,

		Certs:     certs,
		Settings:  sm,
		Wallet:    wm,
		Contracts: contracts,
		Volumes:   vm,
		Indexer:   idx,
	}
}
