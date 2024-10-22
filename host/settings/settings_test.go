package settings_test

import (
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/index"
	"go.sia.tech/hostd/internal/testutil"
	"go.uber.org/zap/zaptest"
)

func TestSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesisBlock := testutil.V1Network()
	hostKey := types.GeneratePrivateKey()

	node := testutil.NewConsensusNode(t, network, genesisBlock, log)

	// TODO: its unfortunate that all these managers need to be created just to
	// test the auto-announce feature.
	wm, err := wallet.NewSingleAddressWallet(hostKey, node.Chain, node.Store)
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	defer wm.Close()

	vm, err := storage.NewVolumeManager(node.Store, storage.WithLogger(log.Named("storage")))
	if err != nil {
		t.Fatal("failed to create volume manager:", err)
	}
	defer vm.Close()

	contracts, err := contracts.NewManager(node.Store, vm, node.Chain, node.Syncer, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	defer contracts.Close()

	storage, err := storage.NewVolumeManager(node.Store)
	if err != nil {
		t.Fatal("failed to create storage manager:", err)
	}
	defer storage.Close()

	sm, err := settings.NewConfigManager(hostKey, node.Store, node.Chain, node.Syncer, wm, storage, settings.WithLog(log.Named("settings")), settings.WithAnnounceInterval(50), settings.WithValidateNetAddress(false))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	idx, err := index.NewManager(node.Store, node.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(0)) // off-by-one
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	defer idx.Close()

	if !reflect.DeepEqual(sm.Settings(), settings.DefaultSettings) {
		t.Fatal("settings not equal to default")
	}

	updated := sm.Settings()
	updated.WindowSize = 100
	updated.NetAddress = "localhost:10082"
	updated.BaseRPCPrice = types.Siacoins(1)

	if err := sm.UpdateSettings(updated); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(sm.Settings(), updated) {
		t.Fatal("settings not equal to updated")
	}
}
