package settings_test

import (
	"reflect"
	"testing"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/build"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/internal/testutil"
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

	contracts, err := contracts.NewManager(node.Store, vm, node.Chain, &testutil.MockSyncer{}, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	defer contracts.Close()

	sm, err := settings.NewConfigManager(hostKey, node.Store, node.Chain, &testutil.MockSyncer{}, vm, wm, settings.WithLog(log.Named("settings")), settings.WithAnnounceInterval(50), settings.WithValidateNetAddress(false))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	idx, err := index.NewManager(node.Store, node.Chain, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(1))
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	defer idx.Close()

	// helper to derive RHP4 settings from settings.Settings
	v4Settings := func(settings settings.Settings) proto4.HostSettings {
		return proto4.HostSettings{
			ProtocolVersion:     [3]uint8{1, 0, 0},
			Release:             "hostd " + build.Version(),
			WalletAddress:       wm.Address(),
			AcceptingContracts:  settings.AcceptingContracts,
			MaxCollateral:       settings.MaxCollateral,
			MaxContractDuration: settings.MaxContractDuration,
			RemainingStorage:    0,
			TotalStorage:        0,
			Prices: proto4.HostPrices{
				ContractPrice:   settings.ContractPrice,
				StoragePrice:    settings.StoragePrice,
				Collateral:      settings.StoragePrice.Mul64(uint64(settings.CollateralMultiplier * 1000)).Div64(1000),
				IngressPrice:    settings.IngressPrice,
				EgressPrice:     settings.EgressPrice,
				FreeSectorPrice: types.Siacoins(1).Div64((1 << 40) / proto4.SectorSize), // 1 SC / TB
			},
		}
	}

	if !reflect.DeepEqual(sm.Settings(), settings.DefaultSettings) {
		t.Fatal("settings not equal to default")
	} else if !reflect.DeepEqual(sm.RHP4Settings(), v4Settings(sm.Settings())) {
		t.Fatal("rhp4 settings not equal to expected")
	}

	updated := sm.Settings()
	updated.WindowSize = 100
	updated.NetAddress = "localhost"
	updated.BaseRPCPrice = types.Siacoins(1)

	if err := sm.UpdateSettings(updated); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(sm.Settings(), updated) {
		t.Fatal("settings not equal to updated")
	} else if !reflect.DeepEqual(sm.RHP4Settings(), v4Settings(updated)) {
		t.Fatal("rhp4 settings not equal to expected")
	}
}

func TestRHP2Settings(t *testing.T) {
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

	sm, err := settings.NewConfigManager(hostKey, node.Store, node.Chain, node.Syncer, vm, wm, settings.WithLog(log.Named("settings")), settings.WithAnnounceInterval(50), settings.WithRHP2Port(1234), settings.WithRHP3Port(5678))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	updated := sm.Settings()
	updated.NetAddress = "foo.bar"
	if err := sm.UpdateSettings(updated); err != nil {
		t.Fatal(err)
	}

	r2, err := sm.RHP2Settings()
	if err != nil {
		t.Fatal(err)
	} else if r2.NetAddress != "foo.bar:1234" {
		t.Fatal("expected netaddress to have port")
	} else if r2.SiaMuxPort != "5678" {
		t.Fatal("expected siamux port to be 5678")
	}
}
