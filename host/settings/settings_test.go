package settings_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSettings(t *testing.T) {
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()
	log := zaptest.NewLogger(t)
	node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	webhookReporter, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		t.Fatal(err)
	}

	am := alerts.NewManager(webhookReporter, log.Named("alerts"))
	manager, err := settings.NewConfigManager(settings.WithHostKey(hostKey),
		settings.WithStore(db),
		settings.WithChainManager(node.ChainManager()),
		settings.WithTransactionPool(node.TPool()),
		settings.WithWallet(node),
		settings.WithAlertManager(am),
		settings.WithLog(log.Named("settings")))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	if !reflect.DeepEqual(manager.Settings(), settings.DefaultSettings) {
		t.Fatal("settings not equal to default")
	}

	updated := manager.Settings()
	updated.WindowSize = 100
	updated.NetAddress = "localhost:10082"
	updated.BaseRPCPrice = types.Siacoins(1)

	if err := manager.UpdateSettings(updated); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(manager.Settings(), updated) {
		t.Fatal("settings not equal to updated")
	}
}
