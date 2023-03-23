package settings_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestSettings(t *testing.T) {
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()
	node, err := test.NewWallet(hostKey, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	manager, err := settings.NewConfigManager(hostKey, "localhost:9882", db, node.ChainManager(), node.TPool(), node, log.Named("settings"))
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
