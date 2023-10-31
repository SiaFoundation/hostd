package settings_test

import (
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestAutoAnnounce(t *testing.T) {
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()
	log := zaptest.NewLogger(t)
	node, err := test.NewWallet(hostKey, dir, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	// fund the wallet
	if err := node.MineBlocks(node.Address(), 20); err != nil {
		t.Fatal(err)
	}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	a := alerts.NewManager()
	manager, err := settings.NewConfigManager(dir, hostKey, "localhost:9882", db, node.ChainManager(), node.TPool(), node, a, log.Named("settings"))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	settings := settings.DefaultSettings
	settings.NetAddress = "foo.bar:1234"
	manager.UpdateSettings(settings)

	// trigger an auto-announce
	if err := node.MineBlocks(node.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	// confirm the announcement
	if err := node.MineBlocks(node.Address(), 5); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	lastAnnouncement, err := manager.LastAnnouncement()
	if err != nil {
		t.Fatal(err)
	} else if lastAnnouncement.Index.Height == 0 {
		t.Fatal("announcement not recorded")
	} else if lastAnnouncement.Address != "foo.bar:1234" {
		t.Fatal("announcement not updated")
	}
	lastHeight := lastAnnouncement.Index.Height

	// mine more blocks to ensure another announcement is not triggered
	if err := node.MineBlocks(node.Address(), 10); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	lastAnnouncement, err = manager.LastAnnouncement()
	if err != nil {
		t.Fatal(err)
	} else if lastAnnouncement.Index.Height != lastHeight {
		t.Fatal("announcement triggered unexpectedly")
	}
}
