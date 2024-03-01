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
	"go.sia.tech/hostd/webhooks"
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
	if err := node.MineBlocks(node.Address(), 99); err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("expected an announcement, got %v", lastAnnouncement.Index.Height)
	} else if lastAnnouncement.Address != "foo.bar:1234" {
		t.Fatal("announcement not updated")
	}
	lastHeight := lastAnnouncement.Index.Height

	remainingBlocks := lastHeight + 100 - node.ChainManager().TipState().Index.Height
	t.Log("remaining blocks:", remainingBlocks)

	// mine until right before the next announcement to ensure that the
	// announcement is not triggered early
	if err := node.MineBlocks(node.Address(), int(remainingBlocks-1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	lastAnnouncement, err = manager.LastAnnouncement()
	if err != nil {
		t.Fatal(err)
	} else if lastAnnouncement.Index.Height != lastHeight {
		t.Fatal("announcement triggered unexpectedly")
	}

	// trigger an auto-announce
	if err := node.MineBlocks(node.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	// confirm the announcement
	if err := node.MineBlocks(node.Address(), 2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	prevHeight := lastAnnouncement.Index.Height
	lastAnnouncement, err = manager.LastAnnouncement()
	if err != nil {
		t.Fatal(err)
	} else if lastAnnouncement.Index.Height <= prevHeight {
		t.Fatalf("expected a new announcement after %v, got %v", prevHeight, lastAnnouncement.Index.Height)
	} else if lastAnnouncement.Address != "foo.bar:1234" {
		t.Fatal("announcement not updated")
	}
}
