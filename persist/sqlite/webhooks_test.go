package sqlite

import (
	"path/filepath"
	"reflect"
	"testing"

	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap/zaptest"
)

func TestWebhooks(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	wh1 := webhooks.Webhook{
		Scope: []string{"alert"},
		URL:   "http://example.com",
	}
	wh2 := webhooks.Webhook{
		Scope: []string{"alert"},
		URL:   "http://example2.com",
	}

	if err := db.AddWebhook(wh1); err != nil {
		t.Fatal(err)
	}

	whs, err := db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !reflect.DeepEqual(whs[0], wh1) {
		t.Fatal("unexpected webhook", whs[0])
	}

	wh, err := db.GetWebhook(1)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(whs[0], wh) {
		t.Fatal("webhooks don't match")
	}

	// Add it again. Should be a no-op.
	if err := db.AddWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err = db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !reflect.DeepEqual(whs[0], wh1) {
		t.Fatal("unexpected webhook", whs[0])
	}

	// Add a webhook with the same hook url, but different scope. Should update scope.
	wh1.Scope = []string{"info"}
	if err := db.AddWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err = db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !reflect.DeepEqual(whs[0], wh1) {
		t.Fatal("unexpected webhook", whs[0])
	}

	// Add another.
	if err := db.AddWebhook(wh2); err != nil {
		t.Fatal(err)
	}
	whs, err = db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 2 {
		t.Fatal("expected 2 webhooks", len(whs))
	} else if !reflect.DeepEqual(whs[0], wh1) {
		t.Fatal("unexpected webhook", whs[0])
	} else if !reflect.DeepEqual(whs[1], wh2) {
		t.Fatal("unexpected webhook", whs[1])
	}

	// Remove one.
	if err := db.DeleteWebhook(wh1); err != nil {
		t.Fatal(err)
	}
	whs, err = db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !reflect.DeepEqual(whs[0], wh2) {
		t.Fatal("unexpected webhook", whs[0])
	}

	// Remove the same hook again.
	if err := db.DeleteWebhook(wh1); err != ErrWebhookNotFound {
		t.Fatal("expected error ErrWebhookNotFound, got %w", err)
	}
	whs, err = db.Webhooks()
	if err != nil {
		t.Fatal(err)
	} else if len(whs) != 1 {
		t.Fatal("expected 1 webhook")
	} else if !reflect.DeepEqual(whs[0], wh2) {
		t.Fatal("unexpected webhook", whs[0])
	}
}
