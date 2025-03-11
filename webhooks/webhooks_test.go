package webhooks_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/hostd/v2/persist/sqlite"
	"go.sia.tech/hostd/v2/webhooks"
	"go.uber.org/zap/zaptest"
)

type jsonEvent struct {
	ID    webhooks.UID    `json:"id"`
	Event string          `json:"event"`
	Scope string          `json:"scope"`
	Data  json.RawMessage `json:"data"`
	Error error           `json:"-"`
}

func registerWebhook(t testing.TB, wr *webhooks.Manager, scopes []string) (webhooks.Webhook, <-chan jsonEvent, error) {
	// create a listener for the webhook
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return webhooks.Webhook{}, nil, fmt.Errorf("failed to create listener: %w", err)
	}
	t.Cleanup(func() {
		l.Close()
	})

	// add a webhook
	hook, err := wr.RegisterWebhook("http://"+l.Addr().String(), scopes)
	if err != nil {
		return webhooks.Webhook{}, nil, fmt.Errorf("failed to register webhook: %w", err)
	}

	// create an http server to listen for the webhook
	recv := make(chan jsonEvent, 1)
	go func() {
		http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, password, ok := r.BasicAuth()
			if !ok || password != hook.SecretKey {
				w.WriteHeader(http.StatusUnauthorized)
				recv <- jsonEvent{Error: errors.New("bad auth")}
				return
			}

			// handle the webhook
			var event jsonEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				recv <- jsonEvent{Error: fmt.Errorf("failed to decode webhook: %w", err)}
				return
			}

			w.WriteHeader(http.StatusNoContent)
			recv <- event
		}))
	}()
	return hook, recv, nil
}

func TestWebhooks(t *testing.T) {
	log := zaptest.NewLogger(t)

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	wr, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		t.Fatal(err)
	}

	// add a webhook
	hook, hook1Ch, err := registerWebhook(t, wr, []string{"tld", "scope/subscope"})
	if err != nil {
		t.Fatal(err)
	}

	checkEvent := func(event, scope, data string) error {
		select {
		case <-time.After(time.Second):
			return errors.New("timed out")
		case ev := <-hook1Ch:
			switch {
			case ev.Event != event:
				return fmt.Errorf("expected event %q, got %q", event, ev.Event)
			case ev.Scope != scope:
				return fmt.Errorf("expected scope %q, got %q", scope, ev.Scope)
			case string(ev.Data) != data:
				return fmt.Errorf("expected data %q, got %q", data, ev.Data)
			}
		}
		return nil
	}

	tests := []struct {
		event, scope string
		receive      bool
	}{
		{"test", "tld", true},            // direct match
		{"test", "scope/subscope", true}, // direct match
		{"test", "tld/subscope", true},   // subscope match
		{"test", "scope", false},         // no match
		{"test", "all", false},           // no match
	}

	for _, test := range tests {
		if err := wr.BroadcastEvent(test.event, test.scope, "hello, world!"); err != nil {
			t.Fatal(err)
		}

		if err := checkEvent(test.event, test.scope, `"hello, world!"`); test.receive && err != nil {
			t.Fatal(err)
		} else if !test.receive && err == nil {
			t.Fatal("expected no event")
		}
	}

	// update the webhook to have the "all scope"
	hook, err = wr.UpdateWebhook(hook.ID, hook.CallbackURL, []string{"all"})
	if err != nil {
		t.Fatal(err)
	} else if hooks, err := wr.Webhooks(); err != nil {
		t.Fatal(err)
	} else if len(hooks) != 1 {
		t.Fatal("expected 1 webhook")
	}

	// ensure all events are received
	for _, test := range tests {
		if err := wr.BroadcastEvent(test.event, test.scope, "hello, world!"); err != nil {
			t.Fatal(err)
		} else if err := checkEvent(test.event, test.scope, `"hello, world!"`); err != nil {
			t.Fatal(err)
		}
	}

	// unregister the webhook
	if err := wr.RemoveWebhook(hook.ID); err != nil {
		t.Fatal(err)
	} else if hooks, err := wr.Webhooks(); err != nil {
		t.Fatal(err)
	} else if len(hooks) != 0 {
		t.Fatal("expected no webhooks")
	}

	// ensure no more events are received
	for _, test := range tests {
		if err := wr.BroadcastEvent(test.event, test.scope, "hello, world!"); err != nil {
			t.Fatal(err)
		} else if err := checkEvent(test.event, test.scope, `"hello, world!"`); err == nil {
			t.Fatal("expected no event")
		}
	}
}

func TestBroadcastToWebhook(t *testing.T) {
	log := zaptest.NewLogger(t)

	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	wr, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		t.Fatal(err)
	}

	checkEvent := func(recv <-chan jsonEvent, event, scope, data string) error {
		select {
		case <-time.After(time.Second):
			return errors.New("timed out")
		case ev := <-recv:
			switch {
			case ev.Event != event:
				return fmt.Errorf("expected event %q, got %q", event, ev.Event)
			case ev.Scope != scope:
				return fmt.Errorf("expected scope %q, got %q", scope, ev.Scope)
			case string(ev.Data) != data:
				return fmt.Errorf("expected data %q, got %q", data, ev.Data)
			}
		}
		return nil
	}

	hook1, hook1Ch, err := registerWebhook(t, wr, []string{"all"})
	if err != nil {
		t.Fatal(err)
	}

	_, hook2Ch, err := registerWebhook(t, wr, []string{"all"})
	if err != nil {
		t.Fatal(err)
	}

	// broadcast to hook1
	if err := wr.BroadcastToWebhook(hook1.ID, "test", "test", "hello, world!"); err != nil {
		t.Fatal(err)
	}

	// check that hook 2 did not receive the event
	if err := checkEvent(hook2Ch, "test", "test", `"hello, world!"`); err == nil {
		t.Fatal("expected no event")
	}

	// check that hook 1 did receive the event
	if err := checkEvent(hook1Ch, "test", "test", `"hello, world!"`); err != nil {
		t.Fatal(err)
	}
}
