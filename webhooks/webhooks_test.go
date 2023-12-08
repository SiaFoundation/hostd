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

	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap/zaptest"
)

type jsonEvent struct {
	ID    webhooks.UID    `json:"id"`
	Event string          `json:"event"`
	Scope string          `json:"scope"`
	Data  json.RawMessage `json:"data"`
}

func TestWebHooks(t *testing.T) {
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

	// create a listener for the webhook
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// add a webhook
	scopes := []string{"tld", "scope/subscope"}
	hook, err := wr.RegisterWebHook("http://"+l.Addr().String(), scopes)
	if err != nil {
		t.Fatal(err)
	}

	// create an http server to listen for the webhook
	recv := make(chan jsonEvent, 1)
	go func() {
		http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, password, ok := r.BasicAuth()
			if !ok || password != hook.SecretKey {
				t.Error("bad auth")
			}

			// handle the webhook
			var event jsonEvent
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				t.Error(err)
			}

			w.WriteHeader(http.StatusNoContent)
			recv <- event
		}))
	}()

	checkEvent := func(event, scope, data string) error {
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
	hook, err = wr.UpdateWebHook(hook.ID, "http://"+l.Addr().String(), []string{"all"})
	if err != nil {
		t.Fatal(err)
	} else if hooks, err := wr.WebHooks(); err != nil {
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
	if err := wr.RemoveWebHook(hook.ID); err != nil {
		t.Fatal(err)
	} else if hooks, err := wr.WebHooks(); err != nil {
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
