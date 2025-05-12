package alerts

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"lukechampine.com/frand"
)

func TestAlerts(t *testing.T) {
	m := NewManager()

	expectedAlert := Alert{
		ID:        frand.Entropy256(),
		Severity:  SeverityCritical,
		Message:   "foo",
		Timestamp: time.Now(),
	}

	assertAlert := func() {
		t.Helper()

		alerts := m.Active()
		if len(alerts) != 1 {
			t.Fatalf("expected 1 alert, got %d", len(alerts))
		}
		ab, err := json.Marshal(alerts[0])
		if err != nil {
			t.Fatalf("failed to marshal alert: %v", err)
		}
		bb, err := json.Marshal(expectedAlert)
		if err != nil {
			t.Fatalf("failed to marshal expected alert: %v", err)
		} else if !bytes.Equal(ab, bb) {
			t.Fatalf("expected alert %s, got %s", string(bb), string(ab))
		}
	}

	// register the alert
	m.Register(expectedAlert)
	assertAlert()

	// update the alert
	expectedAlert.Data = map[string]any{
		"bar": "baz",
	}
	m.Register(expectedAlert)
	assertAlert()

	// update the alert
	expectedAlert.Data = map[string]any{
		"baz": "qux",
	}
	m.Register(expectedAlert)
	assertAlert()

	// dismiss the alert
	m.Dismiss(expectedAlert.ID)
	if alerts := m.Active(); len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}
