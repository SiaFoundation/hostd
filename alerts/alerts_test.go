package alerts

import (
	"reflect"
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
		Timestamp: time.Now().Truncate(time.Millisecond), // json rounds to milliseconds
	}
	// register the alert
	m.Register(expectedAlert)
	alerts := m.Active()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], expectedAlert) {
		t.Fatalf("expected alert %v, got %v", expectedAlert, alerts[0])
	}

	// update the alert
	expectedAlert.Data = map[string]any{
		"bar": "baz",
	}
	m.Register(expectedAlert)
	alerts = m.Active()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], expectedAlert) {
		t.Fatalf("expected alert %v, got %v", expectedAlert, alerts[0])
	}

	// update the alert
	expectedAlert.Data = map[string]any{
		"baz": "qux",
	}
	m.Register(expectedAlert)
	alerts = m.Active()
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	} else if !reflect.DeepEqual(alerts[0], expectedAlert) {
		t.Fatalf("expected alert %v, got %v", expectedAlert, alerts[0])
	}

	// dismiss the alert
	m.Dismiss(expectedAlert.ID)
	alerts = m.Active()
	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}
