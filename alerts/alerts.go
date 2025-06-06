package alerts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	// SeverityInfo indicates that the alert is informational.
	SeverityInfo Severity = iota + 1
	// SeverityWarning indicates that the alert is a warning.
	SeverityWarning
	// SeverityError indicates that the alert is an error.
	SeverityError
	// SeverityCritical indicates that the alert is critical.
	SeverityCritical

	severityInfoStr     = "info"
	severityWarningStr  = "warning"
	severityErrorStr    = "error"
	severityCriticalStr = "critical"
)

type (
	// Severity indicates the severity of an alert.
	Severity uint8

	// An EventReporter broadcasts events to subscribers.
	EventReporter interface {
		BroadcastEvent(event string, scope string, data any) error
	}

	// An Alerter is an interface that registers and dismisses alerts.
	Alerter interface {
		Register(a Alert)
		Dismiss(ids ...types.Hash256)
	}

	// An Alert is a dismissible message that is displayed to the user.
	Alert struct {
		// ID is a unique identifier for the alert.
		ID types.Hash256 `json:"id"`
		// Category is the category of the alert. This is used to group
		// alerts together
		Category string `json:"category"`
		// Severity is the severity of the alert.
		Severity Severity `json:"severity"`
		// Message is a human-readable message describing the alert.
		Message string `json:"message"`
		// Data is a map of arbitrary data that can be used to provide
		// additional context to the alert.
		Data      map[string]any `json:"data"`
		Timestamp time.Time      `json:"timestamp"`
	}

	// A Manager manages the host's alerts.
	Manager struct {
		log    *zap.Logger
		events EventReporter

		mu sync.Mutex
		// alerts is a map of alert IDs to their current alert.
		alerts map[types.Hash256]Alert
	}
)

var _ Alerter = (*Manager)(nil)

// String implements the fmt.Stringer interface.
func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return severityInfoStr
	case SeverityWarning:
		return severityWarningStr
	case SeverityError:
		return severityErrorStr
	case SeverityCritical:
		return severityCriticalStr
	default:
		panic(fmt.Sprintf("unrecognized severity %d", s))
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`%q`, s.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *Severity) UnmarshalJSON(b []byte) error {
	status := strings.Trim(string(b), `"`)
	switch status {
	case severityInfoStr:
		*s = SeverityInfo
	case severityWarningStr:
		*s = SeverityWarning
	case severityErrorStr:
		*s = SeverityError
	case severityCriticalStr:
		*s = SeverityCritical
	default:
		return fmt.Errorf("unrecognized severity: %v", status)
	}
	return nil
}

// DeepCopy creates a deep copy of the Alert.
// This is necessary because the Data field is a map, which is a reference type
// and can lead to concurrent map access issues if not copied properly.
func (a Alert) DeepCopy() (b Alert) {
	// Perform a JSON round-trip to create a deep copy of the Data field.
	// This avoids concurrent map access issues and ensures nested reference
	// types are also copied.
	buf, err := json.Marshal(a)
	if err != nil {
		panic(fmt.Errorf("failed to marshal alert: %w", err)) // developer error
	} else if err := json.Unmarshal(buf, &b); err != nil {
		panic(fmt.Errorf("failed to unmarshal alert: %w", err)) // developer error
	}
	return
}

// Register registers a new alert with the manager
func (m *Manager) Register(a Alert) {
	if a.ID == (types.Hash256{}) {
		panic("cannot register alert with empty ID") // developer error
	} else if a.Timestamp.IsZero() {
		panic("cannot register alert with zero timestamp") // developer error
	}

	if a.Data == nil {
		a.Data = make(map[string]any)
	}

	if m.events != nil {
		if err := m.events.BroadcastEvent("alert", "alerts."+a.Severity.String(), a.DeepCopy()); err != nil {
			m.log.Error("failed to broadcast alert", zap.Error(err))
		}
	}

	m.mu.Lock()
	m.alerts[a.ID] = a.DeepCopy()
	m.mu.Unlock()
}

// Dismiss removes the alerts with the given IDs.
func (m *Manager) Dismiss(ids ...types.Hash256) {
	m.mu.Lock()
	for _, id := range ids {
		delete(m.alerts, id)
	}
	m.mu.Unlock()
}

// DismissCategory removes all alerts in the given category.
func (m *Manager) DismissCategory(category string) {
	m.mu.Lock()
	for id, alert := range m.alerts {
		if alert.Category == category {
			delete(m.alerts, id)
		}
	}
	m.mu.Unlock()
}

// Active returns the host's active alerts.
func (m *Manager) Active() []Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	alerts := make([]Alert, 0, len(m.alerts))
	for _, alert := range m.alerts {
		alerts = append(alerts, alert.DeepCopy())
	}
	// Sort by timestamp in desc order.
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].Timestamp.After(alerts[j].Timestamp)
	})
	return alerts
}

// NewManager initializes a new alerts manager.
func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		alerts: make(map[types.Hash256]Alert),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}
