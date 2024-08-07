package alerts

import "go.uber.org/zap"

// ManagerOption is a functional option for the alert manager.
type ManagerOption func(*Manager) error

// WithLog sets the logger for the manager.
func WithLog(l *zap.Logger) ManagerOption {
	return func(m *Manager) error {
		m.log = l
		return nil
	}
}

// WithEventReporter sets the event reporter for the manager.
func WithEventReporter(e EventReporter) ManagerOption {
	return func(m *Manager) error {
		m.events = e
		return nil
	}
}
