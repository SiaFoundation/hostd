package contracts

import "go.uber.org/zap"

// A ManagerOption sets options on a Manager.
type ManagerOption func(*Manager)

// WithRejectAfter sets the number of blocks before a contract will be considered
// rejected
func WithRejectAfter(rejectBuffer uint64) ManagerOption {
	return func(m *Manager) {
		m.rejectBuffer = rejectBuffer
	}
}

// WithRevisionSubmissionBuffer sets the number of blocks before the proof window
// to broadcast the final revision and prevent modification of the contract.
func WithRevisionSubmissionBuffer(revisionSubmissionBuffer uint64) ManagerOption {
	return func(m *Manager) {
		m.revisionSubmissionBuffer = revisionSubmissionBuffer
	}
}

// WithAlerter sets the alerts for the Manager.
func WithAlerter(a Alerts) ManagerOption {
	return func(m *Manager) {
		m.alerts = a
	}
}

// WithLog sets the logger for the Manager.
func WithLog(l *zap.Logger) ManagerOption {
	return func(m *Manager) {
		m.log = l
	}
}
