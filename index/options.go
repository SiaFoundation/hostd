package index

import "go.uber.org/zap"

// An Option is a functional option for the Manager.
type Option func(*Manager)

// WithLog sets the logger for the Manager.
func WithLog(l *zap.Logger) Option {
	return func(m *Manager) {
		m.log = l
	}
}

// WithBatchSize sets the batch size for chain updates.
func WithBatchSize(bs int) Option {
	return func(m *Manager) {
		m.updateBatchSize = bs
	}
}

func WithPruneTarget(target uint64) Option {
	return func(m *Manager) {
		m.pruneTarget = target
	}
}
