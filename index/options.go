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

// WithPruneTarget sets the prune target of the manager. A prune target of 0
// means pruning is diabled. A target n > 0 means that only the last n blocks
// will be kept. On mainnet this should be set to at least 144.
func WithPruneTarget(target uint64) Option {
	return func(m *Manager) {
		m.pruneTarget = target
	}
}
