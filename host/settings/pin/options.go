package pin

import (
	"time"

	"go.uber.org/zap"
)

// An Option is a functional option for configuring a pin Manager.
type Option func(*Manager)

// WithLogger sets the logger for the manager.
func WithLogger(log *zap.Logger) Option {
	return func(m *Manager) {
		m.log = log
	}
}

func WithAlerts(a Alerts) Option {
	return func(m *Manager) {
		m.alerts = a
	}
}

// WithFrequency sets the frequency at which the manager updates the host's
// settings based on the current exchange rate.
func WithFrequency(frequency time.Duration) Option {
	return func(m *Manager) {
		m.frequency = frequency
	}
}

// WithSettings sets the settings manager for the manager.
func WithSettings(s SettingsManager) Option {
	return func(m *Manager) {
		m.sm = s
	}
}

// WithStore sets the store for the manager.
func WithStore(s Store) Option {
	return func(m *Manager) {
		m.store = s
	}
}

// WithExchangeRateRetriever sets the exchange rate retriever for the manager.
func WithExchangeRateRetriever(e ExchangeRateRetriever) Option {
	return func(m *Manager) {
		m.explorer = e
	}
}

// WithAverageRateWindow sets the window over which the manager calculates the
// average exchange rate.
func WithAverageRateWindow(window time.Duration) Option {
	return func(m *Manager) {
		m.rateWindow = window
	}
}
