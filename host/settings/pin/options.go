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

// WithAlerts sets the alerts manager for the pinner to register alerts with.
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

// WithAverageRateWindow sets the window over which the manager calculates the
// average exchange rate.
func WithAverageRateWindow(window time.Duration) Option {
	return func(m *Manager) {
		m.rateWindow = window
	}
}
