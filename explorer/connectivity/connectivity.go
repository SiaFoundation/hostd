package connectivity

import (
	"context"
	"errors"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	"go.uber.org/zap"
)

type (
	// Explorer is an interface to run connectivity tests on an external
	// service
	Explorer interface {
		TestConnection(ctx context.Context, host explorer.Host) (explorer.TestResult, error)
	}

	// Settings is an interface to access the host's settings, specifically
	// the RHP4 net addresses.
	Settings interface {
		RHP4NetAddresses() []chain.NetAddress
	}

	// Alerts is an interface to register and dismiss alerts related to
	// connectivity issues.
	Alerts interface {
		Register(alerts.Alert)
		DismissCategory(category string)
	}

	// An Option is a functional option to configure the connectivity
	// manager.
	Option func(*Manager)

	// Manager is a connectivity manager that periodically tests the host's
	// connectivity to renters and registers alerts if the host is not
	// reachable.
	Manager struct {
		hostKey types.PublicKey

		tg       *threadgroup.ThreadGroup
		alerts   Alerts
		settings Settings
		explorer Explorer
		log      *zap.Logger

		maxCheckInterval time.Duration
		backoffFn        func(int) time.Duration
	}
)

// Close stops the connectivity manager and cleans up any resources
// it is using.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

// WithLog sets the logger for the connectivity manager.
func WithLog(log *zap.Logger) Option {
	return func(m *Manager) {
		m.log = log
	}
}

// WithAlerts sets the alerts manager for the connectivity manager.
func WithAlerts(alerts Alerts) Option {
	return func(m *Manager) {
		m.alerts = alerts
	}
}

// WithMaxCheckInterval sets the maximum interval between connectivity tests.
// If the backoff function returns a longer duration, it will be clamped to
// this value.
//
// The default value is 2 hours.
func WithMaxCheckInterval(d time.Duration) Option {
	return func(m *Manager) {
		m.maxCheckInterval = d
	}
}

// WithBackoff sets the backoff function for the connectivity manager.
// The function should take the number of consecutive failures and return
// the duration to wait before the next connectivity test.
//
// It will be clamped to the maximum check interval set by
// WithMaxCheckInterval.
func WithBackoff(fn func(failures int) time.Duration) Option {
	return func(m *Manager) {
		m.backoffFn = fn
	}
}

// NewManager creates a new connectivity manager that periodically tests the
// host's connectivity to renters.
func NewManager(hostKey types.PublicKey, settings Settings, explorer Explorer, opts ...Option) (*Manager, error) {
	m := &Manager{
		hostKey: hostKey,

		tg:       threadgroup.New(),
		settings: settings,
		explorer: explorer,
		log:      zap.NewNop(),

		maxCheckInterval: 2 * time.Hour,
		backoffFn: func(failures int) time.Duration {
			return time.Minute * time.Duration(math.Pow(2, float64(failures)))
		},
	}
	for _, opt := range opts {
		opt(m)
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		var consecutiveFailures int
		var nextTestTime time.Duration
		for {
			select {
			case <-time.After(nextTestTime):
				result, ok, err := m.TestConnection(ctx)
				if errors.Is(err, context.Canceled) {
					return // shutdown
				} else if err != nil {
					m.log.Error("failed to test connection", zap.Error(err))
				} else {
					m.log.Debug("connection test result", zap.Bool("ok", ok), zap.Any("result", result))
				}

				if !ok {
					consecutiveFailures++
					nextTestTime = min(m.maxCheckInterval, m.backoffFn(consecutiveFailures))
				} else {
					consecutiveFailures = 0
					nextTestTime = m.maxCheckInterval
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return m, nil
}
