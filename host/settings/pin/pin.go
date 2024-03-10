package pin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/explorer"
	"go.uber.org/zap"
)

type (
	// PinnedSettings contains the settings that can be optionally
	// pinned to an external currency. This uses an external explorer
	// to retrieve the current exchange rate.
	PinnedSettings struct {
		Currency  string          `json:"currency"`
		Threshold decimal.Decimal `json:"threshold"`

		Storage decimal.Decimal `json:"storage"`
		Ingress decimal.Decimal `json:"ingress"`
		Egress  decimal.Decimal `json:"egress"`

		MaxCollateral decimal.Decimal `json:"maxCollateral"`
	}

	// A SettingsManager updates and retrieves the host's settings.
	SettingsManager interface {
		Settings() settings.Settings
		UpdateSettings(settings.Settings) error
	}

	// A Store stores and retrieves pinned settings.
	Store interface {
		PinnedSettings() (PinnedSettings, error)
		UpdatePinnedSettings(PinnedSettings) error
	}

	// A Manager manages the host's pinned settings and updates the host's
	// settings based on the current exchange rate.
	Manager struct {
		log      *zap.Logger
		store    Store
		explorer *explorer.Explorer
		sm       SettingsManager

		frequency time.Duration

		mu       sync.Mutex
		rates    []decimal.Decimal
		lastRate decimal.Decimal
		pinned   PinnedSettings // in-memory cache of pinned settings
	}
)

func isOverThreshold(a, b, percentage decimal.Decimal) bool {
	threshold := a.Mul(percentage)
	diff := a.Sub(b).Abs()
	return diff.GreaterThan(threshold)
}

func convertToCurrency(target decimal.Decimal, rate decimal.Decimal) types.Currency {
	hastings := target.Div(rate).Mul(decimal.New(1, 24)).Round(0).String()
	c, err := types.ParseCurrency(hastings)
	if err != nil {
		panic(err)
	}
	return c
}

func (m *Manager) updatePrices(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	m.mu.Lock()
	currency := m.pinned.Currency
	m.mu.Unlock()

	current, err := m.explorer.SiacoinExchangeRate(ctx, currency)
	if err != nil {
		return fmt.Errorf("failed to get exchange rate: %w", err)
	} else if current.IsZero() {
		return fmt.Errorf("exchange rate is zero")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	maxRates := int(12 * time.Hour / m.frequency)
	m.rates = append(m.rates, current)
	if len(m.rates) >= maxRates {
		m.rates = m.rates[1:]
	}

	// skip updating prices if the pinned settings are zero
	if m.pinned.Storage.IsZero() && m.pinned.Ingress.IsZero() && m.pinned.Egress.IsZero() && m.pinned.MaxCollateral.IsZero() {
		return nil
	}

	var sum decimal.Decimal
	for _, r := range m.rates {
		sum = sum.Add(r)
	}
	avgRate := sum.Div(decimal.New(int64(len(m.rates)), 0))

	if !isOverThreshold(m.lastRate, avgRate, m.pinned.Threshold) {
		m.log.Debug("new rate not over threshold", zap.Stringer("current", current), zap.Stringer("average", avgRate), zap.Stringer("last", m.lastRate))
		return nil
	}
	m.lastRate = avgRate

	settings := m.sm.Settings()
	if !m.pinned.Storage.IsZero() {
		settings.StoragePrice = convertToCurrency(m.pinned.Storage, avgRate).Div64(4320).Div64(1e12)
	}

	if !m.pinned.Ingress.IsZero() {
		settings.IngressPrice = convertToCurrency(m.pinned.Ingress, avgRate).Div64(1e12)
	}

	if !m.pinned.Egress.IsZero() {
		settings.EgressPrice = convertToCurrency(m.pinned.Egress, avgRate).Div64(1e12)
	}

	if !m.pinned.MaxCollateral.IsZero() {
		settings.MaxCollateral = convertToCurrency(m.pinned.MaxCollateral, avgRate).Div64(4320).Div64(1e12)
	}

	if err := m.sm.UpdateSettings(settings); err != nil {
		return fmt.Errorf("failed to update settings: %w", err)
	}
	m.log.Info("updated prices", zap.Stringer("storage", settings.StoragePrice), zap.Stringer("ingress", settings.IngressPrice), zap.Stringer("egress", settings.EgressPrice))
	return nil
}

// Pinned returns the host's pinned settings.
func (m *Manager) Pinned() PinnedSettings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pinned
}

// Update updates the host's pinned settings.
func (m *Manager) Update(p PinnedSettings) error {
	m.mu.Lock()
	if m.pinned.Currency != p.Currency {
		m.rates = m.rates[:0] // currency has changed, reset rates
	}
	m.pinned = p
	m.mu.Unlock()
	if err := m.store.UpdatePinnedSettings(p); err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	} else if err := m.updatePrices(context.Background()); err != nil {
		return fmt.Errorf("failed to update prices: %w", err)
	}
	return nil
}

// Run starts the PinManager's update loop.
func (m *Manager) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Minute)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := m.updatePrices(ctx); err != nil {
				m.log.Error("failed to update prices", zap.Error(err))
			}
		}
	}
}

// NewManager creates a new pin manager.
func NewManager(frequency time.Duration, store Store, explorer *explorer.Explorer, sm SettingsManager, log *zap.Logger) (*Manager, error) {
	pinned, err := store.PinnedSettings()
	if err != nil {
		return nil, fmt.Errorf("failed to get pinned settings: %w", err)
	}

	return &Manager{
		log:      log,
		store:    store,
		explorer: explorer,
		sm:       sm,

		frequency: frequency,
		pinned:    pinned,
	}, nil
}
