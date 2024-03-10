package pin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.uber.org/zap"
)

type (
	// PinnedSettings contains the settings that can be optionally
	// pinned to an external currency. This uses an external explorer
	// to retrieve the current exchange rate.
	PinnedSettings struct {
		// Currency is the external three letter currency code. If empty,
		// pinning is disabled. If the explorer does not support the
		// currency an error is returned.
		Currency string `json:"currency"`

		// Threshold is a percentage from 0 to 1 that determines when the
		// host's settings are updated based on the current exchange rate.
		Threshold float64 `json:"threshold"`

		// Storage, Ingress, and Egress are the pinned prices in the
		// external currency.
		Storage float64 `json:"storage"`
		Ingress float64 `json:"ingress"`
		Egress  float64 `json:"egress"`

		// MaxCollateral is the maximum collateral that the host will
		// accept in the external currency.
		MaxCollateral float64 `json:"maxCollateral"`
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

	// An ExchangeRateRetriever retrieves the current exchange rate from
	// an external source.
	ExchangeRateRetriever interface {
		SiacoinExchangeRate(ctx context.Context, currency string) (float64, error)
	}

	// A Manager manages the host's pinned settings and updates the host's
	// settings based on the current exchange rate.
	Manager struct {
		log      *zap.Logger
		store    Store
		explorer ExchangeRateRetriever
		sm       SettingsManager

		frequency  time.Duration
		rateWindow time.Duration

		mu       sync.Mutex
		rates    []decimal.Decimal
		lastRate decimal.Decimal
		pinned   PinnedSettings // in-memory cache of pinned settings
	}
)

func isOverThreshold(a, b, percentage decimal.Decimal) bool {
	threshold := a.Mul(percentage)
	diff := a.Sub(b).Abs()
	return diff.GreaterThanOrEqual(threshold)
}

func convertToCurrency(target decimal.Decimal, rate decimal.Decimal) types.Currency {
	hastings := target.Div(rate).Mul(decimal.New(1, 24)).Round(0).String()
	c, err := types.ParseCurrency(hastings)
	if err != nil {
		panic(err)
	}
	return c
}

func averageRate(rates []decimal.Decimal) decimal.Decimal {
	var sum decimal.Decimal
	for _, r := range rates {
		sum = sum.Add(r)
	}
	return sum.Div(decimal.NewFromInt(int64(len(rates))))
}

func (m *Manager) updatePrices(ctx context.Context, force bool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	m.mu.Lock()
	currency := m.pinned.Currency
	m.mu.Unlock()

	if currency == "" {
		return nil
	}

	rate, err := m.explorer.SiacoinExchangeRate(ctx, currency)
	if err != nil {
		return fmt.Errorf("failed to get exchange rate: %w", err)
	} else if rate <= 0 {
		return fmt.Errorf("exchange rate must be positive")
	}
	current := decimal.NewFromFloat(rate)

	m.mu.Lock()
	defer m.mu.Unlock()

	maxRates := int(m.rateWindow / m.frequency)
	m.rates = append(m.rates, current)
	if len(m.rates) >= maxRates {
		m.rates = m.rates[1:]
	}

	// skip updating prices if the pinned settings are zero
	if m.pinned.Storage <= 0 && m.pinned.Ingress <= 0 && m.pinned.Egress <= 0 && m.pinned.MaxCollateral <= 0 {
		return nil
	}

	avgRate := averageRate(m.rates)
	threshold := decimal.NewFromFloat(m.pinned.Threshold)

	log := m.log.With(zap.String("currency", currency), zap.Stringer("threshold", threshold), zap.Stringer("current", current), zap.Stringer("average", avgRate), zap.Stringer("last", m.lastRate))
	if !force && !isOverThreshold(m.lastRate, avgRate, threshold) {
		log.Debug("new rate not over threshold")
		return nil
	}
	m.lastRate = avgRate

	settings := m.sm.Settings()
	if m.pinned.Storage > 0 {
		settings.StoragePrice = convertToCurrency(decimal.NewFromFloat(m.pinned.Storage), avgRate).Div64(4320).Div64(1e12)
	}

	if m.pinned.Ingress > 0 {
		settings.IngressPrice = convertToCurrency(decimal.NewFromFloat(m.pinned.Ingress), avgRate).Div64(1e12)
	}

	if m.pinned.Egress > 0 {
		settings.EgressPrice = convertToCurrency(decimal.NewFromFloat(m.pinned.Egress), avgRate).Div64(1e12)
	}

	if m.pinned.MaxCollateral > 0 {
		settings.MaxCollateral = convertToCurrency(decimal.NewFromFloat(m.pinned.MaxCollateral), avgRate)
	}

	if err := m.sm.UpdateSettings(settings); err != nil {
		return fmt.Errorf("failed to update settings: %w", err)
	}
	log.Info("updated prices", zap.Stringer("storage", settings.StoragePrice), zap.Stringer("ingress", settings.IngressPrice), zap.Stringer("egress", settings.EgressPrice))
	return nil
}

// Pinned returns the host's pinned settings.
func (m *Manager) Pinned() PinnedSettings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pinned
}

// Update updates the host's pinned settings.
func (m *Manager) Update(ctx context.Context, p PinnedSettings) error {
	switch {
	case p.Currency == "":
		return fmt.Errorf("currency must be set")
	case p.Threshold < 0 || p.Threshold > 1:
		return fmt.Errorf("threshold must be between 0 and 1")
	case p.Storage < 0:
		return fmt.Errorf("storage price must be non-negative")
	case p.Ingress < 0:
		return fmt.Errorf("ingress price must be non-negative")
	case p.Egress < 0:
		return fmt.Errorf("egress price must be non-negative")
	case p.MaxCollateral < 0:
		return fmt.Errorf("max collateral must be non-negative")
	}

	m.mu.Lock()
	if m.pinned.Currency != p.Currency {
		m.rates = m.rates[:0] // currency has changed, reset rates
	}
	m.pinned = p
	m.mu.Unlock()
	if err := m.store.UpdatePinnedSettings(p); err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	} else if err := m.updatePrices(ctx, true); err != nil {
		return fmt.Errorf("failed to update prices: %w", err)
	}
	return nil
}

// Run starts the PinManager's update loop.
func (m *Manager) Run(ctx context.Context) error {
	t := time.NewTicker(m.frequency)

	// update prices immediately
	if err := m.updatePrices(ctx, true); err != nil {
		m.log.Error("failed to update prices", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := m.updatePrices(ctx, false); err != nil {
				m.log.Error("failed to update prices", zap.Error(err))
			}
		}
	}
}

// NewManager creates a new pin manager.
func NewManager(opts ...Option) (*Manager, error) {
	m := &Manager{
		log: zap.NewNop(),

		frequency:  5 * time.Minute,
		rateWindow: 6 * time.Hour,
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.store == nil {
		return nil, fmt.Errorf("store is required")
	} else if m.explorer == nil {
		return nil, fmt.Errorf("exchange rate retriever is required")
	} else if m.sm == nil {
		return nil, fmt.Errorf("settings manager is required")
	} else if m.log == nil {
		return nil, fmt.Errorf("logger is required")
	} else if m.frequency <= 0 {
		return nil, fmt.Errorf("frequency must be positive")
	} else if m.rateWindow <= 0 {
		return nil, fmt.Errorf("rate window must be positive")
	} else if m.rateWindow < m.frequency {
		return nil, fmt.Errorf("rate window must be greater than or equal to frequency")
	}

	// load the current pinned settings
	pinned, err := m.store.PinnedSettings()
	if err != nil {
		return nil, fmt.Errorf("failed to get pinned settings: %w", err)
	}
	m.pinned = pinned
	return m, nil
}
