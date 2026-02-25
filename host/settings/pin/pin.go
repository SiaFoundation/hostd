package pin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var pinAlertID = frand.Entropy256()

type (
	// A Pin is a pinned price in an external currency.
	Pin struct {
		Pinned bool    `json:"pinned"`
		Value  float64 `json:"value"`
	}

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
		Storage Pin `json:"storage"`
		Ingress Pin `json:"ingress"`
		Egress  Pin `json:"egress"`

		// MaxCollateral is the maximum collateral that the host will
		// accept in the external currency.
		MaxCollateral Pin `json:"maxCollateral"`
	}

	// Alerts registers global alerts.
	Alerts interface {
		Register(alerts.Alert)
		Dismiss(...types.Hash256)
	}

	// A SettingsManager updates and retrieves the host's settings.
	SettingsManager interface {
		Settings() settings.Settings
		UpdateSettings(settings.Settings) error
	}

	// A Store stores and retrieves pinned settings.
	Store interface {
		PinnedSettings(context.Context) (PinnedSettings, error)
		UpdatePinnedSettings(context.Context, PinnedSettings) error
	}

	// A Forex retrieves the current exchange rate from
	// an external source.
	Forex interface {
		SiacoinExchangeRate(ctx context.Context, currency string) (float64, error)
	}

	// A Manager manages the host's pinned settings and updates the host's
	// settings based on the current exchange rate.
	Manager struct {
		log    *zap.Logger
		store  Store
		alerts Alerts
		forex  Forex
		sm     SettingsManager
		tg     *threadgroup.ThreadGroup

		frequency  time.Duration
		rateWindow time.Duration

		mu       sync.Mutex
		rates    []decimal.Decimal
		lastRate decimal.Decimal
		settings PinnedSettings // in-memory cache of pinned settings
	}
)

// IsPinned returns true if the pin is enabled and the value is greater than 0.
func (p Pin) IsPinned() bool {
	return p.Pinned && p.Value > 0
}

func isOverThreshold(a, b, percentage decimal.Decimal) bool {
	threshold := a.Mul(percentage)
	diff := a.Sub(b).Abs()
	return diff.GreaterThanOrEqual(threshold)
}

func averageRate(rates []decimal.Decimal) decimal.Decimal {
	var sum decimal.Decimal
	for _, r := range rates {
		sum = sum.Add(r)
	}
	return sum.Div(decimal.NewFromInt(int64(len(rates))))
}

func (m *Manager) registerPinFailureAlert(err error) {
	if m.alerts != nil && err != nil {
		m.alerts.Register(alerts.Alert{
			ID:        pinAlertID,
			Severity:  alerts.SeverityError,
			Message:   "failed to update prices",
			Timestamp: time.Now(),
			Data: map[string]any{
				"error": err.Error(),
			},
		})
	}
}

func (m *Manager) dismissPinFailureAlert() {
	if m.alerts != nil {
		m.alerts.Dismiss(pinAlertID)
	}
}

func (m *Manager) updatePrices(ctx context.Context, force bool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	m.mu.Lock()
	currency := m.settings.Currency
	m.mu.Unlock()

	if currency == "" {
		return nil
	}

	rate, err := m.forex.SiacoinExchangeRate(ctx, currency)
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
	if !m.settings.Storage.IsPinned() && !m.settings.Ingress.IsPinned() && !m.settings.Egress.IsPinned() && !m.settings.MaxCollateral.IsPinned() {
		return nil
	}

	avgRate := averageRate(m.rates)
	threshold := decimal.NewFromFloat(m.settings.Threshold)

	log := m.log.With(zap.String("currency", currency), zap.Stringer("threshold", threshold), zap.Stringer("current", current), zap.Stringer("average", avgRate), zap.Stringer("last", m.lastRate))
	if !force && !isOverThreshold(m.lastRate, avgRate, threshold) {
		log.Debug("new rate not over threshold")
		return nil
	}
	m.lastRate = avgRate

	settings := m.sm.Settings()
	if m.settings.Storage.IsPinned() {
		value, err := ConvertCurrencyToSC(decimal.NewFromFloat(m.settings.Storage.Value), avgRate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %w", err)
		}
		settings.StoragePrice = value.Div64(4320).Div64(1e12)
	}

	if m.settings.Ingress.IsPinned() {
		value, err := ConvertCurrencyToSC(decimal.NewFromFloat(m.settings.Ingress.Value), avgRate)
		if err != nil {
			return fmt.Errorf("failed to convert ingress price: %w", err)
		}
		settings.IngressPrice = value.Div64(1e12)
	}

	if m.settings.Egress.IsPinned() {
		value, err := ConvertCurrencyToSC(decimal.NewFromFloat(m.settings.Egress.Value), avgRate)
		if err != nil {
			return fmt.Errorf("failed to convert egress price: %w", err)
		}
		settings.EgressPrice = value.Div64(1e12)
	}

	if m.settings.MaxCollateral.IsPinned() {
		value, err := ConvertCurrencyToSC(decimal.NewFromFloat(m.settings.MaxCollateral.Value), avgRate)
		if err != nil {
			return fmt.Errorf("failed to convert max collateral: %w", err)
		}
		settings.MaxCollateral = value
	}

	if err := m.sm.UpdateSettings(settings); err != nil {
		return fmt.Errorf("failed to update settings: %w", err)
	}
	log.Info("updated prices", zap.Stringer("storage", settings.StoragePrice), zap.Stringer("ingress", settings.IngressPrice), zap.Stringer("egress", settings.EgressPrice))
	return nil
}

// Pinned returns the host's pinned settings.
func (m *Manager) Pinned(context.Context) PinnedSettings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settings
}

// Update updates the host's pinned settings.
func (m *Manager) Update(ctx context.Context, p PinnedSettings) error {
	switch {
	case p.Currency == "":
		return fmt.Errorf("currency must be set")
	case p.Threshold < 0 || p.Threshold > 1:
		return fmt.Errorf("threshold must be between 0 and 1")
	case p.Storage.Pinned && p.Storage.Value <= 0:
		return fmt.Errorf("storage price must be greater than 0")
	case p.Ingress.Pinned && p.Ingress.Value <= 0:
		return fmt.Errorf("ingress price must be greater than 0")
	case p.Egress.Pinned && p.Egress.Value <= 0:
		return fmt.Errorf("egress price must be greater than 0")
	case p.MaxCollateral.Pinned && p.MaxCollateral.Value <= 0:
		return fmt.Errorf("max collateral must be greater than 0")
	}

	m.mu.Lock()
	if m.settings.Currency != p.Currency {
		m.rates = m.rates[:0] // currency has changed, reset rates
	}
	m.settings = p
	m.mu.Unlock()
	if err := m.store.UpdatePinnedSettings(ctx, p); err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	} else if err := m.updatePrices(ctx, true); err != nil {
		return fmt.Errorf("failed to update prices: %w", err)
	}
	return nil
}

// Close closes the PinManager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

// run starts the PinManager's update loop.
func (m *Manager) run() error {
	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	defer cancel()

	t := time.NewTicker(m.frequency)

	// update prices immediately
	if err := m.updatePrices(ctx, true); err != nil {
		m.registerPinFailureAlert(err)
		m.log.Error("failed to update prices", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := m.updatePrices(ctx, false); err != nil {
				m.log.Error("failed to update prices", zap.Error(err))
				m.registerPinFailureAlert(err)
			} else {
				m.dismissPinFailureAlert()
			}
		}
	}
}

// ConvertCurrencyToSC converts a value in an external currency and an exchange
// rate to Siacoins.
func ConvertCurrencyToSC(target decimal.Decimal, rate decimal.Decimal) (types.Currency, error) {
	if rate.IsZero() {
		return types.Currency{}, nil
	}

	i := target.Div(rate).Mul(decimal.New(1, 24)).BigInt()
	if i.Sign() < 0 {
		return types.Currency{}, errors.New("negative currency")
	} else if i.BitLen() > 128 {
		return types.Currency{}, errors.New("currency overflow")
	}
	return types.NewCurrency(i.Uint64(), i.Rsh(i, 64).Uint64()), nil
}

// NewManager creates a new pin manager.
func NewManager(store Store, settings SettingsManager, f Forex, opts ...Option) (*Manager, error) {
	m := &Manager{
		store: store,
		sm:    settings,
		forex: f,

		tg:     threadgroup.New(),
		alerts: alerts.NewNop(),
		log:    zap.NewNop(),

		frequency:  5 * time.Minute,
		rateWindow: 6 * time.Hour,
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.frequency <= 0 {
		return nil, fmt.Errorf("frequency must be positive")
	} else if m.rateWindow <= 0 {
		return nil, fmt.Errorf("rate window must be positive")
	} else if m.rateWindow < m.frequency {
		return nil, fmt.Errorf("rate window must be greater than or equal to frequency")
	}

	// load the current pinned settings
	pinned, err := m.store.PinnedSettings(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get pinned settings: %w", err)
	}
	m.settings = pinned

	go m.run() // run the update loop in the background
	return m, nil
}
