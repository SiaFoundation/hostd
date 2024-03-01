package settings

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/explorer"
	"go.uber.org/zap"
)

type (
	// A PinStore stores and retrieves pinned settings.
	PinStore interface {
		PinnedSettings() (PinnedSettings, error)
		UpdatePinnedSettings(PinnedSettings) error
	}

	// A PinManager manages the host's pinned settings and updates the host's
	// settings based on the current exchange rate.
	PinManager struct {
		log      *zap.Logger
		store    PinStore
		explorer *explorer.Explorer
		cm       *ConfigManager

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

func (pm *PinManager) update(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pm.mu.Lock()
	currency := pm.pinned.Currency
	pm.mu.Unlock()

	current, err := pm.explorer.SiacoinExchangeRate(ctx, currency)
	if err != nil {
		return fmt.Errorf("failed to get exchange rate: %w", err)
	} else if current.IsZero() {
		return fmt.Errorf("exchange rate is zero")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	maxRates := int(12 * time.Hour / pm.frequency)
	pm.rates = append(pm.rates, current)
	if len(pm.rates) >= maxRates {
		pm.rates = pm.rates[1:]
	}

	// skip updating prices if the pinned settings are zero
	if pm.pinned.Storage.IsZero() && pm.pinned.Ingress.IsZero() && pm.pinned.Egress.IsZero() && pm.pinned.MaxCollateral.IsZero() {
		return nil
	}

	var sum decimal.Decimal
	for _, r := range pm.rates {
		sum = sum.Add(r)
	}
	avgRate := sum.Div(decimal.New(int64(len(pm.rates)), 0))

	if !isOverThreshold(pm.lastRate, avgRate, pm.pinned.Threshold) {
		pm.log.Debug("new rate not over threshold", zap.Stringer("current", current), zap.Stringer("average", avgRate), zap.Stringer("last", pm.lastRate))
		return nil
	}
	pm.lastRate = avgRate

	settings := pm.cm.Settings()
	if !pm.pinned.Storage.IsZero() {
		settings.StoragePrice = convertToCurrency(pm.pinned.Storage, avgRate).Div64(4320).Div64(1e12)
	}

	if !pm.pinned.Ingress.IsZero() {
		settings.IngressPrice = convertToCurrency(pm.pinned.Ingress, avgRate).Div64(1e12)
	}

	if !pm.pinned.Egress.IsZero() {
		settings.EgressPrice = convertToCurrency(pm.pinned.Egress, avgRate).Div64(1e12)
	}

	if !pm.pinned.MaxCollateral.IsZero() {
		settings.MaxCollateral = convertToCurrency(pm.pinned.MaxCollateral, avgRate).Div64(4320).Div64(1e12)
	}

	if err := pm.cm.UpdateSettings(settings); err != nil {
		return fmt.Errorf("failed to update settings: %w", err)
	}
	pm.log.Debug("updated prices", zap.Stringer("storage", settings.StoragePrice), zap.Stringer("ingress", settings.IngressPrice), zap.Stringer("egress", settings.EgressPrice))
	return nil
}

// Pinned returns the host's pinned settings.
func (pm *PinManager) Pinned() PinnedSettings {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.pinned
}

// Update updates the host's pinned settings.
func (pm *PinManager) Update(p PinnedSettings) error {
	pm.mu.Lock()
	if pm.pinned.Currency != p.Currency {
		pm.rates = pm.rates[:0] // currency has changed, reset rates
	}
	pm.pinned = p
	pm.mu.Unlock()
	if err := pm.store.UpdatePinnedSettings(p); err != nil {
		return fmt.Errorf("failed to update pinned settings: %w", err)
	} else if err := pm.update(context.Background()); err != nil {
		return fmt.Errorf("failed to update prices: %w", err)
	}
	return nil
}

// Run starts the PinManager's update loop.
func (pm *PinManager) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Minute)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := pm.update(ctx); err != nil {
				pm.log.Error("failed to update prices", zap.Error(err))
			}
		}
	}
}

// NewPinManager creates a new PinManager.
func NewPinManager(frequency time.Duration, store PinStore, explorer *explorer.Explorer, cm *ConfigManager, log *zap.Logger) (*PinManager, error) {
	pinned, err := store.PinnedSettings()
	if err != nil {
		return nil, fmt.Errorf("failed to get pinned settings: %w", err)
	}

	return &PinManager{
		log:      log,
		store:    store,
		explorer: explorer,
		cm:       cm,

		frequency: frequency,
		pinned:    pinned,
	}, nil
}
