package pin_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/internal/testutil"
	"go.uber.org/zap/zaptest"
)

type exchangeRateRetrieverStub struct {
	mu       sync.Mutex
	value    float64
	currency string
}

func (e *exchangeRateRetrieverStub) updateRate(value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = value
}

func (e *exchangeRateRetrieverStub) SiacoinExchangeRate(_ context.Context, currency string) (float64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !strings.EqualFold(currency, e.currency) {
		return 0, errors.New("currency not found")
	}
	return e.value, nil
}

func checkSettings(settings settings.Settings, pinned pin.PinnedSettings, expectedRate float64) error {
	rate := decimal.NewFromFloat(expectedRate)
	if pinned.Storage.IsPinned() {
		storagePrice, err := pin.ConvertCurrencyToSC(decimal.NewFromFloat(pinned.Storage.Value), rate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %w", err)
		} else if !storagePrice.Div64(4320).Div64(1e12).Equals(settings.StoragePrice) {
			return fmt.Errorf("expected storage price %d, got %d", storagePrice, settings.StoragePrice)
		}
	}

	if pinned.Ingress.IsPinned() {
		ingressPrice, err := pin.ConvertCurrencyToSC(decimal.NewFromFloat(pinned.Ingress.Value), rate)
		if err != nil {
			return fmt.Errorf("failed to convert ingress price: %w", err)
		} else if !ingressPrice.Div64(1e12).Equals(settings.IngressPrice) {
			return fmt.Errorf("expected ingress price %d, got %d", ingressPrice, settings.IngressPrice)
		}
	}

	if pinned.Egress.IsPinned() {
		egressPrice, err := pin.ConvertCurrencyToSC(decimal.NewFromFloat(pinned.Egress.Value), rate)
		if err != nil {
			return fmt.Errorf("failed to convert egress price: %w", err)
		} else if !egressPrice.Div64(1e12).Equals(settings.EgressPrice) {
			return fmt.Errorf("expected egress price %d, got %d", egressPrice, settings.EgressPrice)
		}
	}

	if pinned.MaxCollateral.IsPinned() {
		maxCollateral, err := pin.ConvertCurrencyToSC(decimal.NewFromFloat(pinned.MaxCollateral.Value), rate)
		if err != nil {
			return fmt.Errorf("failed to convert max collateral: %w", err)
		} else if !maxCollateral.Equals(settings.MaxCollateral) {
			return fmt.Errorf("expected max collateral %d, got %d", maxCollateral, settings.MaxCollateral)
		}
	}
	return nil
}

func TestConvertConvertCurrencyToSC(t *testing.T) {
	tests := []struct {
		target   decimal.Decimal
		rate     decimal.Decimal
		expected types.Currency
		err      error
	}{
		{decimal.NewFromFloat(1), decimal.NewFromFloat(1), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(2), types.Siacoins(1).Div64(2), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.5), types.Siacoins(2), nil},
		{decimal.NewFromFloat(0.5), decimal.NewFromFloat(0.5), types.Siacoins(1), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0.001), types.Siacoins(1000), nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(0), types.Currency{}, nil},
		{decimal.NewFromFloat(1), decimal.NewFromFloat(-1), types.Currency{}, errors.New("negative currency")},
		{decimal.NewFromFloat(-1), decimal.NewFromFloat(1), types.Currency{}, errors.New("negative currency")},
		{decimal.New(1, 50), decimal.NewFromFloat(0.1), types.Currency{}, errors.New("currency overflow")},
	}
	for i, test := range tests {
		if result, err := pin.ConvertCurrencyToSC(test.target, test.rate); test.err != nil {
			if err == nil {
				t.Fatalf("%d: expected error, got nil", i)
			} else if err.Error() != test.err.Error() {
				t.Fatalf("%d: expected %v, got %v", i, test.err, err)
			}
		} else if !test.expected.Equals(result) {
			t.Fatalf("%d: expected %d, got %d", i, test.expected, result)
		}
	}
}

func TestPinnedFields(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesis := testutil.V1Network()
	node := testutil.NewConsensusNode(t, network, genesis, log)

	fr := &exchangeRateRetrieverStub{
		value:    1,
		currency: "usd",
	}

	sm, err := settings.NewConfigManager(types.GeneratePrivateKey(), node.Store, node.Chain, node.Syncer, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	pm, err := pin.NewManager(node.Store, sm, fr, pin.WithAverageRateWindow(time.Minute),
		pin.WithFrequency(100*time.Millisecond),
		pin.WithLogger(log.Named("pin")))
	if err != nil {
		t.Fatal(err)
	}

	initialSettings := sm.Settings()
	pin := pin.PinnedSettings{
		Currency: "usd",

		Threshold: 0.1,
		Storage: pin.Pin{
			Pinned: true,
			Value:  1.0,
		},
		Ingress: pin.Pin{
			Pinned: false,
			Value:  1.0,
		},
		Egress: pin.Pin{
			Pinned: false,
			Value:  1.0,
		},
		MaxCollateral: pin.Pin{
			Pinned: false,
			Value:  1.0,
		},
	}

	// only storage is pinned
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	}

	currentSettings := sm.Settings()
	if err := checkSettings(currentSettings, pin, 1); err != nil {
		t.Fatal(err)
	} else if !currentSettings.MaxCollateral.Equals(initialSettings.MaxCollateral) {
		t.Fatalf("expected max collateral to be %d, got %d", initialSettings.MaxCollateral, currentSettings.MaxCollateral)
	} else if !currentSettings.IngressPrice.Equals(initialSettings.IngressPrice) {
		t.Fatalf("expected ingress price to be %d, got %d", initialSettings.IngressPrice, currentSettings.IngressPrice)
	} else if !currentSettings.EgressPrice.Equals(initialSettings.EgressPrice) {
		t.Fatalf("expected egress price to be %d, got %d", initialSettings.EgressPrice, currentSettings.EgressPrice)
	}

	// pin ingress
	pin.Ingress.Pinned = true
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	}

	currentSettings = sm.Settings()
	if err := checkSettings(currentSettings, pin, 1); err != nil {
		t.Fatal(err)
	} else if !currentSettings.MaxCollateral.Equals(initialSettings.MaxCollateral) {
		t.Fatalf("expected max collateral to be %d, got %d", initialSettings.MaxCollateral, currentSettings.MaxCollateral)
	} else if !currentSettings.EgressPrice.Equals(initialSettings.EgressPrice) {
		t.Fatalf("expected egress price to be %d, got %d", initialSettings.EgressPrice, currentSettings.EgressPrice)
	}

	// pin egress
	pin.Egress.Pinned = true
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	}

	currentSettings = sm.Settings()
	if err := checkSettings(currentSettings, pin, 1); err != nil {
		t.Fatal(err)
	} else if !currentSettings.MaxCollateral.Equals(initialSettings.MaxCollateral) {
		t.Fatalf("expected max collateral to be %d, got %d", initialSettings.MaxCollateral, currentSettings.MaxCollateral)
	}

	// pin max collateral
	pin.MaxCollateral.Pinned = true
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	} else if err := checkSettings(sm.Settings(), pin, 1); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticUpdate(t *testing.T) {
	log := zaptest.NewLogger(t)
	network, genesis := testutil.V1Network()
	node := testutil.NewConsensusNode(t, network, genesis, log)

	fr := &exchangeRateRetrieverStub{
		value:    1,
		currency: "usd",
	}

	sm, err := settings.NewConfigManager(types.GeneratePrivateKey(), node.Store, node.Chain, node.Syncer, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	pm, err := pin.NewManager(node.Store, sm, fr, pin.WithAverageRateWindow(time.Second/2),
		pin.WithFrequency(100*time.Millisecond),
		pin.WithLogger(log.Named("pin")))
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := pm.Run(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			panic(err)
		}
	}()

	time.Sleep(time.Second)

	pin := pin.PinnedSettings{
		Currency: "usd",

		Threshold: 1.0,
		Storage: pin.Pin{
			Pinned: true,
			Value:  1.0,
		},
		Ingress: pin.Pin{
			Pinned: true,
			Value:  1.0,
		},
		Egress: pin.Pin{
			Pinned: true,
			Value:  1.0,
		},
		MaxCollateral: pin.Pin{
			Pinned: true,
			Value:  1.0,
		},
	}

	// check that the settings have not changed
	if err := checkSettings(sm.Settings(), pin, 1); err == nil {
		t.Fatal("expected settings to not be updated")
	}

	// pin the settings
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	} else if err := checkSettings(sm.Settings(), pin, 1); err != nil {
		t.Fatal(err)
	}

	// update the exchange rate below the threshold
	fr.updateRate(1.5)
	time.Sleep(time.Second)
	if err := checkSettings(sm.Settings(), pin, 1); err != nil {
		t.Fatal(err)
	}

	// update the exchange rate to put it over the threshold
	fr.updateRate(2)
	time.Sleep(time.Second)
	if err := checkSettings(sm.Settings(), pin, 2); err != nil {
		t.Fatal(err)
	}
}
