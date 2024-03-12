package pin_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
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
	if pinned.Storage > 0 {
		storagePrice, err := pin.CurrencyToSiacoins(decimal.NewFromFloat(pinned.Storage), rate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %v", err)
		} else if !storagePrice.Div64(4320).Div64(1e12).Equals(settings.StoragePrice) {
			return fmt.Errorf("expected storage price %d, got %d", storagePrice, settings.StoragePrice)
		}
	}

	if pinned.Ingress > 0 {
		ingressPrice, err := pin.CurrencyToSiacoins(decimal.NewFromFloat(pinned.Ingress), rate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %v", err)
		} else if !ingressPrice.Div64(1e12).Equals(settings.IngressPrice) {
			return fmt.Errorf("expected ingress price %d, got %d", ingressPrice, settings.IngressPrice)
		}
	}

	if pinned.Egress > 0 {
		egressPrice, err := pin.CurrencyToSiacoins(decimal.NewFromFloat(pinned.Egress), rate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %v", err)
		} else if !egressPrice.Div64(1e12).Equals(settings.EgressPrice) {
			return fmt.Errorf("expected egress price %d, got %d", egressPrice, settings.EgressPrice)
		}
	}

	if pinned.MaxCollateral > 0 {
		maxCollateral, err := pin.CurrencyToSiacoins(decimal.NewFromFloat(pinned.MaxCollateral), rate)
		if err != nil {
			return fmt.Errorf("failed to convert storage price: %v", err)
		} else if !maxCollateral.Equals(settings.MaxCollateral) {
			return fmt.Errorf("expected max collateral %d, got %d", maxCollateral, settings.MaxCollateral)
		}
	}
	return nil
}

func TestConvertCurrencyToSiacoins(t *testing.T) {
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
		if result, err := pin.CurrencyToSiacoins(test.target, test.rate); test.err != nil {
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
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rr := &exchangeRateRetrieverStub{
		value:    1,
		currency: "usd",
	}

	node, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	sm, err := settings.NewConfigManager(settings.WithHostKey(types.GeneratePrivateKey()), settings.WithStore(db), settings.WithChainManager(node.ChainManager()))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	pm, err := pin.NewManager(pin.WithAverageRateWindow(time.Minute),
		pin.WithFrequency(100*time.Millisecond),
		pin.WithExchangeRateRetriever(rr),
		pin.WithSettings(sm),
		pin.WithStore(db),
		pin.WithLogger(log.Named("pin")))
	if err != nil {
		t.Fatal(err)
	}

	initialSettings := sm.Settings()
	pin := pin.PinnedSettings{
		Currency: "usd",

		Threshold:     0.1,
		Storage:       1.0,
		Ingress:       0,
		Egress:        0,
		MaxCollateral: 0,
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
	pin.Ingress = 1.0
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
	pin.Egress = 1.0
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
	pin.MaxCollateral = 1.0
	if err := pm.Update(context.Background(), pin); err != nil {
		t.Fatal(err)
	} else if err := checkSettings(sm.Settings(), pin, 1); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticUpdate(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rr := &exchangeRateRetrieverStub{
		value:    1,
		currency: "usd",
	}

	node, err := test.NewNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	sm, err := settings.NewConfigManager(settings.WithHostKey(types.GeneratePrivateKey()), settings.WithStore(db), settings.WithChainManager(node.ChainManager()))
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	pm, err := pin.NewManager(pin.WithAverageRateWindow(time.Second/2),
		pin.WithFrequency(100*time.Millisecond),
		pin.WithExchangeRateRetriever(rr),
		pin.WithSettings(sm),
		pin.WithStore(db),
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

		Threshold:     1.0,
		Storage:       1.0,
		Ingress:       1.0,
		Egress:        1.0,
		MaxCollateral: 1.0,
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
	rr.updateRate(1.5)
	time.Sleep(time.Second)
	if err := checkSettings(sm.Settings(), pin, 1); err != nil {
		t.Fatal(err)
	}

	// update the exchange rate to put it over the threshold
	rr.updateRate(2)
	time.Sleep(time.Second)
	if err := checkSettings(sm.Settings(), pin, 2); err != nil {
		t.Fatal(err)
	}
}
