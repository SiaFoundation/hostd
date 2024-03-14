package sqlite

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func randomSettings() settings.Settings {
	return settings.Settings{
		AcceptingContracts:   frand.Intn(1) == 1,
		NetAddress:           hex.EncodeToString(frand.Bytes(64)),
		MaxContractDuration:  uint64(frand.Intn(math.MaxInt)),
		ContractPrice:        types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		BaseRPCPrice:         types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		SectorAccessPrice:    types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		CollateralMultiplier: frand.Float64(),
		MaxCollateral:        types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		StoragePrice:         types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		EgressPrice:          types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		IngressPrice:         types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		IngressLimit:         uint64(frand.Intn(math.MaxInt)),
		EgressLimit:          uint64(frand.Intn(math.MaxInt)),
		MaxRegistryEntries:   uint64(frand.Intn(math.MaxInt)),
		AccountExpiry:        time.Duration(frand.Intn(math.MaxInt)),
		PriceTableValidity:   time.Duration(frand.Intn(math.MaxInt)),
		MaxAccountBalance:    types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
	}
}

func randomPinnedSettings() pin.PinnedSettings {
	return pin.PinnedSettings{
		Currency:      hex.EncodeToString(frand.Bytes(3)),
		Threshold:     frand.Float64(),
		Storage:       pin.Pin{Pinned: frand.Intn(1) == 1, Value: frand.Float64()},
		Ingress:       pin.Pin{Pinned: frand.Intn(1) == 1, Value: frand.Float64()},
		Egress:        pin.Pin{Pinned: frand.Intn(1) == 1, Value: frand.Float64()},
		MaxCollateral: pin.Pin{Pinned: frand.Intn(1) == 1, Value: frand.Float64()},
	}
}

func TestPinned(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "hostdb.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 10000; i++ {
		p := randomPinnedSettings()
		if err := db.UpdatePinnedSettings(context.Background(), p); err != nil {
			t.Fatal(err)
		} else if p2, err := db.PinnedSettings(context.Background()); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(p, p2) {
			t.Fatalf("expected %v, got %v", p, p2)
		}
	}
}

func TestSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "hostdb.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// test no settings

	if _, err = db.Settings(); !errors.Is(err, settings.ErrNoSettings) {
		t.Fatalf("expected ErrNoSettings, got %v", err)
	}

	// set initial settings
	initial := randomSettings()
	if err := db.UpdateSettings(initial); err != nil {
		t.Fatal(err)
	}

	// retrieve settings
	current, err := db.Settings()
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(initial, current) {
		t.Fatalf("expected %v, got %v", initial, current)
	}

	// change the settings
	updated := randomSettings()
	if err := db.UpdateSettings(updated); err != nil {
		t.Fatal(err)
	}

	// retrieve settings
	updated.Revision = 1 // increment the revision number for DeepEqual
	current, err = db.Settings()
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(updated, current) {
		t.Fatalf("expected %v, got %v", updated, current)
	}
}
