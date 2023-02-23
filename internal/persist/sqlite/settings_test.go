package sqlite

import (
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func randomSettings() settings.Settings {
	return settings.Settings{
		AcceptingContracts:  frand.Intn(1) == 1,
		NetAddress:          hex.EncodeToString(frand.Bytes(64)),
		MaxContractDuration: uint64(frand.Intn(math.MaxInt)),
		ContractPrice:       types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		BaseRPCPrice:        types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		SectorAccessPrice:   types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		Collateral:          types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		MaxCollateral:       types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		MinStoragePrice:     types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		MinEgressPrice:      types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		MinIngressPrice:     types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
		IngressLimit:        uint64(frand.Intn(math.MaxInt)),
		EgressLimit:         uint64(frand.Intn(math.MaxInt)),
		MaxRegistryEntries:  uint64(frand.Intn(math.MaxInt)),
		AccountExpiry:       time.Duration(frand.Intn(math.MaxInt)),
		MaxAccountBalance:   types.NewCurrency(frand.Uint64n(math.MaxUint64), frand.Uint64n(math.MaxUint64)),
	}
}

func TestSettings(t *testing.T) {
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
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
