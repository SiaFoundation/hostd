package registry_test

import (
	"path/filepath"
	"reflect"
	"testing"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func randomValue(key types.PrivateKey) (value rhpv3.RegistryEntry) {
	value.Tweak = frand.Entropy256()
	value.Data = frand.Bytes(32)
	value.Type = rhpv3.EntryTypeArbitrary
	value.PublicKey = key.PublicKey()
	value.Signature = key.SignHash(value.Hash())
	return
}

func testLog(tb testing.TB) *zap.Logger {
	opt := zap.NewDevelopmentConfig()
	opt.OutputPaths = []string{filepath.Join(tb.TempDir(), "hostd.log")}
	log, err := opt.Build()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { log.Sync() })
	return log
}

func testRegistry(t *testing.T, privKey types.PrivateKey, limit uint64) *registry.Manager {
	log := testLog(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostdb.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	if err := db.UpdateSettings(settings.Settings{MaxRegistryEntries: limit}); err != nil {
		t.Fatal(err)
	}
	return registry.NewManager(privKey, db)
}

func TestRegistryPut(t *testing.T) {
	const registryCap = 10
	hostPriv := types.GeneratePrivateKey()
	renterPriv := types.GeneratePrivateKey()
	reg := testRegistry(t, hostPriv, registryCap)

	// store a random value in the registry
	original := randomValue(renterPriv)
	updated, err := reg.Put(original, registryCap)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(original.RegistryValue, updated) {
		t.Fatal("expected returned value to match")
	}

	// retrieve the value
	value, err := reg.Get(original.RegistryKey)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(original.RegistryValue, value) {
		t.Fatal("expected returned value to match")
	}

	// test storing the same value again; should fail and return the original
	// value
	updated, err = reg.Put(original, 10)
	if err == nil {
		t.Fatalf("expected validation error")
	} else if !reflect.DeepEqual(original.RegistryValue, updated) {
		t.Fatal("expected returned value to match")
	}

	// test updating the value's revision number and data; should succeed
	entry := rhpv3.RegistryEntry{
		RegistryKey: rhpv3.RegistryKey{
			PublicKey: renterPriv.PublicKey(),
			Tweak:     original.Tweak,
		},
		RegistryValue: rhpv3.RegistryValue{
			Data:     original.Data,
			Revision: 1,
			Type:     rhpv3.EntryTypeArbitrary,
		},
	}
	entry.Signature = renterPriv.SignHash(entry.Hash())
	updated, err = reg.Put(entry, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(entry.RegistryValue, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test updating the value's work; should succeed
	updatedEntry := rhpv3.RegistryEntry{
		RegistryKey:   entry.RegistryKey,
		RegistryValue: updated,
	}
	entry = rhpv3.RegistryEntry{
		RegistryKey: original.RegistryKey,
		RegistryValue: rhpv3.RegistryValue{
			Data:     make([]byte, 32),
			Revision: 1,
			Type:     rhpv3.EntryTypeArbitrary,
		},
	}
	for rhpv3.CompareRegistryWork(entry, updatedEntry) <= 0 {
		frand.Read(entry.Data)
	}
	entry.Signature = renterPriv.SignHash(entry.Hash())
	updated, err = reg.Put(entry, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(entry.RegistryValue, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test setting the value to a primary value; should succeed
	hostID := rhpv3.RegistryHostID(hostPriv.PublicKey())
	entry = rhpv3.RegistryEntry{
		RegistryKey: original.RegistryKey,
		RegistryValue: rhpv3.RegistryValue{
			Data:     append([]byte(hostID[:20]), updated.Data...),
			Revision: 1,
			Type:     rhpv3.EntryTypePubKey,
		},
	}
	entry.Signature = renterPriv.SignHash(entry.Hash())
	updated, err = reg.Put(entry, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(entry.RegistryValue, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// fill the registry
	for i := 0; i < registryCap-1; i++ {
		_, err := reg.Put(randomValue(renterPriv), 10)
		if err != nil {
			t.Fatalf("failed on entry %d: %s", i, err)
		}
	}

	// test storing a value that would exceed the registry capacity; should fail
	_, err = reg.Put(randomValue(renterPriv), 10)
	if err == nil {
		t.Fatalf("expected cap error")
	}
}
