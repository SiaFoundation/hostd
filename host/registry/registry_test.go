package registry_test

import (
	"reflect"
	"testing"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/internal/store"
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

func testRegistry(privKey types.PrivateKey, limit uint64) *registry.Manager {
	return registry.NewManager(privKey, store.NewEphemeralRegistryStore(limit))
}

func TestRegistryPut(t *testing.T) {
	const registryCap = 10
	hostPriv := types.GeneratePrivateKey()
	renterPriv := types.GeneratePrivateKey()
	reg := testRegistry(hostPriv, registryCap)

	// store a random value in the registry
	original := randomValue(renterPriv)
	updated, err := reg.Put(original, registryCap)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(original, updated) {
		t.Fatal("expected returned value to match")
	}

	// test storing the same value again; should fail and return the original
	// value
	updated, err = reg.Put(original, 10)
	if err == nil {
		t.Fatalf("expected validation error")
	} else if !reflect.DeepEqual(original, updated) {
		t.Fatal("expected returned value to match")
	}

	// test updating the value's revision number and data; should succeed
	value := rhpv3.RegistryEntry{
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
	value.Signature = renterPriv.SignHash(value.Hash())
	updated, err = reg.Put(value, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(value, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test updating the value's work; should succeed
	value = rhpv3.RegistryEntry{
		RegistryKey: rhpv3.RegistryKey{
			PublicKey: renterPriv.PublicKey(),
			Tweak:     original.Tweak,
		},
		RegistryValue: rhpv3.RegistryValue{
			Data:     make([]byte, 32),
			Revision: 1,
			Type:     rhpv3.EntryTypeArbitrary,
		},
	}
	for rhpv3.CompareRegistryWork(value, updated) <= 0 {
		frand.Read(value.Data)
	}
	value.Signature = renterPriv.SignHash(value.Hash())
	updated, err = reg.Put(value, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(value, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test setting the value to a primary value; should succeed
	hostID := rhpv3.RegistryHostID(hostPriv.PublicKey())
	value = rhpv3.RegistryEntry{
		RegistryKey: rhpv3.RegistryKey{
			PublicKey: renterPriv.PublicKey(),
			Tweak:     original.Tweak,
		},
		RegistryValue: rhpv3.RegistryValue{
			Data:     append([]byte(hostID[:20]), updated.Data...),
			Revision: 1,
			Type:     rhpv3.EntryTypePubKey,
		},
	}
	value.Signature = renterPriv.SignHash(value.Hash())
	updated, err = reg.Put(value, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(value, updated) {
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
