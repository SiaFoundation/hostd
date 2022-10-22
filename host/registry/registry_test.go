package registry_test

import (
	"crypto/ed25519"
	"reflect"
	"testing"

	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

func randomValue(key ed25519.PrivateKey) (value registry.Value) {
	value.Tweak = frand.Entropy256()
	value.Data = frand.Bytes(32)
	value.Type = registry.EntryTypeArbitrary
	value.PublicKey = types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       key.Public().(ed25519.PublicKey),
	}
	sigHash := value.Hash()
	value.Signature = ed25519.Sign(key, sigHash[:])
	return
}

func testRegistry(priKey ed25519.PrivateKey, limit uint64) *registry.RegistryManager {
	return registry.NewRegistryManager(priKey, store.NewEphemeralRegistryStore(limit))
}

func TestRegistryPut(t *testing.T) {
	const registryCap = 10
	hostPriv := ed25519.NewKeyFromSeed(frand.Bytes(32))
	renterPriv := ed25519.NewKeyFromSeed(frand.Bytes(32))
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
	value := registry.Value{
		Tweak:    original.Tweak,
		Data:     original.Data,
		Revision: 1,
		Type:     registry.EntryTypeArbitrary,
		PublicKey: types.SiaPublicKey{
			Algorithm: types.SignatureEd25519,
			Key:       renterPriv.Public().(ed25519.PublicKey),
		},
	}
	sigHash := value.Hash()
	value.Signature = ed25519.Sign(renterPriv, sigHash[:])
	updated, err = reg.Put(value, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(value, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test updating the value's work; should succeed
	value = registry.Value{
		Tweak:    original.Tweak,
		Data:     make([]byte, 32),
		Revision: 1,
		Type:     registry.EntryTypeArbitrary,
		PublicKey: types.SiaPublicKey{
			Algorithm: types.SignatureEd25519,
			Key:       renterPriv.Public().(ed25519.PublicKey),
		},
	}
	var i int
	for i = 0; i < 1e6; i++ {
		frand.Read(value.Data)
		if value.Work().Cmp(updated.Work()) > 0 {
			break
		}
	}
	sigHash = value.Hash()
	value.Signature = ed25519.Sign(renterPriv, sigHash[:])
	updated, err = reg.Put(value, 10)
	if err != nil {
		t.Fatalf("expected update to succeed, got %s", err)
	} else if !reflect.DeepEqual(value, updated) {
		t.Fatal("expected returned value to match new value")
	}

	// test setting the value to a primary value; should succeed
	hostID := registry.RegistryHostID(hostPriv.Public().(ed25519.PublicKey))
	value = registry.Value{
		Tweak:    original.Tweak,
		Data:     append([]byte(hostID[:20]), updated.Data...),
		Revision: 1,
		Type:     registry.EntryTypePubKey,
		PublicKey: types.SiaPublicKey{
			Algorithm: types.SignatureEd25519,
			Key:       renterPriv.Public().(ed25519.PublicKey),
		},
	}
	sigHash = value.Hash()
	value.Signature = ed25519.Sign(renterPriv, sigHash[:])
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
