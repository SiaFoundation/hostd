package registry

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/siad/crypto"
)

var (
	// ErrEntryNotFound should be returned when a registry key does not exist
	// in the registry.
	ErrEntryNotFound = errors.New("entry not found")
)

type (
	// A RegistryStore stores host registry entries. The registry is a key/value
	// store for small data.
	RegistryStore interface {
		// Get returns the registry value for the given key. If the key is not
		// found should return ErrEntryNotFound.
		Get(crypto.Hash) (Value, error)
		// Set sets the registry value for the given key.
		Set(key crypto.Hash, value Value, expiration uint64) (Value, error)
		// Len returns the number of entries in the registry.
		Len() uint64
		// Cap returns the maximum number of entries the registry can hold.
		Cap() uint64

		Close() error
	}

	// A RegistryManager manages registry entries stored in a RegistryStore.
	RegistryManager struct {
		hostID crypto.Hash
		store  RegistryStore

		// registry entries must be locked while they are being modified
		mu sync.Mutex
	}
)

func (r *RegistryManager) Close() error {
	return r.store.Close()
}

// Get returns the registry value for the provided key.
func (r *RegistryManager) Get(key crypto.Hash) (Value, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.Get(key)
}

// Put creates or updates the registry value for the provided key. If err is nil
// the new value is returned, otherwise the previous value is returned.
func (r *RegistryManager) Put(value Value, expirationHeight uint64) (Value, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ValidateRegistryEntry(value); err != nil {
		return Value{}, fmt.Errorf("invalid registry entry: %w", err)
	}

	// get the current value.
	key := value.Key()
	old, err := r.store.Get(key)
	// if the key doesn't exist, we don't need to validate it further.
	if errors.Is(err, ErrEntryNotFound) {
		if _, err = r.store.Set(key, value, expirationHeight); err != nil {
			return value, fmt.Errorf("failed to create registry key: %w", err)
		}
		return value, nil
	} else if err != nil {
		return old, fmt.Errorf("failed to get registry value: %w", err)
	}

	if err := ValidateRegistryUpdate(old, value, r.hostID); err != nil {
		return old, fmt.Errorf("invalid registry update: %w", err)
	}

	return r.store.Set(key, value, expirationHeight)
}

// NewRegistryManager returns a new registry manager.
func NewRegistryManager(privkey ed25519.PrivateKey, store RegistryStore) *RegistryManager {
	return &RegistryManager{
		hostID: RegistryHostID(privkey.Public().(ed25519.PublicKey)),
		store:  store,
	}
}
