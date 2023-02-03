package registry

import (
	"errors"
	"fmt"
	"sync"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	// ErrEntryNotFound should be returned when a registry key does not exist
	// in the registry.
	ErrEntryNotFound = errors.New("entry not found")
)

type (
	// A Store stores host registry entries. The registry is a key/value
	// store for small data.
	Store interface {
		// Get returns the registry value for the given key. If the key is not
		// found should return ErrEntryNotFound.
		Get(types.Hash256) (rhpv3.RegistryEntry, error)
		// Set sets the registry value for the given key.
		Set(key types.Hash256, value rhpv3.RegistryEntry, expiration uint64) (rhpv3.RegistryEntry, error)
		// Len returns the number of entries in the registry.
		Len() uint64
		// Cap returns the maximum number of entries the registry can hold.
		Cap() uint64

		Close() error
	}

	// A Manager manages registry entries stored in a RegistryStore.
	Manager struct {
		hostID types.Hash256
		store  Store

		// registry entries must be locked while they are being modified
		mu sync.Mutex
	}
)

// Close closes the registry store.
func (r *Manager) Close() error {
	return r.store.Close()
}

// Cap returns the maximum number of entries the registry can hold.
func (r *Manager) Cap() uint64 {
	return r.store.Cap()
}

// Len returns the number of entries in the registry.
func (r *Manager) Len() uint64 {
	return r.store.Len()
}

// Get returns the registry value for the provided key.
func (r *Manager) Get(key types.Hash256) (rhpv3.RegistryEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.Get(key)
}

// Put creates or updates the registry value for the provided key. If err is nil
// the new value is returned, otherwise the previous value is returned.
func (r *Manager) Put(value rhpv3.RegistryEntry, expirationHeight uint64) (rhpv3.RegistryEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := rhpv3.ValidateRegistryEntry(value); err != nil {
		return rhpv3.RegistryEntry{}, fmt.Errorf("invalid registry entry: %w", err)
	}

	// get the current value.
	key := value.RegistryKey.Hash()
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

	if err := rhpv3.ValidateRegistryUpdate(old, value, r.hostID); err != nil {
		return old, fmt.Errorf("invalid registry update: %w", err)
	}

	return r.store.Set(key, value, expirationHeight)
}

// NewManager returns a new registry manager.
func NewManager(privkey types.PrivateKey, store Store) *Manager {
	return &Manager{
		hostID: rhpv3.RegistryHostID(privkey.PublicKey()),
		store:  store,
	}
}
