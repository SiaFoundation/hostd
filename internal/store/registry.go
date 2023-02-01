package store

import (
	"errors"
	"sync"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/registry"
)

// EphemeralRegistryStore implements an in-memory registry key-value store.
// Implements host.RegistryStore.
type EphemeralRegistryStore struct {
	mu sync.Mutex

	cap    uint64
	values map[types.Hash256]rhpv3.RegistryEntry
}

// Close closes the registry store.
func (es *EphemeralRegistryStore) Close() error {
	return nil
}

// Get returns the registry value for the given key.
func (es *EphemeralRegistryStore) Get(key types.Hash256) (rhpv3.RegistryEntry, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	val, exists := es.values[key]
	if !exists {
		return rhpv3.RegistryEntry{}, registry.ErrEntryNotFound
	}
	return val, nil
}

// Set sets the registry value for the given key.
func (es *EphemeralRegistryStore) Set(key types.Hash256, value rhpv3.RegistryEntry, expiration uint64) (rhpv3.RegistryEntry, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	if _, exists := es.values[key]; !exists && uint64(len(es.values)) >= es.cap {
		return rhpv3.RegistryEntry{}, errors.New("capacity exceeded")
	}

	es.values[key] = value
	return value, nil
}

// Len returns the number of entries in the registry.
func (es *EphemeralRegistryStore) Len() uint64 {
	es.mu.Lock()
	defer es.mu.Unlock()

	return uint64(len(es.values))
}

// Cap returns the maximum number of entries the registry can hold.
func (es *EphemeralRegistryStore) Cap() uint64 {
	return es.cap
}

// NewEphemeralRegistryStore initializes a new EphemeralRegistryStore.
func NewEphemeralRegistryStore(limit uint64) *EphemeralRegistryStore {
	return &EphemeralRegistryStore{
		cap:    limit,
		values: make(map[types.Hash256]rhpv3.RegistryEntry),
	}
}
