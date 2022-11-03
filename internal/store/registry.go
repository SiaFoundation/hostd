package store

import (
	"errors"
	"sync"

	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/siad/crypto"
)

// EphemeralRegistryStore implements an in-memory registry key-value store.
// Implements host.RegistryStore.
type EphemeralRegistryStore struct {
	mu sync.Mutex

	cap    uint64
	values map[crypto.Hash]registry.Value
}

// Close closes the registry store.
func (es *EphemeralRegistryStore) Close() error {
	return nil
}

// Get returns the registry value for the given key.
func (es *EphemeralRegistryStore) Get(key crypto.Hash) (registry.Value, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	val, exists := es.values[key]
	if !exists {
		return registry.Value{}, registry.ErrEntryNotFound
	}
	return val, nil
}

// Set sets the registry value for the given key.
func (es *EphemeralRegistryStore) Set(key crypto.Hash, value registry.Value, expiration uint64) (registry.Value, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	if _, exists := es.values[key]; !exists && uint64(len(es.values)) >= es.cap {
		return registry.Value{}, errors.New("capacity exceeded")
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
		values: make(map[crypto.Hash]registry.Value),
	}
}
