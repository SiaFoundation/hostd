package registry

import (
	"errors"
	"fmt"
	"sync"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
)

var (
	// ErrEntryNotFound should be returned when a registry key does not exist
	// in the registry.
	ErrEntryNotFound = errors.New("entry not found")

	// ErrNotEnoughSpace should be returned when the registry is full and
	// there is no more space to store a new entry.
	ErrNotEnoughSpace = errors.New("not enough space")
)

type (
	// A Store stores host registry entries. The registry is a key/value
	// store for small data.
	Store interface {
		// GetRegistryValue returns the registry value for the given key. If the key is not
		// found should return ErrEntryNotFound.
		GetRegistryValue(key rhp3.RegistryKey) (entry rhp3.RegistryValue, _ error)
		// SetRegistryValue sets the registry value for the given key. If the
		// value would exceed the maximum number of entries, should return
		// ErrNotEnoughSpace.
		SetRegistryValue(entry rhp3.RegistryEntry, expiration uint64) error
		// RegistryEntries returns the current number of entries as well as the
		// maximum number of entries the registry can hold.
		RegistryEntries() (count uint64, total uint64, err error)

		IncrementRegistryAccess(read, write uint64) error
	}

	// A Manager manages registry entries stored in a RegistryStore.
	Manager struct {
		hostID types.Hash256

		store    Store
		tg       *threadgroup.ThreadGroup
		recorder *registryAccessRecorder

		// registry entries must be locked while they are being modified
		mu sync.Mutex
	}
)

// Close closes the registry store.
func (r *Manager) Close() error {
	r.tg.Stop()
	// flush any metrics
	r.recorder.Flush()
	return nil
}

// Entries returns the current and maximum number of entries the registry can
// hold.
func (r *Manager) Entries() (count uint64, total uint64, err error) {
	return r.store.RegistryEntries()
}

// Get returns the registry value for the provided key.
func (r *Manager) Get(key rhp3.RegistryKey) (value rhp3.RegistryValue, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, err = r.store.GetRegistryValue(key)
	if err == nil {
		r.recorder.AddRead()
	}
	return
}

// Put creates or updates the registry value for the provided key. If err is nil
// the new value is returned, otherwise the previous value is returned.
func (r *Manager) Put(entry rhp3.RegistryEntry, expirationHeight uint64) (rhp3.RegistryValue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := rhp3.ValidateRegistryEntry(entry); err != nil {
		return rhp3.RegistryValue{}, fmt.Errorf("invalid registry entry: %w", err)
	}

	// get the current value.
	old, err := r.store.GetRegistryValue(entry.RegistryKey)
	// if the key doesn't exist, we don't need to validate it further.
	if errors.Is(err, ErrEntryNotFound) {
		if err = r.store.SetRegistryValue(entry, expirationHeight); err != nil {
			return entry.RegistryValue, fmt.Errorf("failed to create registry key: %w", err)
		}
		return entry.RegistryValue, nil
	} else if err != nil {
		return old, fmt.Errorf("failed to get registry value: %w", err)
	}
	oldEntry := rhp3.RegistryEntry{
		RegistryKey:   entry.RegistryKey,
		RegistryValue: old,
	}

	if err := rhp3.ValidateRegistryUpdate(oldEntry, entry, r.hostID); err != nil {
		return old, fmt.Errorf("invalid registry update: %w", err)
	} else if err = r.store.SetRegistryValue(entry, expirationHeight); err != nil {
		return old, fmt.Errorf("failed to update registry key: %w", err)
	}
	r.recorder.AddWrite()
	return entry.RegistryValue, nil
}

// NewManager returns a new registry manager.
func NewManager(privkey types.PrivateKey, store Store, log *zap.Logger) *Manager {
	m := &Manager{
		hostID: rhp3.RegistryHostID(privkey.PublicKey()),
		tg:     threadgroup.New(),
		store:  store,
		recorder: &registryAccessRecorder{
			log: log.Named("recorder"),
		},
	}
	go m.recorder.Run(m.tg.Done())
	return m
}
