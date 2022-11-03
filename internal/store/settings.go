package store

import (
	"sync"

	"go.sia.tech/hostd/host/settings"
)

// An EphemeralSettingsStore implements an in-memory settings store.
type EphemeralSettingsStore struct {
	mu       sync.Mutex
	settings settings.Settings
}

// Close closes the settings store.
func (es *EphemeralSettingsStore) Close() error {
	return nil
}

// Settings returns the host's current settings. Implements host.SettingsStore.
func (es *EphemeralSettingsStore) Settings() (settings.Settings, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.settings, nil
}

// UpdateSettings updates the host's settings. Implements host.SettingsStore.
func (es *EphemeralSettingsStore) UpdateSettings(s settings.Settings) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	s.Revision = es.settings.Revision + 1
	es.settings = s
	return nil
}

// NewEphemeralSettingsStore returns a new EphemeralSettingsStore.
func NewEphemeralSettingsStore() *EphemeralSettingsStore {
	return &EphemeralSettingsStore{}
}
