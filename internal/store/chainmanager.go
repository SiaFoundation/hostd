package store

import (
	"sync"

	"go.sia.tech/siad/modules"
)

type (
	// An EphemeralChainManagerStore stores the last consensus change ID in memory.
	EphemeralChainManagerStore struct {
		mu         sync.Mutex
		lastChange modules.ConsensusChangeID
	}
)

// Close closes the store.
func (s *EphemeralChainManagerStore) Close() error {
	return nil
}

// SetLastChange updates the last consensus change ID.
func (s *EphemeralChainManagerStore) SetLastChange(id modules.ConsensusChangeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastChange = id
	return nil
}

// GetLastChange returns the last consensus change ID.
func (s *EphemeralChainManagerStore) GetLastChange() (modules.ConsensusChangeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastChange, nil
}

// NewEphemeralChainManagerStore creates a new ephemeral chain manager store.
func NewEphemeralChainManagerStore() *EphemeralChainManagerStore {
	return &EphemeralChainManagerStore{}
}
