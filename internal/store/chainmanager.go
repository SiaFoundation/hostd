package store

import (
	"sync"

	"go.sia.tech/siad/modules"
)

type (
	EphemeralChainManagerStore struct {
		mu         sync.Mutex
		lastChange modules.ConsensusChangeID
	}
)

func (s *EphemeralChainManagerStore) Close() error {
	return nil
}

func (s *EphemeralChainManagerStore) SetLastChange(id modules.ConsensusChangeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastChange = id
	return nil
}

func (s *EphemeralChainManagerStore) GetLastChange() (modules.ConsensusChangeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastChange, nil
}

func NewEphemeralChainManagerStore() *EphemeralChainManagerStore {
	return &EphemeralChainManagerStore{}
}
