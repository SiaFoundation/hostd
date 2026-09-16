package contracts

import (
	"fmt"
	"slices"
	"sync"

	"go.sia.tech/core/types"
)

type rootsCache struct {
	store ContractStore

	mu                sync.RWMutex // protects the fields below
	lastExpiredHeight uint64
	contractSectors   map[types.FileContractID][]types.Hash256
}

// SectorRoots gets the cached sector roots for the contract
func (rc *rootsCache) SectorRoots(id types.FileContractID) []types.Hash256 {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return slices.Clone(rc.contractSectors[id])
}

// UpdateSectorRoots replaces the cached sector roots for the given file contract
func (rc *rootsCache) UpdateSectorRoots(id types.FileContractID, roots []types.Hash256) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.contractSectors[id] = slices.Clone(roots)
}

// ExpireContracts removes the cached sector roots of contracts that were
// rejected or resolved before expireHeight.
func (rc *rootsCache) ExpireContracts(height uint64) error {
	rc.mu.RLock()
	lastExpiredHeight := rc.lastExpiredHeight
	rc.mu.RUnlock()
	if height <= lastExpiredHeight {
		rc.mu.Lock()
		rc.lastExpiredHeight = height
		rc.mu.Unlock()
		return nil
	}

	expired, err := rc.store.ExpiredV2Contracts(lastExpiredHeight, height)
	if err != nil {
		return fmt.Errorf("failed to get expired contracts: %w", err)
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for _, id := range expired {
		delete(rc.contractSectors, id)
	}
	rc.lastExpiredHeight = height
	return nil
}
