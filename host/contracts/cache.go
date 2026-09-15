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
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if height > rc.lastExpiredHeight {
		expired, err := rc.store.ExpiredV2Contracts(rc.lastExpiredHeight, height)
		if err != nil {
			return fmt.Errorf("failed to get expired contracts: %w", err)
		}
		for _, id := range expired {
			delete(rc.contractSectors, id)
		}
	}
	rc.lastExpiredHeight = height
	return nil
}
