package store

import (
	"errors"
	"math"
	"sync"

	"go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/siad/crypto"
)

type sectorMeta struct {
	data       []byte
	references uint64
}

// An EphemeralStorageManager implements an in-memory sector store.
type EphemeralStorageManager struct {
	mu      sync.Mutex
	sectors map[crypto.Hash]*sectorMeta
}

func (es *EphemeralStorageManager) Close() error {
	return nil
}

func (es *EphemeralStorageManager) Usage() (used, total uint64, _ error) {
	return uint64(len(es.sectors)) * rhp.SectorSize, math.MaxUint64, nil
}

func (es *EphemeralStorageManager) HasSector(root crypto.Hash) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	_, exists := es.sectors[root]
	return exists, nil
}

// AddSector adds a sector to the storage manager.
func (es *EphemeralStorageManager) AddSector(root crypto.Hash, sector []byte, refs int) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if _, exists := es.sectors[root]; exists {
		es.sectors[root].references += uint64(refs)
		return nil
	}
	es.sectors[root] = &sectorMeta{
		data:       sector,
		references: 1,
	}
	return nil
}

// DeleteSector deletes the sector with the given root.
func (es *EphemeralStorageManager) DeleteSector(root crypto.Hash, refs int) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	sector, exists := es.sectors[root]
	if !exists {
		return errors.New("sector not found")
	} else if sector.references <= uint64(refs) {
		delete(es.sectors, root)
	} else {
		es.sectors[root].references--
		if es.sectors[root].references == 0 {
			delete(es.sectors, root)
		}
	}
	return nil
}

// Sector reads a sector from the store
func (es *EphemeralStorageManager) Sector(root crypto.Hash) ([]byte, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	sector, exists := es.sectors[root]
	if !exists {
		return nil, errors.New("sector not found")
	}
	buf := append([]byte(nil), sector.data...)
	return buf, nil
}

func NewEphemeralStorageManager() *EphemeralStorageManager {
	return &EphemeralStorageManager{
		sectors: make(map[crypto.Hash]*sectorMeta),
	}
}
