package sqlite

import (
	"fmt"

	"go.sia.tech/hostd/host/storage"
)

type (
	// A SectorStore manages sector metadata.
	SectorStore struct {
		db *Store
	}

	sectorLoc struct {
		storage.SectorLocation
		Root storage.SectorRoot
	}

	sectorUpdateTransaction struct {
		tx txn
	}
)

// AddSectorMetadata adds a sector's metadata to the sector store.
func (tx *sectorUpdateTransaction) AddSectorMetadata(root storage.SectorRoot, loc storage.SectorLocation) error {
	const query = `UPDATE volume_sectors SET sector_root=? WHERE volume_id=? AND volume_index=?;`
	_, err := tx.tx.Exec(query, valueHash(root), valueHash(loc.Volume), loc.Index)
	return err
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume. The location is locked until release is
// called.
func (ss *SectorStore) RemoveSector(root storage.SectorRoot) (loc storage.SectorLocation, err error) {
	const query = `UPDATE volume_sectors SET sector_root=null WHERE sector_root=? RETURNING volume_id, volume_index;`
	err = ss.db.db.QueryRow(query, root).Scan(&loc.Volume, &loc.Index)
	return
}

// SectorLocation returns the location of a sector or an error if the
// sector is not found. The location is locked until release is
// called.
func (ss *SectorStore) SectorLocation(root storage.SectorRoot) (loc storage.SectorLocation, release func() error, err error) {
	err = ss.db.exclusiveTransaction(func(tx txn) error {
		err = ss.db.db.QueryRow(`SELECT volume_id, volume_index FROM volume_sectors WHERE sector_root=?;`, valueHash(root)).Scan(scanHash((*[32]byte)(&loc.Volume)), &loc.Index)
		if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}
		if err := lockSector(tx, loc); err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return
	}
	return loc, unlockSectorFn(ss.db, loc), nil
}

// Prune removes the metadata of all sectors that are no longer
// referenced by either a contract or temporary storage.
func (ss *SectorStore) Prune() error {
	_, err := ss.db.db.Exec(`UPDATE volume_sectors AS vs SET vs.sector_root=null
LEFT JOIN contract_sectors cs ON (cs.sector_root = vs.sector_root)
LEFT JOIN temp_storage_sectors ts ON (ts.sector_root = vs.sector_root)
WHERE ts.sector_root IS NULL AND cs.sector_root IS NULL AND vs.locks<=0;`)
	return err
}

// Update atomically updates the sector store.
func (ss *SectorStore) Update(updateFn func(storage.SectorUpdateTransaction) error) error {
	return ss.db.exclusiveTransaction(func(tx txn) error {
		return updateFn(&sectorUpdateTransaction{tx})
	})
}

func lockSector(tx txn, loc storage.SectorLocation) error {
	_, err := tx.Exec(`UPDATE volume_sectors SET locks=locks+1 WHERE volume_id=? AND volume_index=?`, valueHash(loc.Volume), loc.Index)
	return err
}

func unlockSectorFn(db *Store, loc storage.SectorLocation) func() error {
	var released bool
	return func() error {
		if released {
			panic("sector already released")
		}
		released = true
		return db.exclusiveTransaction(func(tx txn) error {
			_, err := tx.Exec(`UPDATE volume_sectors SET locks=locks-1 WHERE volume_id=? AND volume_index=? AND locks > 0`, valueHash(loc.Volume), loc.Index)
			return err
		})
	}
}

func NewSectorStore(db *Store) *SectorStore {
	return &SectorStore{
		db: db,
	}
}
