package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
)

// lockSector locks a sector location and returns a lock ID. The lock
// id is used with unlockSector to unlock the sector.
func (s *Store) lockSector(tx txn, locationID uint64) (uint64, error) {
	var lockID uint64
	err := tx.QueryRow(`INSERT INTO locked_volume_sectors (volume_sector_id) VALUES ($1) RETURNING id;`, locationID).
		Scan(&lockID)
	return lockID, err
}

// unlockSector unlocks a locked sector location. It is safe to call
// multiple times.
func (s *Store) unlockSector(id uint64) error {
	_, err := s.db.Exec(`DELETE FROM locked_volume_sectors WHERE id=?;`, id)
	return err
}

// unlockSectorFn returns a function that unlocks a sector when called.
func (s *Store) unlockSectorFn(id uint64) func() error {
	return func() error { return s.unlockSector(id) }
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	var id uint64
	const query = `UPDATE volume_sectors SET sector_root=null WHERE sector_root=$1 RETURNING id;`
	err = s.db.QueryRow(query, sqlHash256(root)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrSectorNotFound
	}
	return
}

// SectorLocation returns the location of a sector or an error if the
// sector is not found. The location is locked until release is
// called.
func (s *Store) SectorLocation(root types.Hash256) (loc storage.SectorLocation, release func() error, err error) {
	var lockID uint64
	err = s.transaction(func(tx txn) error {
		err = s.db.QueryRow(`SELECT id, volume_id, volume_index FROM volume_sectors WHERE sector_root=?;`, sqlHash256(root)).Scan(&loc.ID, &loc.Volume, &loc.Index)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}
		lockID, err = s.lockSector(tx, loc.ID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return
	}
	return loc, s.unlockSectorFn(lockID), nil
}

// Prune removes the metadata of any sectors that are not locked or referenced
// by a contract.
func (s *Store) Prune() error {
	_, err := s.db.Exec(`UPDATE volume_sectors AS vs SET vs.sector_root=null
LEFT JOIN contract_sectors cs ON (cs.sector_root = vs.sector_root)
LEFT JOIN temp_storage_sectors ts ON (ts.sector_root = vs.sector_root)
LEFT JOIN locked_volume_sectors lvs ON (lvs.volume_sector_id = vs.id)
WHERE ts.sector_root IS NULL AND cs.sector_root IS NULL AND lvs.id IS NULL;`)
	return err
}

// lockSectorBatch locks multiple sector locations and returns a list of lock
// IDs. The lock ids can be used with either unlockSector or unlockSectorBatch
// to unlock the locations.
func lockSectorBatch(tx txn, locations ...storage.SectorLocation) (locks []uint64, err error) {
	if len(locations) == 0 {
		return nil, nil
	}
	stmt, err := tx.Prepare(`INSERT INTO locked_volume_sectors (volume_sector_id) VALUES ($1) RETURNING id;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()
	for _, location := range locations {
		var lockID uint64
		err := stmt.QueryRow(location.ID).Scan(&lockID)
		if err != nil {
			return nil, fmt.Errorf("failed to lock location %v:%v: %w", location.Volume, location.Index, err)
		}
		locks = append(locks, lockID)
	}
	return
}

// unlockSectorBatch unlocks multiple locked sector locations. It is safe to
// call multiple times.
func unlockSectorBatch(tx txn, ids ...uint64) error {
	if len(ids) == 0 {
		return nil
	}
	query := `DELETE FROM locked_volume_sectors WHERE id=$1`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err := stmt.Exec(id); err != nil {
			return fmt.Errorf("failed to unlock sector %v: %w", id, err)
		}
	}
	return nil
}
