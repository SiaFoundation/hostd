package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/hostd/host/storage"
	"lukechampine.com/frand"
)

// migrateSector returns an empty location in a writeable volume. As a special
// case, if startIndex is non-zero, the new location can be within the same
// volume as the old location even if the volume is not currently writeable. The
// returned location is locked until release is called. If no space is
// available, ErrNotEnoughStorage is returned.
func (s *Store) migrateSector(oldLoc storage.SectorLocation, startIndex uint64) (storage.SectorLocation, func() error, error) {
	const locQuery = `SELECT s.id, s.volume_id, s.volume_index
	FROM volume_sectors s
	INNER JOIN storage_volumes v
	LEFT JOIN locked_volume_sectors l ON s.id=l.volume_sector_id
	WHERE l.volume_sector_id IS NULL AND s.sector_root IS NULL AND (v.writeable=true OR (s.volume_id=? AND s.volume_index<?))
	ORDER BY s.volume_index LIMIT 1;`

	var newLoc storage.SectorLocation
	var lockID uint64
	err := s.exclusiveTransaction(func(tx txn) error {
		// get a sector location. If no rows are returned, there is no remaining
		// space.
		err := tx.QueryRow(locQuery, valueHash(oldLoc.Volume), startIndex).Scan(&newLoc.ID, scanHash((*[32]byte)(&newLoc.Volume)), &newLoc.Index)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrNotEnoughStorage
		} else if err != nil {
			return fmt.Errorf("failed to find open sector location: %w", err)
		}

		// lock the sector location
		lockID, err = s.lockSector(tx, newLoc.ID)
		if err != nil {
			return fmt.Errorf("failed to lock sector location: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.SectorLocation{}, nil, err
	}
	return newLoc, s.unlockSectorFn(lockID), err
}

// Volumes returns a list of all volumes.
func (s *Store) Volumes() ([]storage.Volume, error) {
	const query = `SELECT v.id, v.disk_path, NOT v.writeable, 
	COUNT(*) AS total_sectors,
	COUNT(*) FILTER (WHERE s.sector_root IS NOT NULL) AS used_sectors
FROM storage_volumes v
LEFT JOIN volume_sectors s`
	rows, err := s.db.QueryContext(context.Background(), query)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var volumes []storage.Volume
	for rows.Next() {
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan volume: %w", err)
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

// Volume returns a volume by its ID.
func (s *Store) Volume(id storage.VolumeID) (storage.Volume, error) {
	const query = `SELECT v.id, v.disk_path, NOT v.writeable, 
	COUNT(*) AS total_sectors,
	COUNT(*) FILTER (WHERE s.sector_root IS NOT NULL) AS used_sectors
FROM storage_volumes v
LEFT JOIN volume_sectors s
WHERE v.id=$1`
	row := s.db.QueryRow(query, valueHash(id))
	vol, err := scanVolume(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Volume{}, storage.ErrVolumeNotFound
	} else if err != nil {
		return storage.Volume{}, fmt.Errorf("query failed: %w", err)
	}
	return vol, nil
}

// StoreSector calls fn with an empty location in a writeable volume. If
// the sector root already exists, fn is called with the existing
// location and exists is true. Unless exists is true, The sector must
// be written to disk within fn. If fn returns an error, the metadata is
// rolled back. If no space is available, ErrNotEnoughStorage is
// returned. The location is locked until release is called.
//
// The sector should be referenced by either a contract or temp store
// before release is called to prevent Prune() from removing it.
func (s *Store) StoreSector(root storage.SectorRoot, fn func(loc storage.SectorLocation, exists bool) error) (release func() error, err error) {
	// SQLite sorts nulls first -- sort by sector_root DESC, volume_index
	// ASC to push the existing index to the top.
	const locQuery = `SELECT s.id, s.volume_id, s.volume_index, s.sector_root IS NOT NULL AS sector_exists
FROM volume_sectors s
INNER JOIN storage_volumes v
LEFT JOIN locked_volume_sectors l ON s.id=l.volume_sector_id
WHERE s.sector_root=? OR l.volume_sector_id IS NULL AND v.writeable=true AND s.sector_root IS NULL
ORDER BY s.sector_root DESC, s.volume_index ASC LIMIT 1;`

	var location storage.SectorLocation
	var exists bool
	var lockID uint64
	err = s.exclusiveTransaction(func(tx txn) error {
		// get a sector location. If no rows are returned, there is no remaining
		// space.
		err := tx.QueryRow(locQuery, valueHash(root)).Scan(&location.ID, scanHash((*[32]byte)(&location.Volume)), &location.Index, &exists)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrNotEnoughStorage
		} else if err != nil {
			return fmt.Errorf("failed to find open sector location: %w", err)
		}

		// lock the sector location
		lockID, err = s.lockSector(tx, location.ID)
		if err != nil {
			return fmt.Errorf("failed to lock sector location: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// call fn with the location
	if err := fn(location, exists); err != nil {
		return nil, fmt.Errorf("failed to store sector: %w", err)
	}
	// commit the sector location
	var updatedID uint64
	err = s.db.QueryRow(`UPDATE volume_sectors SET sector_root=$1 WHERE id=$2 RETURNING id`, valueHash(root), location.ID).Scan(&updatedID)
	if err != nil {
		return nil, fmt.Errorf("failed to commit sector location: %w", err)
	} else if updatedID != location.ID {
		panic("sector location not updated correctly")
	}
	return s.unlockSectorFn(lockID), nil
}

// MigrateSectors returns a new location for each occupied sector of a volume
// starting at startIndex. The sector data should be copied to the new location
// during migrateFn. Iteration is stopped if migrateFn returns an error. Changes
// are committed after commitFn.
func (s *Store) MigrateSectors(id storage.VolumeID, startIndex uint64, migrateFn func(root storage.SectorRoot, newLoc storage.SectorLocation) error, commitFn func() error) error {
	// batch changes until commitFn is called
	var changes []sectorLoc
	for {
		// get the old location and lock it while its being migrated
		var oldLoc sectorLoc
		var lockID uint64
		err := s.exclusiveTransaction(func(tx txn) error {
			err := tx.QueryRow(`SELECT id, sector_root, volume_id, volume_index FROM volume_sectors WHERE volume_id=$1 AND volume_index>$2 AND sector_root IS NOT NULL ORDER BY volume_index ASC LIMIT 1`, valueHash(id), startIndex).
				Scan(&oldLoc.ID, scanHash((*[32]byte)(&oldLoc.Root)), scanHash((*[32]byte)(&oldLoc.Volume)), &oldLoc.Index)
			if err != nil {
				return fmt.Errorf("failed to get empty sector location: %w", err)
			}
			lockID, err = s.lockSector(tx, oldLoc.ID)
			if err != nil {
				return fmt.Errorf("failed to lock sector location: %w", err)
			}
			return nil
		})
		if errors.Is(err, sql.ErrNoRows) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to get next sector: %w", err)
		}
		defer s.unlockSector(lockID)

		// get a new location
		newLoc, release, err := s.migrateSector(oldLoc.SectorLocation, startIndex)
		if err != nil {
			return fmt.Errorf("failed to get new sector location: %w", err)
		}
		defer release()

		// migrate the sector
		if err = migrateFn(oldLoc.Root, newLoc); err != nil {
			return fmt.Errorf("failed to migrate sector %v: %w", oldLoc.Root, err)
		}

		changes = append(changes, sectorLoc{
			SectorLocation: newLoc,
			Root:           oldLoc.Root,
		})
	}

	// commit the volumes
	if err := commitFn(); err != nil {
		return fmt.Errorf("failed to commit sector volumes: %w", err)
	}

	// update the sector locations
	err := s.exclusiveTransaction(func(tx txn) error {
		updateStmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_root=NULL WHERE sector_root=?;
UPDATE volume_sectors SET sector_root=? WHERE id=?;`)
		if err != nil {
			return fmt.Errorf("failed to prepare sector update statement: %w", err)
		}
		defer updateStmt.Close()
		for _, change := range changes {
			if _, err := updateStmt.Exec(valueHash(change.Root), valueHash(change.Root), valueHash(change.Volume), change.Index); err != nil {
				return fmt.Errorf("failed to update sector location: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update sector locations: %w", err)
	}
	return nil
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (storage.Volume, error) {
	id := frand.Entropy256()
	const query = `INSERT INTO storage_volumes (id, disk_path, writeable) VALUES (?, ?, ?);`
	_, err := s.db.Exec(query, valueHash(id), localPath, !readOnly)
	if err != nil {
		return storage.Volume{}, err
	}
	return storage.Volume{
		ID:        id,
		LocalPath: localPath,
		ReadOnly:  readOnly,
	}, nil
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
func (s *Store) RemoveVolume(id storage.VolumeID, force bool) error {
	return s.exclusiveTransaction(func(tx txn) error {
		if !force {
			// check if the volume is empty
			var count int
			err := tx.QueryRow(`SELECT COUNT(*) FROM volume_sectors WHERE volume_id=$1 AND sector_root IS NOT NULL;`, valueHash(id)).Scan(&count)
			if err != nil {
				return fmt.Errorf("failed to check if volume is empty: %w", err)
			} else if count != 0 {
				return storage.ErrVolumeNotEmpty
			}
		}

		// remove the volume sectors
		_, err := tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=?;`, valueHash(id))
		if err != nil {
			return fmt.Errorf("failed to remove volume sectors: %w", err)
		}

		// remove the volume
		_, err = tx.Exec(`DELETE FROM storage_volumes WHERE id=?;`, valueHash(id))
		if err != nil {
			return fmt.Errorf("failed to remove volume: %w", err)
		}
		return nil
	})
}

// GrowVolume grows a storage volume's metadata by n sectors.
func (s *Store) GrowVolume(id storage.VolumeID, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}
	return s.exclusiveTransaction(func(tx txn) error {
		var nextIndex uint64
		err := tx.QueryRow(`SELECT COALESCE(MAX(volume_index)+1, 0) AS next_index FROM volume_sectors WHERE volume_id=?;`, valueHash(id)).Scan(&nextIndex)
		if err != nil {
			return fmt.Errorf("failed to get last volume index: %w", err)
		}

		stmt, err := tx.Prepare(`INSERT INTO volume_sectors (volume_id, volume_index) VALUES ($1, $2);`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		if nextIndex >= maxSectors {
			panic(fmt.Errorf("nextIndex must be less than maxSectors: %v <= %v", nextIndex, maxSectors)) // dev error
		}

		for i := nextIndex; i < maxSectors; i++ {
			if _, err = stmt.Exec(valueHash(id), i); err != nil {
				return fmt.Errorf("failed to grow volume: %w", err)
			}
		}
		return nil
	})
}

// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If there are
// used sectors outside of the new maximum, ErrVolumeNotEmpty is returned.
func (s *Store) ShrinkVolume(id storage.VolumeID, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}
	return s.exclusiveTransaction(func(tx txn) error {
		// check if there are any used sectors in the shrink range
		var count uint64
		err := tx.QueryRow(`SELECT COUNT(sector_root) FROM volume_sectors WHERE volume_id=$1 AND volume_index > $2 AND sector_root IS NOT NULL;`, valueHash(id), maxSectors).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to get used sectors: %w", err)
		} else if count != 0 {
			return fmt.Errorf("cannot shrink volume to %d sectors, %d sectors are in use: %w", maxSectors, count, storage.ErrVolumeNotEmpty)
		}
		_, err = tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=$1 AND volume_index > $2;`, valueHash(id), maxSectors)
		if err != nil {
			return fmt.Errorf("failed to shrink volume: %w", err)
		}
		return nil
	})
}

// SetReadOnly sets the read-only flag on a volume.
func (s *Store) SetReadOnly(id storage.VolumeID, readOnly bool) error {
	const query = `UPDATE storage_volumes SET writeable=? WHERE id=?;`
	_, err := s.db.Exec(query, !readOnly, valueHash(id))
	return err
}

func scanVolume(scanner row) (volume storage.Volume, err error) {
	err = scanner.Scan(scanHash((*[32]byte)(&volume.ID)), &volume.LocalPath, &volume.ReadOnly, &volume.TotalSectors, &volume.UsedSectors)
	return
}
