package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.sia.tech/hostd/host/storage"
)

const sectorSize = 1 << 22

// StorageUsage returns the number of bytes used and the total number of bytes
// available in the storage pool.
func (s *Store) StorageUsage() (usedBytes, totalBytes uint64, _ error) {
	// nulls are not included in COUNT() -- counting sector roots is equivalent
	// to counting used sectors.
	const query = `SELECT COUNT(id) AS total_sectors, COUNT(sector_root) AS used_sectors FROM volume_sectors`
	err := s.db.QueryRow(query).Scan(&totalBytes, &usedBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("query failed: %w", err)
	}
	totalBytes *= sectorSize
	usedBytes *= sectorSize
	return usedBytes, totalBytes, nil
}

// Volumes returns a list of all volumes.
func (s *Store) Volumes() ([]storage.Volume, error) {
	const query = `SELECT v.id, v.disk_path, NOT v.writeable, COUNT(s.id) AS total_sectors, COUNT(s.sector_root) AS used_sectors
	FROM storage_volumes v
	LEFT JOIN volume_sectors s ON (s.volume_id = v.id)
	GROUP BY v.id
	ORDER BY v.id ASC`
	rows, err := s.db.Query(query)
	if err != nil {
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
func (s *Store) Volume(id int) (storage.Volume, error) {
	const query = `SELECT v.id, v.disk_path, NOT v.writeable, 
	COUNT(s.id) AS total_sectors,
	COUNT(s.id) FILTER (WHERE s.sector_root IS NOT NULL) AS used_sectors
FROM storage_volumes v
LEFT JOIN volume_sectors s ON (s.volume_id = v.id)
WHERE v.id=$1`
	row := s.db.QueryRow(query, id)
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
		err := tx.QueryRow(locQuery, valueHash(root)).Scan(&location.ID, &location.Volume, &location.Index, &exists)
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

// MigrateSectors migrates each occupied sector of a volume starting at
// startIndex. The sector data should be copied to the new location and synced
// to disk during migrateFn. Sectors are migrated in batches of 256.
func (s *Store) MigrateSectors(volumeID int, startIndex uint64, migrateFn func(locations []storage.SectorLocation) error) error {
	const batchSize = 256 // migrate 1GiB at a time
	for {
		// get a batch of sectors to migrate
		var oldLocations, newLocations []storage.SectorLocation
		var locks []uint64
		err := s.exclusiveTransaction(func(tx txn) (err error) {
			oldLocations, err = sectorsForMigration(tx, volumeID, startIndex, batchSize)
			if err != nil {
				return fmt.Errorf("failed to get sectors for migration: %w", err)
			} else if len(oldLocations) == 0 {
				return nil // no more sectors to migrate
			}

			// get new locations for each sector
			newLocations, err = locationsForMigration(tx, volumeID, startIndex, len(oldLocations))
			if err != nil {
				return fmt.Errorf("failed to get new locations: %w", err)
			} else if len(newLocations) != len(oldLocations) {
				return storage.ErrNotEnoughStorage // not enough space to complete the batch
			}

			// add the sector root to the new locations
			for i := range newLocations {
				newLocations[i].Root = oldLocations[i].Root
			}

			// lock the old and new locations
			locks, err = lockSectorBatch(tx, append(oldLocations, newLocations...)...)
			if err != nil {
				return fmt.Errorf("failed to lock sectors: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to move sectors: %w", err)
		} else if len(oldLocations) == 0 {
			return nil // no more sectors to migrate
		}
		defer unlockSectorBatch(s.db, locks...)

		// call migrateFn with the new locations, data should be copied to the
		// new locations and synced to disk
		if err := migrateFn(newLocations); err != nil {
			return fmt.Errorf("failed to migrate sectors: %w", err)
		}

		// update the sector locations in a separate transaction
		err = s.exclusiveTransaction(func(tx txn) error {
			var oldIDs []string
			for _, loc := range oldLocations {
				oldIDs = append(oldIDs, strconv.FormatUint(loc.ID, 10))
			}
			clearQuery := fmt.Sprintf(`UPDATE volume_sectors SET sector_root=NULL WHERE id IN (%s);`, strings.Join(oldIDs, ","))
			if _, err := tx.Exec(clearQuery); err != nil {
				return fmt.Errorf("failed to clear old sector locations: %w", err)
			}

			updateStmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_root=? WHERE id=? RETURNING id;`)
			if err != nil {
				return fmt.Errorf("failed to prepare sector update statement: %w", err)
			}
			defer updateStmt.Close()
			for _, loc := range newLocations {
				var newID uint64
				if err = updateStmt.QueryRow(valueHash(loc.Root), loc.ID).Scan(&newID); err != nil {
					return fmt.Errorf("failed to update sector location: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int, err error) {
	const query = `INSERT INTO storage_volumes (disk_path, writeable) VALUES (?, ?) RETURNING id;`
	err = s.db.QueryRow(query, localPath, !readOnly).Scan(&volumeID)
	return
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
func (s *Store) RemoveVolume(id int, force bool) error {
	return s.exclusiveTransaction(func(tx txn) error {
		if !force {
			// check if the volume is empty
			var count int
			err := tx.QueryRow(`SELECT COUNT(*) FROM volume_sectors WHERE volume_id=$1 AND sector_root IS NOT NULL;`, id).Scan(&count)
			if err != nil {
				return fmt.Errorf("failed to check if volume is empty: %w", err)
			} else if count != 0 {
				return storage.ErrVolumeNotEmpty
			}
		}

		// remove the volume sectors
		_, err := tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=?;`, id)
		if err != nil {
			return fmt.Errorf("failed to remove volume sectors: %w", err)
		}

		// remove the volume
		_, err = tx.Exec(`DELETE FROM storage_volumes WHERE id=?;`, id)
		if err != nil {
			return fmt.Errorf("failed to remove volume: %w", err)
		}
		return nil
	})
}

// GrowVolume grows a storage volume's metadata by n sectors.
func (s *Store) GrowVolume(id int, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}
	return s.exclusiveTransaction(func(tx txn) error {
		var nextIndex uint64
		err := tx.QueryRow(`SELECT COALESCE(MAX(volume_index)+1, 0) AS next_index FROM volume_sectors WHERE volume_id=?;`, id).Scan(&nextIndex)
		if err != nil {
			return fmt.Errorf("failed to get last volume index: %w", err)
		}

		stmt, err := tx.Prepare(`INSERT INTO volume_sectors (volume_id, volume_index) VALUES ($1, $2);`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		if nextIndex >= maxSectors {
			panic(fmt.Errorf("nextIndex must be less than maxSectors: %v < %v", nextIndex, maxSectors)) // dev error
		}

		for i := nextIndex; i < maxSectors; i++ {
			if _, err = stmt.Exec(id, i); err != nil {
				return fmt.Errorf("failed to grow volume: %w", err)
			}
		}
		return nil
	})
}

// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If there are
// used sectors outside of the new maximum, ErrVolumeNotEmpty is returned.
func (s *Store) ShrinkVolume(id int, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}
	return s.exclusiveTransaction(func(tx txn) error {
		// check if there are any used sectors in the shrink range
		var count uint64
		err := tx.QueryRow(`SELECT COUNT(sector_root) FROM volume_sectors WHERE volume_id=$1 AND volume_index >= $2 AND sector_root IS NOT NULL;`, id, maxSectors).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to get used sectors: %w", err)
		} else if count != 0 {
			return fmt.Errorf("cannot shrink volume to %d sectors, %d sectors are in use: %w", maxSectors, count, storage.ErrVolumeNotEmpty)
		}
		_, err = tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=$1 AND volume_index >= $2;`, id, maxSectors)
		if err != nil {
			return fmt.Errorf("failed to shrink volume: %w", err)
		}
		return nil
	})
}

// SetReadOnly sets the read-only flag on a volume.
func (s *Store) SetReadOnly(volumeID int, readOnly bool) error {
	const query = `UPDATE storage_volumes SET writeable=? WHERE id=?;`
	_, err := s.db.Exec(query, !readOnly, volumeID)
	return err
}

// sectorsForMigration returns a list of sectors that should be migrated from
// the given volume. The sectors are ordered by volume index, and the returned
// list will contain at most batchSize sectors. The startIndex is the volume
// index of the first sector to return.
func sectorsForMigration(tx txn, volumeID int, startIndex uint64, batchSize int64) (sectors []storage.SectorLocation, _ error) {
	const query = `SELECT id, sector_root, volume_id, volume_index 
	FROM volume_sectors
	WHERE volume_id=$1 AND volume_index>=$2 AND sector_root IS NOT NULL
	ORDER BY volume_index ASC LIMIT $3`

	rows, err := tx.Query(query, volumeID, startIndex, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query sectors: %w", err)
	}
	defer rows.Close()

	// scan the old locations
	for rows.Next() {
		var loc storage.SectorLocation
		if err := rows.Scan(&loc.ID, scanHash((*[32]byte)(&loc.Root)), &loc.Volume, &loc.Index); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector: %w", err)
		}
		sectors = append(sectors, loc)
	}
	return sectors, nil
}

// locationsForMigration returns a list of locations to migrate to. Locations
// are guaranteed to be empty and in a writeable volume. As a special case,
// sectors may be migrated to empty indices less than startIndex within the
// given volume even if the volue is read-only. The locations are ordered by
// volume index and the returned list will contain at most batchSize locations.
func locationsForMigration(tx txn, volumeID int, startIndex uint64, batchSize int) (locations []storage.SectorLocation, _ error) {
	const query = `SELECT s.id, s.volume_id, s.volume_index
	FROM volume_sectors s
	INNER JOIN storage_volumes v ON (s.volume_id=v.id)
	LEFT JOIN locked_volume_sectors l ON (s.id=l.volume_sector_id)
	WHERE l.volume_sector_id IS NULL AND s.sector_root IS NULL AND ((v.writeable=true AND s.volume_id <> $1) OR (s.volume_id=$1 AND s.volume_index<$2))
	ORDER BY s.volume_index LIMIT $3;`

	rows, err := tx.Query(query, volumeID, startIndex, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query locations: %w", err)
	}
	defer rows.Close()

	// scan the new locations
	for rows.Next() {
		var loc storage.SectorLocation
		if err := rows.Scan(&loc.ID, &loc.Volume, &loc.Index); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector: %w", err)
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

func scanVolume(scanner row) (volume storage.Volume, err error) {
	err = scanner.Scan(&volume.ID, &volume.LocalPath, &volume.ReadOnly, &volume.TotalSectors, &volume.UsedSectors)
	return
}
