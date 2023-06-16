package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
)

type volumeSectorRef struct {
	ID    int64
	Empty bool
}

// StorageUsage returns the number of sectors stored and the total number of sectors
// available in the storage pool.
func (s *Store) StorageUsage() (usedSectors, totalSectors uint64, err error) {
	// nulls are not included in COUNT() -- counting sector roots is equivalent
	// to counting used sectors.
	const query = `SELECT COALESCE(SUM(total_sectors), 0) AS total_sectors, COALESCE(SUM(used_sectors), 0) AS used_sectors FROM storage_volumes`
	err = s.queryRow(query).Scan(&totalSectors, &usedSectors)
	return
}

// Volumes returns a list of all volumes.
func (s *Store) Volumes() ([]storage.Volume, error) {
	const query = `SELECT v.id, v.disk_path, v.read_only, v.available, v.total_sectors, v.used_sectors
FROM storage_volumes v
ORDER BY v.id ASC`
	rows, err := s.query(query)
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
	const query = `SELECT v.id, v.disk_path, v.read_only, v.available, v.total_sectors, v.used_sectors
FROM storage_volumes v
WHERE v.id=$1`
	row := s.queryRow(query, id)
	vol, err := scanVolume(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Volume{}, storage.ErrVolumeNotFound
	} else if err != nil {
		return storage.Volume{}, fmt.Errorf("query failed: %w", err)
	}
	return vol, nil
}

// StoreSector calls fn with an empty location in a writable volume. If
// the sector root already exists, fn is called with the existing
// location and exists is true. Unless exists is true, The sector must
// be written to disk within fn. If fn returns an error, the metadata is
// rolled back. If no space is available, ErrNotEnoughStorage is
// returned. The location is locked until release is called.
//
// The sector should be referenced by either a contract or temp store
// before release is called to prevent Prune() from removing it.
func (s *Store) StoreSector(root types.Hash256, fn func(loc storage.SectorLocation, exists bool) error) (func() error, error) {
	var lockID int64
	var location storage.SectorLocation
	var exists bool
	err := s.transaction(func(tx txn) error {
		var err error
		location, err = sectorLocation(tx, root)
		exists = err == nil
		if errors.Is(err, storage.ErrSectorNotFound) {
			location, err = emptyLocation(tx)
			if err != nil {
				return fmt.Errorf("failed to get empty location: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check existing sector location: %w", err)
		}

		// lock the sector location
		lockID, err = lockLocation(tx, location.ID)
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
		unlockLocation(&dbTxn{s}, lockID)
		return nil, fmt.Errorf("failed to store sector: %w", err)
	} else if exists {
		// if the sector is already stored in a volume, no need to update
		// anything else
		return s.unlockLocationFn(lockID), nil
	}
	// commit the sector location
	err = s.transaction(func(tx txn) error {
		sectorID, err := sectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}
		var updatedID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 RETURNING id`, sectorID, location.ID).Scan(&updatedID)
		if err != nil {
			return fmt.Errorf("failed to commit sector location: %w", err)
		} else if updatedID != location.ID {
			panic("sector location not updated correctly")
		}

		// increment the volume usage
		_, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors+1 WHERE id=$1`, location.Volume)
		if err != nil {
			return fmt.Errorf("failed to update volume usage: %w", err)
		} else if err := incrementNumericStat(tx, metricPhysicalSectors, 1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
	if err != nil {
		unlockLocation(&dbTxn{s}, lockID)
		return nil, err
	}
	return s.unlockLocationFn(lockID), nil
}

// MigrateSectors migrates each occupied sector of a volume starting at
// startIndex. The sector data should be copied to the new location and synced
// to disk during migrateFn. Sectors are migrated in batches of 256.
func (s *Store) MigrateSectors(volumeID int, startIndex uint64, migrateFn func(locations []storage.SectorLocation) error) error {
	const batchSize = 256 // migrate 1GiB at a time
	for {
		// get a batch of sectors to migrate
		var oldLocations, newLocations []storage.SectorLocation
		var locks []int64
		err := s.transaction(func(tx txn) (err error) {
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
			} else if len(newLocations) == 0 {
				// if no new locations were returned, there's no more space
				return storage.ErrNotEnoughStorage
			} else if len(newLocations) < len(oldLocations) {
				// only enough space to partially migrate the batch. truncate
				// the old locations to avoid unnecessary locks
				oldLocations = oldLocations[:len(newLocations)]
			}

			// add the sector root to the new locations
			for i := range newLocations {
				newLocations[i].Root = oldLocations[i].Root
			}

			// lock the old and new locations
			locks, err = lockLocationBatch(tx, append(oldLocations, newLocations...)...)
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
		defer unlockLocationBatch(&dbTxn{s}, locks...)

		// call migrateFn with the new locations, data should be copied to the
		// new locations and synced to disk
		if err := migrateFn(newLocations); err != nil {
			return fmt.Errorf("failed to migrate data: %w", err)
		}

		// update the sector locations in a separate transaction
		err = s.transaction(func(tx txn) error {
			selectStmt, err := tx.Prepare(`SELECT id FROM stored_sectors WHERE sector_root=$1`)
			if err != nil {
				return fmt.Errorf("failed to prepare sector select statement: %w", err)
			}
			defer selectStmt.Close()

			clearStmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_id=null WHERE id=$1 RETURNING volume_id`)
			if err != nil {
				return fmt.Errorf("failed to prepare sector clear statement: %w", err)
			}
			defer clearStmt.Close()

			updateSectorStmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 RETURNING volume_id;`)
			if err != nil {
				return fmt.Errorf("failed to prepare sector update statement: %w", err)
			}
			defer updateSectorStmt.Close()

			updateMetaStmt, err := tx.Prepare(`UPDATE storage_volumes SET used_sectors=used_sectors+$1 WHERE id=$2`)
			if err != nil {
				return fmt.Errorf("failed to prepare sector metadata update statement: %w", err)
			}
			defer updateMetaStmt.Close()

			for i, newLoc := range newLocations {
				oldLoc := oldLocations[i]

				var sectorDBID int64
				if err = selectStmt.QueryRow(sqlHash256(oldLoc.Root)).Scan(&sectorDBID); err != nil {
					return fmt.Errorf("failed to select sector: %w", err)
				}
				var oldVolumeID int64
				if err = clearStmt.QueryRow(oldLoc.ID).Scan(&oldVolumeID); err != nil {
					return fmt.Errorf("failed to clear sector location: %w", err)
				} else if _, err = updateMetaStmt.Exec(-1, oldVolumeID); err != nil {
					return fmt.Errorf("failed to update sector metadata: %w", err)
				}
				var newVolumeID int64
				if err = updateSectorStmt.QueryRow(sectorDBID, newLoc.ID).Scan(&newVolumeID); err != nil {
					return fmt.Errorf("failed to update sector location: %w", err)
				} else if _, err = updateMetaStmt.Exec(1, newVolumeID); err != nil {
					return fmt.Errorf("failed to update sector metadata: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		time.Sleep(time.Millisecond) // allow other transactions to run
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int, err error) {
	const query = `INSERT INTO storage_volumes (disk_path, read_only, used_sectors, total_sectors) VALUES (?, ?, 0, 0) RETURNING id;`
	err = s.queryRow(query, localPath, readOnly).Scan(&volumeID)
	return
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
func (s *Store) RemoveVolume(id int, force bool) error {
	// remove the volume sectors in batches to avoid holding a transaction lock
	// for too long
	for {
		var done bool
		err := s.transaction(func(tx txn) error {
			var dbID int64
			err := tx.QueryRow(`SELECT id FROM volume_sectors WHERE volume_id=$1 AND sector_id IS NOT NULL LIMIT 1;`, id).Scan(&dbID)
			if err == nil && !force {
				return storage.ErrVolumeNotEmpty
			} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to check if volume is empty: %w", err)
			}

			locations, err := volumeSectorsForDeletion(tx, id, pruneBatchSize)
			if err != nil {
				return fmt.Errorf("failed to get volume sectors: %w", err)
			} else if len(locations) == 0 {
				done = true
				return nil // no more sectors to remove
			}

			var forceRemoved int
			locIDs := make([]int64, 0, len(locations))
			for _, loc := range locations {
				locIDs = append(locIDs, loc.ID)
				// if the root is not empty, the sector was not migrated and
				// will be forcefully removed
				if !loc.Empty {
					forceRemoved++
				}
			}

			// reduce the physical sectors metric if there are sectors that
			// failed to migrate.
			if forceRemoved > 0 {
				if err := incrementNumericStat(tx, metricPhysicalSectors, -forceRemoved, time.Now()); err != nil {
					return fmt.Errorf("failed to update force removed sector metric: %w", err)
				}
			}

			// remove the sectors
			deleteQuery := `DELETE FROM volume_sectors WHERE id IN (` + queryPlaceHolders(len(locIDs)) + `)`
			_, err = tx.Exec(deleteQuery, queryArgs(locIDs)...)
			if err != nil {
				return fmt.Errorf("failed to remove volume sectors: %w", err)
			}

			const updateMetaQuery = `UPDATE storage_volumes SET total_sectors=total_sectors-$1 WHERE id=$2`
			_, err = tx.Exec(updateMetaQuery, len(locIDs), id)
			if err != nil {
				return fmt.Errorf("failed to update volume metadata: %w", err)
			} else if err := incrementNumericStat(tx, metricTotalSectors, -len(locIDs), time.Now()); err != nil {
				return fmt.Errorf("failed to update total sector metric: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to remove volume: %w", err)
		} else if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	_, err := s.exec(`DELETE FROM storage_volumes WHERE id=?`, id)
	return err
}

// GrowVolume grows a storage volume's metadata by n sectors.
func (s *Store) GrowVolume(id int, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}
	return s.transaction(func(tx txn) error {
		var nextIndex uint64
		err := tx.QueryRow(`SELECT total_sectors FROM storage_volumes WHERE id=?;`, id).Scan(&nextIndex)
		if err != nil {
			return fmt.Errorf("failed to get last volume index: %w", err)
		}

		if nextIndex >= maxSectors {
			panic(fmt.Errorf("nextIndex must be less than maxSectors: %v < %v", nextIndex, maxSectors)) // dev error
		}

		insertStmt, err := tx.Prepare(`INSERT INTO volume_sectors (volume_id, volume_index) VALUES ($1, $2);`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer insertStmt.Close()

		for i := nextIndex; i < maxSectors; i++ {
			if _, err = insertStmt.Exec(id, i); err != nil {
				return fmt.Errorf("failed to grow volume: %w", err)
			}
		}

		if _, err = tx.Exec(`UPDATE storage_volumes SET total_sectors=$1 WHERE id=$2`, maxSectors, id); err != nil {
			return fmt.Errorf("failed to update volume metadata: %w", err)
		} else if err := incrementNumericStat(tx, metricTotalSectors, int(maxSectors-nextIndex), time.Now()); err != nil {
			return fmt.Errorf("failed to update total sectors metric: %w", err)
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
	return s.transaction(func(tx txn) error {
		// check if there are any used sectors in the shrink range
		var usedSectors uint64
		err := tx.QueryRow(`SELECT COUNT(sector_id) FROM volume_sectors WHERE volume_id=$1 AND volume_index >= $2 AND sector_id IS NOT NULL;`, id, maxSectors).Scan(&usedSectors)
		if err != nil {
			return fmt.Errorf("failed to get used sectors: %w", err)
		} else if usedSectors != 0 {
			return fmt.Errorf("cannot shrink volume to %d sectors, %d sectors are in use: %w", maxSectors, usedSectors, storage.ErrVolumeNotEmpty)
		}

		// get the current volume size
		var totalSectors uint64
		err = tx.QueryRow(`SELECT total_sectors FROM storage_volumes WHERE id=$1;`, id).Scan(&totalSectors)
		if err != nil {
			return fmt.Errorf("failed to get volume size: %w", err)
		} else if maxSectors > totalSectors {
			panic(fmt.Errorf("maxSectors must be less than totalSectors: %v < %v", maxSectors, totalSectors))
		}
		// delete the empty sectors
		_, err = tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=$1 AND volume_index >= $2;`, id, maxSectors)
		if err != nil {
			return fmt.Errorf("failed to shrink volume: %w", err)
		}
		// update the volume metadata
		_, err = tx.Exec(`UPDATE storage_volumes SET total_sectors=$1 WHERE id=$2`, maxSectors, id)
		if err != nil {
			return fmt.Errorf("failed to update volume metadata: %w", err)
		} else if err := incrementNumericStat(tx, metricTotalSectors, -int(totalSectors-maxSectors), time.Now()); err != nil {
			return fmt.Errorf("failed to update total sectors metric: %w", err)
		}
		return nil
	})
}

// SetReadOnly sets the read-only flag on a volume.
func (s *Store) SetReadOnly(volumeID int, readOnly bool) error {
	const query = `UPDATE storage_volumes SET read_only=$1 WHERE id=$2;`
	_, err := s.exec(query, readOnly, volumeID)
	return err
}

// SetAvailable sets the available flag on a volume.
func (s *Store) SetAvailable(volumeID int, available bool) error {
	const query = `UPDATE storage_volumes SET available=$1 WHERE id=$2;`
	_, err := s.exec(query, available, volumeID)
	return err
}

// sectorsForMigration returns a list of sectors that should be migrated from
// the given volume. The sectors are ordered by volume index, and the returned
// list will contain at most batchSize sectors. The startIndex is the volume
// index of the first sector to return.
func sectorsForMigration(tx txn, volumeID int, startIndex uint64, batchSize int64) (sectors []storage.SectorLocation, _ error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index, s.sector_root
	FROM volume_sectors vs
	INNER JOIN stored_sectors s ON (s.id=vs.sector_id)
	WHERE vs.volume_id=$1 AND vs.volume_index>=$2
	ORDER BY vs.volume_index ASC LIMIT $3`

	rows, err := tx.Query(query, volumeID, startIndex, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query sectors: %w", err)
	}
	defer rows.Close()

	// scan the old locations
	for rows.Next() {
		var loc storage.SectorLocation
		if err := rows.Scan(&loc.ID, &loc.Volume, &loc.Index, (*sqlHash256)(&loc.Root)); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector: %w", err)
		}
		sectors = append(sectors, loc)
	}
	return sectors, nil
}

func sectorDBID(tx txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO stored_sectors (sector_root, last_access_timestamp) VALUES ($1, $2) ON CONFLICT (sector_root) DO UPDATE SET last_access_timestamp=EXCLUDED.last_access_timestamp RETURNING id`, sqlHash256(root), sqlTime(time.Now())).Scan(&id)
	return
}

func sectorLocation(tx txn, root types.Hash256) (loc storage.SectorLocation, err error) {
	var sectorDBID int64
	err = tx.QueryRow(`UPDATE stored_sectors SET last_access_timestamp=$1 WHERE sector_root=$2 RETURNING id`, sqlTime(time.Now()), sqlHash256(root)).Scan(&sectorDBID)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrSectorNotFound
	} else if err != nil {
		return storage.SectorLocation{}, fmt.Errorf("failed to update sector access time: %w", err)
	}
	loc.Root = root
	const query = `SELECT v.id, v.volume_id, v.volume_index FROM volume_sectors v WHERE v.sector_id=$1`
	err = tx.QueryRow(query, sectorDBID).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		// should only happen if the sector was manually removed
		return storage.SectorLocation{}, storage.ErrSectorNotFound
	}
	return
}

func emptyLocation(tx txn) (loc storage.SectorLocation, err error) {
	// SQLite can only use one index per table and always prefers WHERE clause
	// indices over ORDER BY clause indices. However, since the query needs to
	// order potentially millions of rows, an index on the ORDER BY clause is
	// required. With the existing indices and query, sqlite will use an index
	// for the ORDER BY clause. Any changes may cause sqlite to switch the ORDER
	// BY clause to a temp b-tree index, which is significantly slower (200ms vs
	// 2ms).
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index
FROM volume_sectors vs
INNER JOIN storage_volumes v ON +(vs.volume_id = v.id)
WHERE +vs.sector_id IS NULL AND vs.id NOT IN (SELECT volume_sector_id FROM locked_volume_sectors) AND v.read_only=false AND v.available = true
ORDER BY vs.volume_index ASC -- order by distributes data evenly across volumes rather than filling the first volume.
LIMIT 1;`
	err = tx.QueryRow(query).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrNotEnoughStorage
	}
	return
}

func volumeSectorsForDeletion(tx txn, volumeID, batchSize int) (locs []volumeSectorRef, err error) {
	const query = `SELECT id, sector_id IS NULL AS empty FROM volume_sectors WHERE volume_id=$1 LIMIT $2`
	rows, err := tx.Query(query, volumeID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query volume sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref volumeSectorRef
		if err := rows.Scan(&ref.ID, &ref.Empty); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector: %w", err)
		}
		locs = append(locs, ref)
	}
	return
}

// locationsForMigration returns a list of locations to migrate to. Locations
// are guaranteed to be empty and in a writable volume. As a special case,
// sectors may be migrated to empty indices less than startIndex within the
// given volume even if the volue is read-only. The locations are ordered by
// volume index and the returned list will contain at most batchSize locations.
func locationsForMigration(tx txn, volumeID int, startIndex uint64, batchSize int) (locations []storage.SectorLocation, _ error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index
	FROM volume_sectors vs
	INNER JOIN storage_volumes v ON (vs.volume_id=v.id)
	WHERE vs.sector_id IS NULL AND vs.id NOT IN (SELECT volume_sector_id FROM locked_volume_sectors) 
	AND v.available=true AND ((v.read_only=false AND vs.volume_id <> $1) OR (vs.volume_id=$1 AND vs.volume_index<$2))
	LIMIT $3;`

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

func scanVolume(s scanner) (volume storage.Volume, err error) {
	err = s.Scan(&volume.ID, &volume.LocalPath, &volume.ReadOnly, &volume.Available, &volume.TotalSectors, &volume.UsedSectors)
	return
}
