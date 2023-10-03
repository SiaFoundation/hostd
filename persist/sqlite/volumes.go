package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap"
)

type volumeSectorRef struct {
	ID    int64
	Empty bool
}

var errNoSectorsToMigrate = errors.New("no sectors to migrate")

func (s *Store) migrateSector(volumeID int64, startIndex uint64, migrateFn func(location storage.SectorLocation) error, log *zap.Logger) error {
	var locks []int64
	var oldLoc, newLoc storage.SectorLocation
	err := s.transaction(func(tx txn) (err error) {
		oldLoc, err = sectorForMigration(tx, volumeID, startIndex)
		if err != nil {
			return fmt.Errorf("failed to get sector for migration: %w", err)
		}

		newLoc, err = emptyLocationForMigration(tx, volumeID)
		if errors.Is(err, storage.ErrNotEnoughStorage) && startIndex > 0 {
			// if there is no space in other volumes, try to migrate within the
			// same volume
			newLoc, err = locationWithinVolume(tx, volumeID, startIndex)
			if err != nil {
				return fmt.Errorf("failed to get empty location in volume: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get empty location: %w", err)
		}

		newLoc.Root = oldLoc.Root

		// lock the old and new locations
		locks, err = lockLocations(tx, []storage.SectorLocation{oldLoc, newLoc})
		if err != nil {
			return fmt.Errorf("failed to lock sectors: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to migrate sector: %w", err)
	}
	// unlock the locations
	defer unlockLocations(&dbTxn{s}, locks)

	// call the migrateFn with the new location, data should be copied to the
	// new location and synced to disk
	if err := migrateFn(newLoc); err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	// update the sector location in a separate transaction
	err = s.transaction(func(tx txn) error {
		// get the sector ID
		var sectorID int64
		err := tx.QueryRow(`SELECT sector_id FROM volume_sectors WHERE id=$1`, oldLoc.ID).Scan(&sectorID)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// clear the old sector
		var oldVolumeID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=null WHERE id=$1 AND sector_id=$2 RETURNING volume_id`, oldLoc.ID, sectorID).Scan(&oldVolumeID)
		if err != nil {
			return fmt.Errorf("failed to clear sector location: %w", err)
		}

		// update the old volume metadata
		if _, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1`, oldVolumeID); err != nil {
			return fmt.Errorf("failed to update old volume metadata: %w", err)
		}

		// add the sector to the new location
		var newVolumeID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 RETURNING volume_id`, sectorID, newLoc.ID).Scan(&newVolumeID)
		if err != nil {
			return fmt.Errorf("failed to update sector location: %w", err)
		}

		// update the new volume metadata
		if _, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors+1 WHERE id=$1`, newVolumeID); err != nil {
			return fmt.Errorf("failed to update new volume metadata: %w", err)
		}
		return nil
	})
	log.Debug("migrated sector", zap.Uint64("oldIndex", oldLoc.Index), zap.Stringer("root", newLoc.Root), zap.Int64("newVolume", newLoc.Volume), zap.Uint64("newIndex", newLoc.Index))
	return err
}

func (s *Store) batchRemoveVolume(id int64, force bool) (bool, error) {
	var done bool
	err := s.transaction(func(tx txn) error {
		var dbID int64
		err := tx.QueryRow(`SELECT id FROM volume_sectors WHERE volume_id=$1 AND sector_id IS NOT NULL LIMIT 1;`, id).Scan(&dbID)
		if err == nil && !force {
			return storage.ErrVolumeNotEmpty
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to check if volume is empty: %w", err)
		}

		locations, err := volumeSectorsForDeletion(tx, id, sqlSectorBatchSize)
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
	return done, err
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
func (s *Store) Volume(id int64) (storage.Volume, error) {
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
// before release is called to prevent it from being pruned
func (s *Store) StoreSector(root types.Hash256, fn func(loc storage.SectorLocation, exists bool) error) (func() error, error) {
	var sectorLockID int64
	var locationLocks []int64
	var location storage.SectorLocation
	var exists bool

	err := s.transaction(func(tx txn) error {
		sectorID, err := sectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// lock the sector
		sectorLockID, err = lockSector(tx, sectorID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}

		// check if the sector already exists
		location, err = sectorLocation(tx, sectorID)
		exists = err == nil
		if errors.Is(err, storage.ErrSectorNotFound) {
			location, err = emptyLocation(tx)
			if err != nil {
				return fmt.Errorf("failed to get empty location: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check existing sector location: %w", err)
		}

		// lock the location
		locationLocks, err = lockLocations(tx, []storage.SectorLocation{location})
		if err != nil {
			return fmt.Errorf("failed to lock sector location: %w", err)
		}

		// if the sector already exists, return the existing location
		if exists {
			return nil
		}
		res, err := tx.Exec(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2`, sectorID, location.ID)
		if err != nil {
			return fmt.Errorf("failed to commit sector location: %w", err)
		} else if rows, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to check rows affected: %w", err)
		} else if rows == 0 {
			return storage.ErrSectorNotFound
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
		return nil, err
	}

	unlock := func() error {
		return s.transaction(func(tx txn) error {
			if err := unlockLocations(tx, locationLocks); err != nil {
				return fmt.Errorf("failed to unlock sector location: %w", err)
			} else if err := unlockSector(tx, sectorLockID); err != nil {
				return fmt.Errorf("failed to unlock sector: %w", err)
			}
			return nil
		})
	}

	// call fn with the location
	if err := fn(location, exists); err != nil {
		unlock()
		return nil, fmt.Errorf("failed to store sector: %w", err)
	}
	return unlock, nil
}

// MigrateSectors migrates each occupied sector of a volume starting at
// startIndex. The sector data should be copied to the new location and synced
// to disk during migrateFn.
func (s *Store) MigrateSectors(volumeID int64, startIndex uint64, migrateFn func(location storage.SectorLocation) error) error {
	log := s.log.Named("migrate").With(zap.Int64("oldVolume", volumeID), zap.Uint64("startIndex", startIndex))
	for i := 0; ; i++ {
		if err := s.migrateSector(volumeID, startIndex, migrateFn, log); err != nil {
			if errors.Is(err, errNoSectorsToMigrate) {
				return nil
			}
			return fmt.Errorf("failed to migrate sector: %w", err)
		}
		if i%64 == 0 {
			jitterSleep(time.Millisecond) // allow other transactions to run
		}
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int64, err error) {
	const query = `INSERT INTO storage_volumes (disk_path, read_only, used_sectors, total_sectors) VALUES (?, ?, 0, 0) RETURNING id;`
	err = s.queryRow(query, localPath, readOnly).Scan(&volumeID)
	return
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
func (s *Store) RemoveVolume(id int64, force bool) error {
	// remove the volume sectors in batches to avoid holding a transaction lock
	// for too long
	for {
		done, err := s.batchRemoveVolume(id, force)
		if err != nil {
			return err
		} else if done {
			break
		}
		jitterSleep(time.Millisecond)
	}
	if _, err := s.exec(`DELETE FROM storage_volumes WHERE id=?`, id); err != nil {
		return fmt.Errorf("failed to remove volume: %w", err)
	}
	return nil
}

// GrowVolume grows a storage volume's metadata by n sectors.
func (s *Store) GrowVolume(id int64, maxSectors uint64) error {
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
func (s *Store) ShrinkVolume(id int64, maxSectors uint64) error {
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
func (s *Store) SetReadOnly(volumeID int64, readOnly bool) error {
	const query = `UPDATE storage_volumes SET read_only=$1 WHERE id=$2;`
	_, err := s.exec(query, readOnly, volumeID)
	return err
}

// SetAvailable sets the available flag on a volume.
func (s *Store) SetAvailable(volumeID int64, available bool) error {
	const query = `UPDATE storage_volumes SET available=$1 WHERE id=$2;`
	_, err := s.exec(query, available, volumeID)
	return err
}

// sectorDBID returns the database ID of a sector.
func sectorDBID(tx txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO stored_sectors (sector_root, last_access_timestamp) VALUES ($1, $2) ON CONFLICT (sector_root) DO UPDATE SET last_access_timestamp=EXCLUDED.last_access_timestamp RETURNING id`, sqlHash256(root), sqlTime(time.Now())).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrSectorNotFound
	}
	return
}

// sectorLocation returns the location of a sector.
func sectorLocation(tx txn, sectorID int64) (loc storage.SectorLocation, err error) {
	const query = `SELECT v.id, v.volume_id, v.volume_index, s.sector_root
FROM volume_sectors v 
INNER JOIN stored_sectors s ON (s.id=v.sector_id)
WHERE v.sector_id=$1`
	err = tx.QueryRow(query, sectorID).Scan(&loc.ID, &loc.Volume, &loc.Index, (*sqlHash256)(&loc.Root))
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrSectorNotFound
	}
	return
}

// emptyLocationInVolume returns an empty location in the given volume. If
// there is no space available, ErrNotEnoughStorage is returned.
func emptyLocationInVolume(tx txn, volumeID int64) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index FROM volume_sectors vs
LEFT JOIN locked_volume_sectors lvs ON (lvs.volume_sector_id=vs.id)
WHERE vs.sector_id IS NULL AND lvs.volume_sector_id IS NULL AND vs.volume_id=$1 LIMIT 1;`
	err = tx.QueryRow(query, volumeID).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrNotEnoughStorage
	}
	return
}

// emptyLocation returns an empty location in a writable volume. If there is no
// space available, ErrNotEnoughStorage is returned.
func emptyLocation(tx txn) (storage.SectorLocation, error) {
	var volumeID int64
	err := tx.QueryRow(`SELECT id FROM storage_volumes WHERE available=true AND read_only=false AND total_sectors-used_sectors > 0 ORDER BY used_sectors ASC LIMIT 1;`).Scan(&volumeID)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrNotEnoughStorage
	} else if err != nil {
		return storage.SectorLocation{}, fmt.Errorf("failed to get empty location: %w", err)
	}

	// note: there is a slight race here where all sectors in a volume could be
	// locked, but not committed. This is unlikely to happen in practice, and
	// the worst case is that the host fails to store a sector. The performance
	// benefits of choosing a volume first far outweigh the downsides.
	return emptyLocationInVolume(tx, volumeID)
}

// emptyLocationForMigration returns an empty location in a writable volume
// other than the given volumeID. If there is no space available,
// ErrNotEnoughStorage is returned.
func emptyLocationForMigration(tx txn, oldVolumeID int64) (loc storage.SectorLocation, err error) {
	const query = `SELECT id FROM storage_volumes
WHERE available=true AND read_only=false AND total_sectors-used_sectors > 0 AND id<>$1
ORDER BY used_sectors ASC LIMIT 1;`
	var newVolumeID int64
	err = tx.QueryRow(query, oldVolumeID).Scan(&newVolumeID)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrNotEnoughStorage
	} else if err != nil {
		return storage.SectorLocation{}, fmt.Errorf("failed to get empty location: %w", err)
	}

	// note: there is a slight race here where all sectors in a volume could be
	// locked, but not committed. This is unlikely to happen in practice, and
	// the worst case is that the host fails to store a sector. The performance
	// benefits of choosing a volume first far outweigh the downsides.
	return emptyLocationInVolume(tx, newVolumeID)
}

// sectorForMigration returns the location of the first occupied sector in the
// volume starting at minIndex. If there are no sectors to migrate,
// errNoSectorsToMigrate is returned.
func sectorForMigration(tx txn, volumeID int64, minIndex uint64) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index, s.sector_root
	FROM volume_sectors vs
	INNER JOIN stored_sectors s ON (s.id=vs.sector_id)
	WHERE vs.sector_id IS NOT NULL AND vs.volume_id=$1 AND vs.volume_index >= $2`

	err = tx.QueryRow(query, volumeID, minIndex).Scan(&loc.ID, &loc.Volume, &loc.Index, (*sqlHash256)(&loc.Root))
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, errNoSectorsToMigrate
	}
	return
}

// locationWithinVolume returns an empty location within the same volume as
// the given volumeID. If there is no space in the volume, ErrNotEnoughStorage
// is returned.
func locationWithinVolume(tx txn, volumeID int64, maxIndex uint64) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index
	FROM volume_sectors vs
	WHERE vs.sector_id IS NULL AND vs.id NOT IN (SELECT volume_sector_id FROM locked_volume_sectors) 
	AND vs.volume_id=$1 AND vs.volume_index<$2
	LIMIT 1;`

	err = tx.QueryRow(query, volumeID, maxIndex).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrNotEnoughStorage
	}
	return
}

func volumeSectorsForDeletion(tx txn, volumeID int64, batchSize int) (locs []volumeSectorRef, err error) {
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

func scanVolume(s scanner) (volume storage.Volume, err error) {
	err = s.Scan(&volume.ID, &volume.LocalPath, &volume.ReadOnly, &volume.Available, &volume.TotalSectors, &volume.UsedSectors)
	return
}
