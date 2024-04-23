package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap"
)

func (s *Store) migrateSector(volumeID int64, minIndex uint64, marker int64, migrateFn storage.MigrateFunc, log *zap.Logger) (int64, bool, error) {
	start := time.Now()

	var locationLocks []int64
	var sectorLock int64
	var oldLoc, newLoc storage.SectorLocation
	err := s.transaction(func(tx txn) (err error) {
		oldLoc, err = sectorForMigration(tx, volumeID, marker)
		if errors.Is(err, sql.ErrNoRows) {
			marker = math.MaxInt64
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get sector for migration: %w", err)
		}
		marker = int64(oldLoc.Index)

		sectorDBID, err := sectorDBID(tx, oldLoc.Root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		sectorLock, err = lockSector(tx, sectorDBID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}

		newLoc, err = emptyLocationForMigration(tx, volumeID)
		if errors.Is(err, storage.ErrNotEnoughStorage) && minIndex > 0 {
			// if there is no space in other volumes, try to migrate within the
			// same volume
			newLoc, err = locationWithinVolume(tx, volumeID, uint64(minIndex))
			if err != nil {
				return fmt.Errorf("failed to get empty location in volume: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get empty location: %w", err)
		}

		newLoc.Root = oldLoc.Root

		// lock the old and new locations
		locationLocks, err = lockLocations(tx, []storage.SectorLocation{oldLoc, newLoc})
		if err != nil {
			return fmt.Errorf("failed to lock sectors: %w", err)
		}
		return nil
	})
	if errors.Is(err, storage.ErrNotEnoughStorage) {
		return marker, false, nil
	} else if err != nil {
		return 0, false, fmt.Errorf("failed to get new location: %w", err)
	} else if marker == math.MaxInt64 {
		return marker, false, nil
	}
	// unlock the locations
	defer unlockLocations(&dbTxn{s}, locationLocks)
	defer unlockSector(&dbTxn{s}, log.Named("unlockSector"), sectorLock)

	// call the migrateFn with the new location, data should be copied to the
	// new location and synced to disk
	if err := migrateFn(newLoc); err != nil {
		log.Error("failed to migrate sector data", zap.Error(err))
		return marker, false, nil
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
		if err := incrementVolumeUsage(tx, oldVolumeID, -1); err != nil {
			return fmt.Errorf("failed to update old volume metadata: %w", err)
		}

		// add the sector to the new location
		var newVolumeID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 RETURNING volume_id`, sectorID, newLoc.ID).Scan(&newVolumeID)
		if err != nil {
			return fmt.Errorf("failed to update sector location: %w", err)
		}

		// update the new volume metadata
		if err := incrementVolumeUsage(tx, newVolumeID, 1); err != nil {
			return fmt.Errorf("failed to update new volume metadata: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, false, fmt.Errorf("failed to update sector metadata: %w", err)
	}
	log.Debug("migrated sector", zap.Uint64("oldIndex", oldLoc.Index), zap.Stringer("root", newLoc.Root), zap.Int64("newVolume", newLoc.Volume), zap.Uint64("newIndex", newLoc.Index), zap.Duration("elapsed", time.Since(start)))
	return marker, true, nil
}

func forceDeleteVolumeSectors(tx txn, volumeID int64) (removed, lost int64, err error) {
	const query = `DELETE FROM volume_sectors WHERE id IN (SELECT id FROM volume_sectors WHERE volume_id=$1 LIMIT $2) RETURNING sector_id IS NULL AS empty`

	rows, err := tx.Query(query, volumeID, sqlSectorBatchSize)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to remove volume sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var empty bool
		if err := rows.Scan(&empty); err != nil {
			return 0, 0, fmt.Errorf("failed to scan volume sector: %w", err)
		}

		removed++
		if !empty {
			lost++
		}
	}
	err = rows.Err()
	return
}

func deleteVolumeSectors(tx txn, volumeID int64) (removed int64, err error) {
	// check that the volume is empty
	var dummyID int64
	err = tx.QueryRow(`SELECT id FROM volume_sectors WHERE volume_id=$1 AND sector_id IS NOT NULL LIMIT 1`, volumeID).Scan(&dummyID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to check volume sectors: %w", err)
	} else if err == nil {
		return 0, storage.ErrVolumeNotEmpty
	}

	const query = `DELETE FROM volume_sectors WHERE id IN (SELECT id FROM volume_sectors WHERE volume_id=$1 AND sector_id IS NULL LIMIT $2)`
	res, err := tx.Exec(query, volumeID, sqlSectorBatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to remove volume sectors: %w", err)
	}
	removed, err = res.RowsAffected()
	return
}

func (s *Store) batchRemoveVolumeSectors(id int64, force bool) (removed, lost int64, err error) {
	err = s.transaction(func(tx txn) error {
		if force {
			removed, lost, err = forceDeleteVolumeSectors(tx, id)
			if err != nil {
				return fmt.Errorf("failed to remove volume sectors: %w", err)
			}

			if lost > 0 {
				// special case: if the volume sectors are force deleted, any
				// unmigrated sectors  be deducted from the physical sector
				// count.
				if err := incrementNumericStat(tx, metricPhysicalSectors, -int(lost), time.Now()); err != nil {
					return fmt.Errorf("failed to update physical sector metric: %w", err)
				} else if err := incrementNumericStat(tx, metricLostSectors, int(lost), time.Now()); err != nil {
					return fmt.Errorf("failed to update lost sector metric: %w", err)
				}
			}
		} else {
			removed, err = deleteVolumeSectors(tx, id)
			if err != nil {
				return fmt.Errorf("failed to remove volume sectors: %w", err)
			}
		}

		const updateMetaQuery = `UPDATE storage_volumes SET total_sectors=total_sectors-$1 WHERE id=$2`
		_, err = tx.Exec(updateMetaQuery, removed, id)
		if err != nil {
			return fmt.Errorf("failed to update volume metadata: %w", err)
		} else if err := incrementNumericStat(tx, metricTotalSectors, -int(removed), time.Now()); err != nil {
			return fmt.Errorf("failed to update total sector metric: %w", err)
		}
		return nil
	})
	if lost > 0 && !force {
		panic("lost sectors without force delete") // dev error
	}
	return
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

	log := s.log.Named("StoreSector").With(zap.Stringer("root", root))
	err := s.transaction(func(tx txn) error {
		sectorID, err := insertSectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// lock the sector
		sectorLockID, err = lockSector(tx, sectorID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}

		// check if the sector is already stored on disk
		location, err = sectorLocation(tx, sectorID, root)
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
		if err := incrementVolumeUsage(tx, location.Volume, 1); err != nil {
			return fmt.Errorf("failed to update volume metadata: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	log = log.With(zap.Int64("volume", location.Volume), zap.Uint64("index", location.Index))
	log.Debug("stored sector")
	unlock := func() error {
		return s.transaction(func(tx txn) error {
			if err := unlockLocations(tx, locationLocks); err != nil {
				return fmt.Errorf("failed to unlock sector location: %w", err)
			} else if err := unlockSector(tx, log.Named("unlock"), sectorLockID); err != nil {
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
// startIndex. migrateFn will be called for each sector that needs to be migrated.
// The sector data should be copied to the new location and synced
// to disk immediately. If migrateFn returns an error, that sector will be
// considered failed and the migration will continue. If the context is
// canceled, the migration will stop and the error will be returned. The
// number of sectors migrated and failed will always be returned, even if an
// error occurs.
func (s *Store) MigrateSectors(ctx context.Context, volumeID int64, startIndex uint64, migrateFn storage.MigrateFunc) (migrated, failed int, err error) {
	log := s.log.Named("migrate").With(zap.Int64("oldVolume", volumeID), zap.Uint64("startIndex", startIndex))
	// the migration function is called in a loop until all sectors are migrated
	// marker is used to skip sectors that tried to migrate but failed.
	// when removing a volume, marker is -1 to also migrate the first sector
	marker := int64(startIndex) - 1
	for i := 0; ; i++ {
		if ctx.Err() != nil {
			err = ctx.Err()
			return
		}

		var successful bool
		marker, successful, err = s.migrateSector(volumeID, startIndex, marker, migrateFn, log)
		if err != nil {
			err = fmt.Errorf("failed to migrate sector: %w", err)
			return
		} else if marker == math.MaxInt64 {
			return
		}

		if successful {
			migrated++
		} else {
			failed++
		}

		if i%256 == 0 {
			jitterSleep(time.Millisecond) // allow other transactions to run
		}
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int64, err error) {
	return addVolume(&dbTxn{s}, localPath, readOnly)
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
func (s *Store) RemoveVolume(id int64, force bool) error {
	log := s.log.Named("RemoveVolume").With(zap.Int64("volume", id), zap.Bool("force", force))
	// remove the volume sectors in batches to avoid holding a transaction lock
	// for too long
	for i := 0; ; i++ {
		removed, lost, err := s.batchRemoveVolumeSectors(id, force)
		log.Debug("removed volume sectors", zap.Int("batch", i), zap.Int64("removed", removed), zap.Int64("lost", lost), zap.Error(err))
		if err != nil {
			return err
		} else if removed == 0 {
			break
		}
		jitterSleep(time.Millisecond)
	}

	return s.transaction(func(tx txn) error {
		// check that the volume exists
		var volumeID int64
		err := tx.QueryRow(`SELECT id FROM storage_volumes WHERE id=$1`, id).Scan(&volumeID)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrVolumeNotFound
		} else if err != nil {
			return fmt.Errorf("failed to check volume: %w", err)
		}

		// check that the volume is empty
		var volumeSectorID int64
		err = tx.QueryRow(`SELECT id FROM volume_sectors WHERE volume_id=$1 LIMIT 1`, id).Scan(&volumeSectorID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to check volume sectors: %w", err)
		} else if err == nil {
			return storage.ErrVolumeNotEmpty
		}

		// delete the volume
		_, err = tx.Exec(`DELETE FROM storage_volumes WHERE id=$1`, id)
		return err
	})
}

// GrowVolume grows a storage volume's metadata by n sectors.
func (s *Store) GrowVolume(id int64, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}

	return s.transaction(func(tx txn) error {
		return growVolume(tx, id, maxSectors)
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

// sectorDBID returns the ID of a sector root in the stored_sectors table.
func sectorDBID(tx txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM stored_sectors WHERE sector_root=$1`, sqlHash256(root)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrSectorNotFound
	}
	return
}

// insertSectorDBID inserts a sector root into the stored_sectors table if it
// does not already exist. If the sector root already exists, the ID is
// returned.
func insertSectorDBID(tx txn, root types.Hash256) (id int64, err error) {
	id, err = sectorDBID(tx, root)
	if errors.Is(err, storage.ErrSectorNotFound) {
		// insert the sector root
		err = tx.QueryRow(`INSERT INTO stored_sectors (sector_root, last_access_timestamp) VALUES ($1, $2) RETURNING id`, sqlHash256(root), sqlTime(time.Now())).Scan(&id)
		return
	}
	return
}

func addVolume(tx txn, localPath string, readOnly bool) (volumeID int64, err error) {
	const query = `INSERT INTO storage_volumes (disk_path, read_only, used_sectors, total_sectors) VALUES (?, ?, 0, 0) RETURNING id;`
	err = tx.QueryRow(query, localPath, readOnly).Scan(&volumeID)
	return
}

func growVolume(tx txn, id int64, maxSectors uint64) error {
	var nextIndex uint64
	err := tx.QueryRow(`SELECT total_sectors FROM storage_volumes WHERE id=?;`, id).Scan(&nextIndex)
	if err != nil {
		return fmt.Errorf("failed to get last volume index: %w", err)
	}

	if nextIndex >= maxSectors {
		return nil // volume is already large enough
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
}

// sectorLocation returns the location of a sector.
func sectorLocation(tx txn, sectorID int64, root types.Hash256) (loc storage.SectorLocation, err error) {
	const query = `SELECT v.id, v.volume_id, v.volume_index
FROM volume_sectors v
WHERE v.sector_id=$1`
	err = tx.QueryRow(query, sectorID).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.SectorLocation{}, storage.ErrSectorNotFound
	}
	// note: this is roundabout, but it saves an extra join since all calls to
	// sectorLocation are preceded by a call to sectorDBID
	loc.Root = root
	return
}

// emptyLocation returns an empty location in a writable volume. If there is no
// space available, ErrNotEnoughStorage is returned.
func emptyLocation(tx txn) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index 
	FROM volume_sectors vs INDEXED BY volume_sectors_sector_writes_volume_id_sector_id_volume_index_compound
	LEFT JOIN locked_volume_sectors lvs ON (lvs.volume_sector_id=vs.id)
	INNER JOIN storage_volumes sv ON (sv.id=vs.volume_id)
	WHERE vs.sector_id IS NULL AND lvs.volume_sector_id IS NULL AND sv.available=true AND sv.read_only=false
	ORDER BY vs.sector_writes ASC
	LIMIT 1;`
	err = tx.QueryRow(query).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrNotEnoughStorage
		return
	} else if err != nil {
		return
	}
	_, err = tx.Exec(`UPDATE volume_sectors SET sector_writes=sector_writes+1 WHERE id=$1`, loc.ID)
	return
}

// emptyLocationForMigration returns an empty location in a writable volume. If there is no
// space available, ErrNotEnoughStorage is returned.
func emptyLocationForMigration(tx txn, volumeID int64) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index 
	FROM volume_sectors vs INDEXED BY volume_sectors_sector_writes_volume_id_sector_id_volume_index_compound
	LEFT JOIN locked_volume_sectors lvs ON (lvs.volume_sector_id=vs.id)
	INNER JOIN storage_volumes sv ON (sv.id=vs.volume_id)
	WHERE vs.sector_id IS NULL AND lvs.volume_sector_id IS NULL AND sv.available=true AND sv.read_only=false AND vs.volume_id <> $1
	ORDER BY vs.sector_writes ASC
	LIMIT 1;`
	err = tx.QueryRow(query, volumeID).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrNotEnoughStorage
		return
	} else if err != nil {
		return
	}
	_, err = tx.Exec(`UPDATE volume_sectors SET sector_writes=sector_writes+1 WHERE id=$1`, loc.ID)
	return
}

// sectorForMigration returns the location of the first occupied sector in the
// volume starting at minIndex and greater than marker.
func sectorForMigration(tx txn, volumeID int64, marker int64) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index, s.sector_root
	FROM volume_sectors vs
	INNER JOIN stored_sectors s ON (s.id=vs.sector_id)
	WHERE vs.sector_id IS NOT NULL AND vs.volume_id=$1 AND vs.volume_index > $2
	ORDER BY vs.volume_index ASC
	LIMIT 1`

	err = tx.QueryRow(query, volumeID, marker).Scan(&loc.ID, &loc.Volume, &loc.Index, (*sqlHash256)(&loc.Root))
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

func scanVolume(s scanner) (volume storage.Volume, err error) {
	err = s.Scan(&volume.ID, &volume.LocalPath, &volume.ReadOnly, &volume.Available, &volume.TotalSectors, &volume.UsedSectors)
	return
}
