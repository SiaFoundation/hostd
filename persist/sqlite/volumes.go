package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/storage"
	"go.uber.org/zap"
)

func forceDeleteVolumeSectors(tx *txn, volumeID int64) (removed, lost int64, err error) {
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

func deleteVolumeSectors(tx *txn, volumeID int64) (removed int64, err error) {
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
	err = s.transaction(func(tx *txn) error {
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
	const query = `SELECT COALESCE(SUM(total_sectors), 0) AS total_sectors, COALESCE(SUM(used_sectors), 0) AS used_sectors FROM storage_volumes`
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(query).Scan(&totalSectors, &usedSectors)
	})
	return
}

// Volumes returns a list of all volumes.
func (s *Store) Volumes() (volumes []storage.Volume, err error) {
	const query = `SELECT v.id, v.disk_path, v.read_only, v.available, v.total_sectors, v.used_sectors
FROM storage_volumes v
ORDER BY v.id ASC`

	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(query)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			volume, err := scanVolume(rows)
			if err != nil {
				return fmt.Errorf("failed to scan volume: %w", err)
			}
			volumes = append(volumes, volume)
		}
		return rows.Err()
	})
	return
}

// Volume returns a volume by its ID.
func (s *Store) Volume(id int64) (vol storage.Volume, err error) {
	const query = `SELECT v.id, v.disk_path, v.read_only, v.available, v.total_sectors, v.used_sectors
FROM storage_volumes v
WHERE v.id=$1`

	err = s.transaction(func(tx *txn) error {
		vol, err = scanVolume(tx.QueryRow(query, id))
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Volume{}, storage.ErrVolumeNotFound
	}
	return
}

// StoreSector calls fn with an empty location in a writable volume. If
// the sector root already exists, nil is returned. The sector should be
// written to disk within fn. If fn returns an error, the metadata is
// rolled back and the error is returned. If no space is available,
// ErrNotEnoughStorage is returned.
func (s *Store) StoreSector(root types.Hash256, fn storage.StoreFunc) error {
	var location storage.SectorLocation
	var exists bool

	err := s.transaction(func(tx *txn) error {
		sectorID, err := insertSectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// check if the sector is already stored on disk
		location, err = sectorLocation(tx, sectorID, root)
		if err != nil && !errors.Is(err, storage.ErrSectorNotFound) {
			return fmt.Errorf("failed to check existing sector location: %w", err)
		} else if err == nil {
			exists = true
			return nil
		}

		location, err = emptyLocation(tx)
		if err != nil {
			return fmt.Errorf("failed to get empty location: %w", err)
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
		return err
	} else if exists {
		return nil
	}

	// call fn with the location
	if err := fn(location); err != nil {
		rollbackErr := s.transaction(func(tx *txn) error {
			_, err := tx.Exec(`UPDATE volume_sectors SET sector_id=null WHERE id=$1`, location.ID)
			if err != nil {
				return fmt.Errorf("failed to rollback sector location: %w", err)
			} else if err := incrementVolumeUsage(tx, location.Volume, -1); err != nil {
				return fmt.Errorf("failed to update volume metadata: %w", err)
			}
			return nil
		})
		if rollbackErr != nil {
			// rollbacks are best-effort. The dangling reference will be picked up by prune regardless.
			s.log.Error("failed to rollback volume metadata: %w", zap.NamedError("rollbackErr", rollbackErr), zap.Stringer("root", root), zap.Error(err))
		}
		return err
	}
	return nil
}

// MigrateSectors migrates each occupied sector of a volume starting at
// startIndex. migrateFn will be called for each sector that needs to be migrated.
// The sector data should be copied to the new location and synced
// to disk immediately. If migrateFn returns an error, that sector will be
// considered failed and the migration will continue. If the context is
// canceled, the migration will stop and the error will be returned. If the host
// runs out of writable storage, migration will stop and ErrNotEnoughStorage will
// be returned. The number of sectors migrated and failed will always be returned, even if an
// error occurs.
func (s *Store) MigrateSectors(ctx context.Context, volumeID int64, startIndex uint64, migrateFn storage.MigrateFunc) (migrated, failed int, err error) {
	log := s.log.Named("migrate").With(zap.Uint64("startIndex", startIndex))
	for index := startIndex; ; {
		if ctx.Err() != nil {
			err = ctx.Err()
			return
		}

		var done bool
		err = s.transaction(func(tx *txn) error {
			const query = `SELECT vs.id, vs.volume_id, vs.volume_index, ss.sector_root, vs.sector_id FROM volume_sectors vs
LEFT JOIN stored_sectors ss ON vs.sector_id=ss.id
WHERE vs.volume_id=$1 AND vs.volume_index >= $2 AND vs.sector_id IS NOT NULL
ORDER BY vs.volume_index ASC
LIMIT 1;`

			var sectorID int64
			var from storage.SectorLocation
			err := tx.QueryRow(query, volumeID, index).Scan(&from.ID, &from.Volume, &from.Index, decodeNullable(&from.Root), &sectorID)
			if errors.Is(err, sql.ErrNoRows) {
				done = true
				return nil
			} else if err != nil {
				return fmt.Errorf("failed to get sector: %w", err)
			}
			index = from.Index + 1 // update the start index for the next iteration

			to, err := emptyLocationForMigration(tx, volumeID, startIndex)
			if err != nil {
				return fmt.Errorf("failed to get empty location: %w", err)
			}
			to.Root = from.Root

			// this does introduce a performance concern where the database is now locked while
			// waiting on disk I/O. This is acceptable since it's extremely important that migrations
			// are atomic.
			if migrateErr := migrateFn(from, to); migrateErr != nil {
				log.Error("failed to migrate sector", zap.Error(migrateErr), zap.Uint64("index", index), zap.Stringer("root", from.Root))
				failed++
				return nil
			}

			res, err := tx.Exec(`UPDATE volume_sectors SET sector_id=NULL WHERE id=$1 AND sector_id=$2`, from.ID, sectorID)
			if err != nil {
				return fmt.Errorf("failed to clear old sector location: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n != 1 {
				return errors.New("failed to clear old sector location: no rows affected")
			}

			res, err = tx.Exec(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2`, sectorID, to.ID)
			if err != nil {
				return fmt.Errorf("failed to update sector location: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n != 1 {
				return errors.New("failed to update sector location: no rows affected")
			}

			migrated++
			log.Debug("migrated sector", zap.Uint64("fromIndex", from.Index), zap.Int64("fromVolume", from.Volume), zap.Uint64("toIndex", to.Index), zap.Int64("toVolume", to.Volume), zap.Stringer("root", from.Root))
			if from.Volume == to.Volume {
				return nil // skip updating metrics if the volume is not changing
			}

			if err := incrementVolumeUsage(tx, from.Volume, -1); err != nil {
				return fmt.Errorf("failed to update old volume metadata: %w", err)
			} else if err := incrementVolumeUsage(tx, to.Volume, 1); err != nil {
				return fmt.Errorf("failed to update new volume metadata: %w", err)
			}
			return nil
		})
		if err != nil {
			err = fmt.Errorf("failed to migrate sector: %w", err)
			return
		} else if done {
			return
		}
		// allow other transactions to run
		jitterSleep(50 * time.Millisecond) // maximum of 48000 sectors per hour
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int64, err error) {
	err = s.transaction(func(tx *txn) error {
		volumeID, err = addVolume(tx, localPath, readOnly)
		return err
	})
	return
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
		jitterSleep(50 * time.Millisecond)
	}

	return s.transaction(func(tx *txn) error {
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

	return s.transaction(func(tx *txn) error {
		return growVolume(tx, id, maxSectors)
	})
}

// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If there are
// used sectors outside of the new maximum, ErrVolumeNotEmpty is returned.
func (s *Store) ShrinkVolume(id int64, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}

	return s.transaction(func(tx *txn) error {
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
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query, readOnly, volumeID)
		return err
	})
}

// SetAvailable sets the available flag on a volume.
func (s *Store) SetAvailable(volumeID int64, available bool) error {
	const query = `UPDATE storage_volumes SET available=$1 WHERE id=$2;`
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query, available, volumeID)
		return err
	})
}

// sectorDBID returns the ID of a sector root in the stored_sectors table.
func sectorDBID(tx *txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`UPDATE stored_sectors SET last_access_timestamp=$1 WHERE sector_root=$2 RETURNING id`, encode(time.Now()), encode(root)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrSectorNotFound
	}
	return
}

// insertSectorDBID inserts a sector root into the stored_sectors table if it
// does not already exist. If the sector root already exists, the ID is
// returned.
func insertSectorDBID(tx *txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO stored_sectors (sector_root, last_access_timestamp) VALUES ($1, $2) ON CONFLICT (sector_root) DO UPDATE SET last_access_timestamp=EXCLUDED.last_access_timestamp RETURNING id`, encode(root), encode(time.Now())).Scan(&id)
	return
}

func addVolume(tx *txn, localPath string, readOnly bool) (volumeID int64, err error) {
	const query = `INSERT INTO storage_volumes (disk_path, read_only, used_sectors, total_sectors) VALUES (?, ?, 0, 0) RETURNING id;`
	err = tx.QueryRow(query, localPath, readOnly).Scan(&volumeID)
	return
}

func growVolume(tx *txn, id int64, maxSectors uint64) error {
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
func sectorLocation(tx *txn, sectorID int64, root types.Hash256) (loc storage.SectorLocation, err error) {
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
func emptyLocation(tx *txn) (loc storage.SectorLocation, err error) {
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index
	FROM volume_sectors vs INDEXED BY volume_sectors_sector_writes_volume_id_sector_id_volume_index_compound
	INNER JOIN storage_volumes sv ON (sv.id=vs.volume_id)
	WHERE vs.sector_id IS NULL AND sv.available=true AND sv.read_only=false
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
func emptyLocationForMigration(tx *txn, volumeID int64, maxIndex uint64) (loc storage.SectorLocation, err error) {
	loc, err = emptyLocation(tx)
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		return // either a db error or a valid location
	} else if maxIndex == 0 {
		err = storage.ErrNotEnoughStorage
		return // no space in current volume
	}

	// if there is no space available, try to find a location in the same volume
	const query = `SELECT vs.id, vs.volume_id, vs.volume_index
FROM volume_sectors vs
WHERE vs.sector_id IS NULL AND vs.volume_id=$1 AND vs.volume_index < $2
LIMIT 1;`
	err = tx.QueryRow(query, volumeID, maxIndex).Scan(&loc.ID, &loc.Volume, &loc.Index)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrNotEnoughStorage
		return
	} else if err != nil {
		return
	}
	_, err = tx.Exec(`UPDATE volume_sectors SET sector_writes=sector_writes+1 WHERE id=$1`, loc.ID)
	return
}

func scanVolume(s scanner) (volume storage.Volume, err error) {
	err = s.Scan(&volume.ID, &volume.LocalPath, &volume.ReadOnly, &volume.Available, &volume.TotalSectors, &volume.UsedSectors)
	return
}
