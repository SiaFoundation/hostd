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
	empties, err := collectRows(rows, func(s scanner) (empty bool, err error) {
		err = s.Scan(&empty)
		return empty, err
	})
	if err != nil {
		return 0, 0, err
	}
	removed = int64(len(empties))
	for _, empty := range empties {
		if !empty {
			lost++
		}
	}
	return removed, lost, nil
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

func addTempSector(tx *txn, sectorID int64, expiration uint64) error {
	if err := incrementNumericStat(tx, metricTempSectors, 1, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO temp_storage_sector_roots (sector_id, expiration_height) VALUES ($1, $2)`, sectorID, expiration); err != nil {
		return fmt.Errorf("failed to add temp sector root: %w", err)
	}
	return nil
}

func (s *Store) batchRemoveVolumeSectors(id int64, force bool) (removed, lost int64, err error) {
	err = s.writeTransaction(func(tx *txn) error {
		removed, lost = 0, 0
		if err := checkVolumeSectorLocks(tx, id, 0); err != nil {
			return err
		}
		if force {
			removed, lost, err = forceDeleteVolumeSectors(tx, id)
			if err != nil {
				return fmt.Errorf("failed to remove volume sectors: %w", err)
			}

			if lost > 0 {
				// special case: if the volume sectors are force deleted, usage
				// is freed
				if err := incrementVolumeUsage(tx, id, -int(lost)); err != nil {
					return fmt.Errorf("failed to update volume usage: %w", err)
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
		volumes, err = collectRows(rows, scanVolume)
		if err != nil {
			return fmt.Errorf("failed to scan volume: %w", err)
		}
		return nil
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

// AddTempSector adds a temporary reference to a sector, storing it first if
// necessary.
func (s *Store) AddTempSector(root types.Hash256, expiration uint64, fn storage.StoreFunc) error {
	var location storage.SectorLocation
	var exists bool
	err := s.writeTransaction(func(tx *txn) error {
		exists = false
		var sectorID int64
		err := tx.QueryRow(`SELECT ss.id FROM stored_sectors ss
INNER JOIN volume_sectors vs ON vs.sector_id=ss.id
WHERE ss.sector_root=$1`, encode(root)).Scan(&sectorID)
		if err == nil {
			exists = true
			return addTempSector(tx, sectorID, expiration)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to check existing sector: %w", err)
		}
		location, err = emptyLocation(tx)
		if err != nil {
			return fmt.Errorf("failed to get empty location: %w", err)
		}
		location.Root = root
		return lockVolumeSector(tx, location.ID)
	})
	if err != nil {
		return err
	} else if exists {
		return nil
	}

	cleanup := func(cause error) error {
		if err := s.writeTransaction(func(tx *txn) error {
			return releaseVolumeSector(tx, location.ID)
		}); err != nil {
			return errors.Join(cause, fmt.Errorf("failed to release location: %w", err))
		}
		return cause
	}
	if err := fn(location); err != nil {
		return cleanup(err)
	}

	err = s.writeTransaction(func(tx *txn) error {
		sectorID, err := insertSectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to insert sector: %w", err)
		}
		// a concurrent upload may have published the root during fn
		_, err = sectorLocation(tx, sectorID, root)
		if errors.Is(err, storage.ErrSectorNotFound) {
			res, err := tx.Exec(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 AND sector_id IS NULL`, sectorID, location.ID)
			if err != nil {
				return fmt.Errorf("failed to commit sector location: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to check rows affected: %w", err)
			} else if n != 1 {
				return errors.New("reserved sector location is gone or occupied")
			} else if err := incrementVolumeUsage(tx, location.Volume, 1); err != nil {
				return fmt.Errorf("failed to update volume usage: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check completed sector: %w", err)
		}
		if err := addTempSector(tx, sectorID, expiration); err != nil {
			return err
		}
		return releaseVolumeSector(tx, location.ID)
	})
	if err != nil {
		return cleanup(err)
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

		// reserve the source and destination
		var done bool
		var sectorID int64
		var from, to storage.SectorLocation
		err = s.writeTransaction(func(tx *txn) error {
			done = false
			const query = `SELECT vs.id, vs.volume_id, vs.volume_index, ss.sector_root, vs.sector_id
FROM volume_sectors vs
LEFT JOIN stored_sectors ss ON vs.sector_id=ss.id
WHERE vs.volume_id=$1 AND vs.volume_index >= $2 AND vs.sector_id IS NOT NULL
ORDER BY vs.volume_index ASC
LIMIT 1;`

			err := tx.QueryRow(query, volumeID, index).Scan(&from.ID, &from.Volume, &from.Index, decodeNullable(&from.Root), &sectorID)
			if errors.Is(err, sql.ErrNoRows) {
				done = true
				return nil
			} else if err != nil {
				return fmt.Errorf("failed to get sector: %w", err)
			}

			to, err = emptyLocationForMigration(tx, volumeID, startIndex)
			if err != nil {
				return fmt.Errorf("failed to get empty location: %w", err)
			}
			to.Root = from.Root

			if err := lockVolumeSector(tx, from.ID); err != nil {
				return fmt.Errorf("failed to lock source: %w", err)
			} else if err := lockVolumeSector(tx, to.ID); err != nil {
				return fmt.Errorf("failed to lock destination: %w", err)
			}
			return nil
		})
		if err != nil {
			err = fmt.Errorf("failed to reserve sector: %w", err)
			return
		} else if done {
			return
		}
		index = from.Index + 1

		release := func(tx *txn) error {
			if err := releaseVolumeSector(tx, from.ID); err != nil {
				return fmt.Errorf("failed to release source: %w", err)
			} else if err := releaseVolumeSector(tx, to.ID); err != nil {
				return fmt.Errorf("failed to release destination: %w", err)
			}
			return nil
		}

		if migrateErr := migrateFn(from, to); migrateErr != nil {
			log.Error("failed to migrate sector", zap.Error(migrateErr), zap.Uint64("index", from.Index), zap.Stringer("root", from.Root))
			failed++
			if err = s.writeTransaction(release); err != nil {
				err = fmt.Errorf("failed to release locations: %w", err)
				return
			}
			// allow other transactions to run
			jitterSleep(50 * time.Millisecond) // maximum of 48000 sectors per hour
			continue
		}

		// swap the sector to the destination
		var moved bool
		err = s.writeTransaction(func(tx *txn) error {
			moved = false
			res, err := tx.Exec(`UPDATE volume_sectors SET sector_id=NULL WHERE id=$1 AND sector_id=$2`, from.ID, sectorID)
			if err != nil {
				return fmt.Errorf("failed to clear old sector location: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n == 0 {
				// the sector was removed during the copy
				return release(tx)
			}

			res, err = tx.Exec(`UPDATE volume_sectors SET sector_id=$1 WHERE id=$2 AND sector_id IS NULL`, sectorID, to.ID)
			if err != nil {
				return fmt.Errorf("failed to update sector location: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n != 1 {
				return errors.New("failed to update sector location: destination is gone")
			}
			moved = true

			if from.Volume != to.Volume {
				if err := incrementVolumeUsage(tx, from.Volume, -1); err != nil {
					return fmt.Errorf("failed to update old volume metadata: %w", err)
				} else if err := incrementVolumeUsage(tx, to.Volume, 1); err != nil {
					return fmt.Errorf("failed to update new volume metadata: %w", err)
				}
			}
			return release(tx)
		})
		if err != nil {
			err = fmt.Errorf("failed to migrate sector: %w", err)
			if releaseErr := s.writeTransaction(release); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to release locations: %w", releaseErr))
			}
			return
		} else if moved {
			migrated++
			log.Debug("migrated sector", zap.Uint64("fromIndex", from.Index), zap.Int64("fromVolume", from.Volume), zap.Uint64("toIndex", to.Index), zap.Int64("toVolume", to.Volume), zap.Stringer("root", from.Root))
		}
		// allow other transactions to run
		jitterSleep(50 * time.Millisecond) // maximum of 48000 sectors per hour
	}
}

// AddVolume initializes a new storage volume and adds it to the volume
// store. GrowVolume must be called afterwards to initialize the volume
// to its desired size.
func (s *Store) AddVolume(localPath string, readOnly bool) (volumeID int64, err error) {
	err = s.writeTransaction(func(tx *txn) error {
		volumeID, err = addVolume(tx, localPath, readOnly)
		return err
	})
	return
}

// RemoveVolume removes a storage volume from the volume store. If there
// are used sectors in the volume, ErrVolumeNotEmpty is returned. If force is
// true, the volume is removed regardless of whether it is empty.
// Locked locations prevent removal even when force is true.
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

	return s.writeTransaction(func(tx *txn) error {
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

	return s.writeTransaction(func(tx *txn) error {
		return growVolume(tx, id, maxSectors)
	})
}

// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If there are
// used sectors outside of the new maximum, ErrVolumeNotEmpty is returned.
// Locked locations outside of the new maximum also prevent shrinking.
func (s *Store) ShrinkVolume(id int64, maxSectors uint64) error {
	if maxSectors == 0 {
		panic("maxSectors must be greater than 0") // dev error
	}

	return s.writeTransaction(func(tx *txn) error {
		if err := checkVolumeSectorLocks(tx, id, maxSectors); err != nil {
			return err
		}
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
	return s.writeTransaction(func(tx *txn) error {
		_, err := tx.Exec(query, readOnly, volumeID)
		return err
	})
}

// SetAvailable sets the available flag on a volume.
func (s *Store) SetAvailable(volumeID int64, available bool) error {
	const query = `UPDATE storage_volumes SET available=$1 WHERE id=$2;`
	return s.writeTransaction(func(tx *txn) error {
		_, err := tx.Exec(query, available, volumeID)
		return err
	})
}

// sectorDBID returns the ID of a sector root in the stored_sectors table.
func sectorDBID(tx *txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM stored_sectors WHERE sector_root=$1`, encode(root)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = storage.ErrSectorNotFound
	}
	return
}

// insertSectorDBID inserts a sector root into the stored_sectors table if it
// does not already exist. If the sector root already exists, the ID is
// returned.
func insertSectorDBID(tx *txn, root types.Hash256) (id int64, err error) {
	err = tx.QueryRow(`INSERT INTO stored_sectors (sector_root) VALUES ($1) ON CONFLICT (sector_root) DO UPDATE SET sector_root=EXCLUDED.sector_root RETURNING id`, encode(root)).Scan(&id)
	return
}

func checkVolumeSectorLocks(tx *txn, volumeID int64, minIndex uint64) error {
	var index uint64
	err := tx.QueryRow(`SELECT vs.volume_index FROM volume_sector_locks l
CROSS JOIN volume_sectors vs ON vs.id=l.volume_sector_id
WHERE vs.volume_id=$1 AND vs.volume_index >= $2 LIMIT 1`, volumeID, minIndex).Scan(&index)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check volume sector locks: %w", err)
	}
	return fmt.Errorf("sector at volume %d index %d is locked", volumeID, index)
}

// lockVolumeSector locks a volume location so concurrent
// operations can't re-use it while disk IO is inflight. Requires
// an exclusive write transaction.
func lockVolumeSector(tx *txn, volumeSectorID int64) error {
	_, err := tx.Exec(`INSERT INTO volume_sector_locks (volume_sector_id) VALUES ($1)`, volumeSectorID)
	return err
}

func releaseVolumeSector(tx *txn, volumeSectorID int64) error {
	_, err := tx.Exec(`DELETE FROM volume_sector_locks WHERE volume_sector_id=$1`, volumeSectorID)
	return err
}

func clearVolumeSectorLocks(tx *txn) error {
	_, err := tx.Exec(`DELETE FROM volume_sector_locks`)
	return err
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
		AND NOT EXISTS (SELECT 1 FROM volume_sector_locks l WHERE l.volume_sector_id=vs.id)
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
	AND NOT EXISTS (SELECT 1 FROM volume_sector_locks l WHERE l.volume_sector_id=vs.id)
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
