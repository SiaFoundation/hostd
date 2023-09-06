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

var errSectorHasRefs = errors.New("sector has references")

type tempSectorRef struct {
	ID       int64
	SectorID int64
}

func (s *Store) batchExpireTempSectors(height uint64) (refs []tempSectorRef, reclaimed int, err error) {
	err = s.transaction(func(tx txn) error {
		refs, err = expiredTempSectors(tx, height, sqlSectorBatchSize)
		if err != nil {
			return fmt.Errorf("failed to select sectors: %w", err)
		} else if len(refs) == 0 {
			return nil
		}

		var tempIDs []int64
		for _, ref := range refs {
			tempIDs = append(tempIDs, ref.ID)
		}

		// delete the sectors
		query := `DELETE FROM temp_storage_sector_roots WHERE id IN (` + queryPlaceHolders(len(tempIDs)) + `);`
		res, err := tx.Exec(query, queryArgs(tempIDs)...)
		if err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		} else if rows, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if rows != int64(len(tempIDs)) {
			return fmt.Errorf("failed to delete all sectors: %w", err)
		}
		// decrement the temp sectors metric
		if err := incrementNumericStat(tx, metricTempSectors, -len(refs), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}

		for _, ref := range refs {
			err := pruneSectorRef(tx, ref.SectorID)
			if errors.Is(err, errSectorHasRefs) {
				continue
			} else if err != nil {
				return fmt.Errorf("failed to prune sector: %w", err)
			}
			reclaimed++
		}
		return nil
	})
	return
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	return s.transaction(func(tx txn) error {
		sectorID, err := sectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector: %w", err)
		}

		var volumeID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=null WHERE sector_id=$1 RETURNING volume_id;`, sectorID).Scan(&volumeID)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to remove sector: %w", err)
		}

		// decrement volume usage and metrics
		_, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1;`, volumeID)
		if err != nil {
			return fmt.Errorf("failed to update volume: %w", err)
		} else if err = incrementNumericStat(tx, metricPhysicalSectors, -1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
}

// SectorLocation returns the location of a sector or an error if the
// sector is not found. The sector is locked until release is
// called.
func (s *Store) SectorLocation(root types.Hash256) (storage.SectorLocation, func() error, error) {
	var lockID int64
	var location storage.SectorLocation
	err := s.transaction(func(tx txn) error {
		sectorID, err := sectorDBID(tx, root)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}
		location, err = sectorLocation(tx, sectorID)
		if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}
		lockID, err = lockSector(tx, sectorID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.SectorLocation{}, nil, err
	}
	unlock := func() error {
		return s.transaction(func(tx txn) error {
			return unlockSector(tx, lockID)
		})
	}
	return location, unlock, nil
}

// AddTemporarySectors adds the roots of sectors that are temporarily stored
// on the host. The sectors will be deleted after the expiration height.
func (s *Store) AddTemporarySectors(sectors []storage.TempSector) error {
	return s.transaction(func(tx txn) error {
		stmt, err := tx.Prepare(`INSERT INTO temp_storage_sector_roots (sector_id, expiration_height) SELECT id, $1 FROM stored_sectors WHERE sector_root=$2 RETURNING id;`)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
		defer stmt.Close()
		for _, sector := range sectors {
			var dbID int64
			err := stmt.QueryRow(sector.Expiration, sqlHash256(sector.Root)).Scan(&dbID)
			if err != nil {
				return fmt.Errorf("failed to add temp sector root: %w", err)
			}
		}
		if err := incrementNumericStat(tx, metricTempSectors, len(sectors), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
}

// ExpireTempSectors deletes the roots of sectors that are no longer
// temporarily stored on the host.
func (s *Store) ExpireTempSectors(height uint64) error {
	var totalExpired, totalRemoved int
	defer func() {
		s.log.Debug("expired temp sectors", zap.Uint64("height", height), zap.Int("expired", totalExpired), zap.Int("removed", totalRemoved))
	}()
	// delete in batches to avoid holding a lock on the table for too long
	for {
		expired, removed, err := s.batchExpireTempSectors(height)
		if err != nil {
			return fmt.Errorf("failed to expire sectors: %w", err)
		} else if len(expired) == 0 {
			return nil
		}
		totalExpired += len(expired)
		totalRemoved += removed
		jitterSleep(time.Millisecond) // allow other transactions to run
	}
}

func pruneSectorRef(tx txn, id int64) error {
	var hasReference bool
	// check if the sector is referenced by a contract
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM contract_sector_roots WHERE sector_id=$1)`, id).Scan(&hasReference)
	if err != nil {
		return fmt.Errorf("failed to check contract references: %w", err)
	} else if hasReference {
		return fmt.Errorf("sector referenced by contract: %w", errSectorHasRefs)
	}
	// check if the sector is referenced by temp storage
	err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM temp_storage_sector_roots WHERE sector_id=$1)`, id).Scan(&hasReference)
	if err != nil {
		return fmt.Errorf("failed to check temp references: %w", err)
	} else if hasReference {
		return fmt.Errorf("sector referenced by temp storage: %w", errSectorHasRefs)
	}
	// check if the sector is locked
	err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM locked_sectors WHERE sector_id=$1)`, id).Scan(&hasReference)
	if err != nil {
		return fmt.Errorf("failed to check lock references: %w", err)
	} else if hasReference {
		return fmt.Errorf("sector locked: %w", errSectorHasRefs)
	}

	// clear the volume sector reference
	var volumeDBID int64
	err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=NULL WHERE sector_id=$1 RETURNING volume_id`, id).Scan(&volumeDBID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to update volume sectors: %w", err)
	}
	// decrement the volume usage if the sector was in a volume. This should
	// only happen if a sector was forcibly removed
	if err == nil {
		// update the volume usage
		if _, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1`, volumeDBID); err != nil {
			return fmt.Errorf("failed to update volume: %w", err)
		}
		// decrement the physical sectors metric
		if err = incrementNumericStat(tx, metricPhysicalSectors, -1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
	}

	// delete the sector
	if _, err = tx.Exec(`DELETE FROM stored_sectors WHERE id=$1`, id); err != nil {
		return fmt.Errorf("failed to delete sector: %w", err)
	}
	return nil
}

func expiredTempSectors(tx txn, height uint64, limit int) (sectors []tempSectorRef, _ error) {
	const query = `SELECT ts.id, ts.sector_id FROM temp_storage_sector_roots ts
WHERE expiration_height <= $1 LIMIT $2;`
	rows, err := tx.Query(query, height, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref tempSectorRef
		if err := rows.Scan(&ref.ID, &ref.SectorID); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectors = append(sectors, ref)
	}
	return
}

// lockSector locks a sector root. The lock must be released by calling
// unlockSector. A sector must be locked when it is being read or written
// to prevent it from being removed by prune sector.
func lockSector(tx txn, sectorDBID int64) (lockID int64, err error) {
	err = tx.QueryRow(`INSERT INTO locked_sectors (sector_id) VALUES ($1) RETURNING id;`, sectorDBID).Scan(&lockID)
	return
}

// deleteLocks removes the lock records with the given ids and returns the
// sector ids of the deleted locks.
func deleteLocks(tx txn, ids []int64) (sectorIDs []int64, err error) {
	if len(ids) == 0 {
		return nil, nil
	}

	query := `DELETE FROM locked_sectors WHERE id IN (` + queryPlaceHolders(len(ids)) + `) RETURNING sector_id;`
	rows, err := tx.Query(query, queryArgs(ids)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var sectorID int64
		if err := rows.Scan(&sectorID); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectorIDs = append(sectorIDs, sectorID)
	}
	return
}

// unlockSector unlocks a sector root.
func unlockSector(txn txn, lockIDs ...int64) error {
	if len(lockIDs) == 0 {
		return nil
	}

	sectorIDs, err := deleteLocks(txn, lockIDs)
	if err != nil {
		return fmt.Errorf("failed to delete locks: %w", err)
	}

	for _, sectorID := range sectorIDs {
		err := pruneSectorRef(txn, sectorID)
		if errors.Is(err, errSectorHasRefs) {
			continue
		} else if err != nil {
			return fmt.Errorf("failed to prune sector: %w", err)
		}
	}
	return nil
}

// lockLocations locks multiple sector locations and returns a list of lock
// IDs. The lock ids must be unlocked by unlockLocations. Volume locations
// should be locked during writes to prevent the location from being written
// to by another goroutine.
func lockLocations(tx txn, locations []storage.SectorLocation) (locks []int64, err error) {
	if len(locations) == 0 {
		return nil, nil
	}
	stmt, err := tx.Prepare(`INSERT INTO locked_volume_sectors (volume_sector_id) VALUES ($1) RETURNING id;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()
	for _, location := range locations {
		var lockID int64
		err := stmt.QueryRow(location.ID).Scan(&lockID)
		if err != nil {
			return nil, fmt.Errorf("failed to lock location %v:%v: %w", location.Volume, location.Index, err)
		}
		locks = append(locks, lockID)
	}
	return
}

// unlockLocations unlocks multiple locked sector locations. It is safe to
// call multiple times.
func unlockLocations(tx txn, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	query := `DELETE FROM locked_volume_sectors WHERE id IN (` + queryPlaceHolders(len(ids)) + `);`
	_, err := tx.Exec(query, queryArgs(ids)...)
	return err
}
