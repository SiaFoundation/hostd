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

type tempSectorRef struct {
	ID       int64
	SectorID int64
}

func (s *Store) batchExpireTempSectors(height uint64) (refs []tempSectorRef, err error) {
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
			if err := pruneSectorRef(tx, ref.SectorID); err != nil {
				return fmt.Errorf("failed to prune sector: %w", err)
			}
		}
		return nil
	})
	return
}

// unlockLocationFn returns a function that unlocks a sector when called.
func (s *Store) unlockLocationFn(id int64) func() error {
	return func() error { return unlockLocation(&dbTxn{s}, id) }
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	return s.transaction(func(tx txn) error {
		var volumeID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=null WHERE sector_id IN (SELECT id FROM stored_sectors WHERE sector_root=$1) RETURNING volume_id;`, sqlHash256(root)).Scan(&volumeID)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to remove sector: %w", err)
		}

		// decrement volume usage
		_, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1;`, volumeID)
		if err != nil {
			return fmt.Errorf("failed to update volume: %w", err)
		}
		return nil
	})
}

// SectorLocation returns the location of a sector or an error if the
// sector is not found. The location is locked until release is
// called.
func (s *Store) SectorLocation(root types.Hash256) (storage.SectorLocation, func() error, error) {
	var lockID int64
	var location storage.SectorLocation
	err := s.transaction(func(tx txn) error {
		var err error
		location, err = sectorLocation(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}
		lockID, err = lockLocation(tx, location.ID)
		if err != nil {
			return fmt.Errorf("failed to lock sector: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.SectorLocation{}, nil, err
	}
	return location, s.unlockLocationFn(lockID), nil
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
	var total int
	defer func() {
		s.log.Debug("removed temp sectors", zap.Uint64("height", height), zap.Int("removed", total))
	}()
	// delete in batches to avoid holding a lock on the table for too long
	for {
		removed, err := s.batchExpireTempSectors(height)
		if err != nil {
			return fmt.Errorf("failed to expire sectors: %w", err)
		} else if len(removed) == 0 {
			return nil
		}
		total += len(removed)
		jitterSleep(time.Millisecond) // allow other transactions to run
	}
}

func pruneSectorRef(tx txn, id int64) error {
	var contractRef, tempRef, lockRef bool
	// check if the sector is referenced by a contract
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM contract_sector_roots WHERE sector_id=$1)`, id).Scan(&contractRef)
	if err != nil {
		return fmt.Errorf("failed to check contract references: %w", err)
	}
	// check if the sector is referenced by temp storage
	err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM temp_storage_sector_roots WHERE sector_id=$1)`, id).Scan(&tempRef)
	if err != nil {
		return fmt.Errorf("failed to check temp references: %w", err)
	}
	// check if the sector location is locked
	err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM locked_volume_sectors ls INNER JOIN volume_sectors vs ON (ls.volume_sector_id=vs.id) WHERE vs.sector_id=$1)`, id).Scan(&lockRef)
	if err != nil {
		return fmt.Errorf("failed to check lock references: %w", err)
	}
	// if the sector is referenced by a contract, temp storage, or locked, do
	// not remove it
	if contractRef || tempRef || lockRef {
		return nil
	}
	// clear the volume sector reference
	var volumeDBID int64
	err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=NULL WHERE sector_id=$1 RETURNING volume_id`, id).Scan(&volumeDBID)
	if err != nil {
		return fmt.Errorf("failed to update volume sectors: %w", err)
	}
	// update the volume usage
	if _, err = tx.Exec(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1`, volumeDBID); err != nil {
		return fmt.Errorf("failed to update volume: %w", err)
	}
	// decrement the physical sectors metric
	if err = incrementNumericStat(tx, metricPhysicalSectors, -1, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	// delete the sector
	if _, err = tx.Exec(`DELETE FROM stored_sectors WHERE id=$1`, id); err != nil {
		return fmt.Errorf("failed to delete sector: %w", err)
	}
	return nil
}

// lockLocationBatch locks multiple sector locations and returns a list of lock
// IDs. The lock ids can be used with either unlockLocation or unlockLocationBatch
// to unlock the locations.
func lockLocationBatch(tx txn, locations ...storage.SectorLocation) (locks []int64, err error) {
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

// lockLocation locks a sector location and returns a lock ID. The lock
// id is used with unlockLocation to unlock the sector.
func lockLocation(tx txn, locationID int64) (int64, error) {
	var lockID int64
	err := tx.QueryRow(`INSERT INTO locked_volume_sectors (volume_sector_id) VALUES ($1) RETURNING id;`, locationID).
		Scan(&lockID)
	return lockID, err
}

func deleteLocks(tx txn, ids []int64) (volumeSectors []int64, err error) {
	query := `DELETE FROM locked_volume_sectors WHERE id IN (` + queryPlaceHolders(len(ids)) + `) RETURNING volume_sector_id;`
	rows, err := tx.Query(query, queryArgs(ids)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var volumeSectorID int64
		if err := rows.Scan(&volumeSectorID); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector id: %w", err)
		}
		volumeSectors = append(volumeSectors, volumeSectorID)
	}
	return
}

func volumeSectorIDs(tx txn, volumeSectors []int64) (sectorID []int64, err error) {
	query := `SELECT sector_id FROM volume_sectors WHERE sector_id IS NOT NULL AND id IN (` + queryPlaceHolders(len(volumeSectors)) + `);`
	rows, err := tx.Query(query, queryArgs(volumeSectors)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectorID = append(sectorID, id)
	}
	return
}

// unlockLocation unlocks multiple locked sector locations. It is safe to
// call multiple times.
func unlockLocation(tx txn, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}

	// unlock the volume locations
	volumeSectors, err := deleteLocks(tx, ids)
	if err != nil {
		return fmt.Errorf("failed to delete locks: %w", err)
	}

	// get the sectors stored at the unlocked indices
	sectorIDs, err := volumeSectorIDs(tx, volumeSectors)
	if err != nil {
		return fmt.Errorf("failed to get sector ids: %w", err)
	}

	// attempt to prune each of the sectors
	for _, sectorID := range sectorIDs {
		if err := pruneSectorRef(tx, sectorID); err != nil {
			return fmt.Errorf("failed to prune sector: %w", err)
		}
	}
	return nil
}
