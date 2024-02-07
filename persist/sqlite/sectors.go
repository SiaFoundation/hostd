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
		if err = incrementVolumeUsage(tx, volumeID, -1); err != nil {
			return fmt.Errorf("failed to update volume usage: %w", err)
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
		location, err = sectorLocation(tx, sectorID, root)
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

func expiredTempSectors(tx txn, height uint64) (expired []int64, err error) {
	rows, err := tx.Query(`DELETE FROM temp_storage_sector_roots WHERE expiration_height <= $1 RETURNING sector_id;`, height)
	if err != nil {
		return nil, fmt.Errorf("failed to delete expired sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		expired = append(expired, id)
	}
	return
}

// ExpireTempSectors deletes the roots of sectors that are no longer
// temporarily stored on the host.
func (s *Store) ExpireTempSectors(height uint64) error {
	return s.transaction(func(tx txn) error {
		expired, err := expiredTempSectors(tx, height)
		if err != nil {
			return fmt.Errorf("failed to expire sectors: %w", err)
		}

		pruned, err := pruneSectors(tx, expired)
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		}
		s.log.Debug("pruned expired temp sectors", zap.Int("expired", len(expired)), zap.Int("pruned", len(pruned)))

		if err := incrementNumericStat(tx, metricTempSectors, -len(expired), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
}

// HasSector returns true if the sector is stored on the host.
func (s *Store) HasSector(root types.Hash256) (bool, error) {
	var dbID int64
	err := s.queryRow(`SELECT id FROM stored_sectors WHERE sector_root=$1`, sqlHash256(root)).Scan(&dbID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// SectorReferences returns the references, if any of a sector root
func (s *Store) SectorReferences(root types.Hash256) (refs storage.SectorReference, err error) {
	err = s.transaction(func(tx txn) error {
		dbID, err := sectorDBID(tx, root)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// check if the sector is referenced by a contract
		refs.Contracts, err = contractSectorRefs(tx, dbID)
		if err != nil {
			return fmt.Errorf("failed to get contracts: %w", err)
		}

		// check if the sector is referenced by temp storage
		refs.TempStorage, err = getTempStorageCount(tx, dbID)
		if err != nil {
			return fmt.Errorf("failed to get temp storage: %w", err)
		}

		// check if the sector is locked
		refs.Locks, err = getSectorLockCount(tx, dbID)
		if err != nil {
			return fmt.Errorf("failed to get locks: %w", err)
		}
		return nil
	})
	return
}

func contractSectorRefs(tx txn, sectorID int64) (contractIDs []types.FileContractID, err error) {
	rows, err := tx.Query(`SELECT DISTINCT contract_id FROM contract_sector_roots WHERE sector_id=$1;`, sectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to select contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var contractID types.FileContractID
		if err := rows.Scan((*sqlHash256)(&contractID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract id: %w", err)
		}
		contractIDs = append(contractIDs, contractID)
	}
	return
}

func getTempStorageCount(tx txn, sectorID int64) (n int, err error) {
	err = tx.QueryRow(`SELECT COUNT(*) FROM temp_storage_sector_roots WHERE sector_id=$1;`, sectorID).Scan(&n)
	return
}

func getSectorLockCount(tx txn, sectorID int64) (n int, err error) {
	err = tx.QueryRow(`SELECT COUNT(*) FROM locked_sectors WHERE sector_id=$1;`, sectorID).Scan(&n)
	return
}

func incrementVolumeUsage(tx txn, volumeID int64, delta int) error {
	var used int64
	err := tx.QueryRow(`UPDATE storage_volumes SET used_sectors=used_sectors+$1 WHERE id=$2 RETURNING used_sectors;`, delta, volumeID).Scan(&used)
	if err != nil {
		return fmt.Errorf("failed to update volume: %w", err)
	} else if used < 0 {
		panic("volume usage is negative") // developer error
	} else if err = incrementNumericStat(tx, metricPhysicalSectors, delta, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	return nil
}

func clearVolumeSectors(tx txn, ids []int64) error {
	stmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_id=NULL WHERE sector_id=$1 RETURNING volume_id`)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()

	volumeDeltas := make(map[int64]int)
	for _, id := range ids {
		var volumeDBID int64
		err := stmt.QueryRow(id).Scan(&volumeDBID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			return fmt.Errorf("failed to clear volume sector: %w", err)
		}

		volumeDeltas[volumeDBID]--
	}

	// decrement the volume usage
	for volumeDBID, delta := range volumeDeltas {
		if err := incrementVolumeUsage(tx, volumeDBID, delta); err != nil {
			return fmt.Errorf("failed to update volume usage: %w", err)
		}
	}
	return nil
}

func pruneSectors(tx txn, ids []int64) (pruned []int64, err error) {
	checkContractStmt, err := tx.Prepare(`SELECT 1 FROM contract_sector_roots WHERE sector_id=$1;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare contract check: %w", err)
	}
	defer checkContractStmt.Close()

	checkTempStmt, err := tx.Prepare(`SELECT 1 FROM temp_storage_sector_roots WHERE sector_id=$1;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare temp check: %w", err)
	}
	defer checkTempStmt.Close()

	checkLockStmt, err := tx.Prepare(`SELECT 1 FROM locked_sectors WHERE sector_id=$1;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare lock check: %w", err)
	}
	defer checkLockStmt.Close()

	var exists int
	for _, id := range ids {
		if existsErr := checkContractStmt.QueryRow(id).Scan(&exists); err != nil && !errors.Is(existsErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to check contract references: %w", existsErr)
		} else if exists == 1 {
			continue
		}

		if existsErr := checkTempStmt.QueryRow(id).Scan(&exists); err != nil && !errors.Is(existsErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to check temp references: %w", existsErr)
		} else if exists == 1 {
			continue
		}

		if existsErr := checkLockStmt.QueryRow(id).Scan(&exists); err != nil && !errors.Is(existsErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("failed to check lock references: %w", existsErr)
		} else if exists == 1 {
			continue
		}

		pruned = append(pruned, id)
	}

	if len(pruned) == 0 {
		return nil, nil
	}

	if err = clearVolumeSectors(tx, pruned); err != nil {
		return nil, fmt.Errorf("failed to clear volume sectors: %w", err)
	}

	deleteStmt, err := tx.Prepare(`DELETE FROM stored_sectors WHERE id=$1 RETURNING id;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete query: %w", err)
	}
	defer deleteStmt.Close()

	for _, id := range pruned {
		var deleted int64
		if err = deleteStmt.QueryRow(id).Scan(&deleted); err != nil {
			return nil, fmt.Errorf("failed to delete sector: %w", err)
		} else if deleted != id {
			return nil, fmt.Errorf("failed to delete sector: %w", err)
		}
	}
	return pruned, nil
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

	_, err = pruneSectors(txn, sectorIDs)
	return err
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
