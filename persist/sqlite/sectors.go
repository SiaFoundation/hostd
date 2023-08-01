package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const sqlBatchSize = 256 // 1 GiB

type (
	sectorRef struct {
		ID   int64
		Root types.Hash256
	}
)

func (s *Store) batchExpireTempSectors(height uint64) (bool, error) {
	var done bool
	err := s.transaction(func(tx txn) error {
		sectors, err := expiredTempSectors(tx, height, sqlBatchSize)
		if err != nil {
			return fmt.Errorf("failed to select sectors: %w", err)
		} else if len(sectors) == 0 {
			done = true
			return nil
		}

		sectorIDs := make([]int64, 0, len(sectors))
		for _, sector := range sectors {
			sectorIDs = append(sectorIDs, sector.ID)
		}

		query := `DELETE FROM temp_storage_sector_roots WHERE id IN (` + queryPlaceHolders(len(sectorIDs)) + `);`
		if _, err := tx.Exec(query, queryArgs(sectorIDs)...); err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		} else if err := incrementNumericStat(tx, metricTempSectors, -len(sectorIDs), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}

		s.log.Debug("removed temp sectors", zap.Uint64("height", height), zap.Int("removed", len(sectors)), zap.Array("sectors", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, sector := range sectors {
				enc.AppendString(sector.Root.String())
			}
			return nil
		})))
		return nil
	})
	return done, err
}

func (s *Store) batchPruneSectors() (int, bool, error) {
	var count int
	var done bool
	err := s.transaction(func(tx txn) error {
		sectors, err := sectorsForDeletion(tx, sqlBatchSize)
		if err != nil {
			return fmt.Errorf("failed to select sectors: %w", err)
		} else if len(sectors) == 0 {
			done = true
			return nil
		}

		// TODO: each sector is removed in individual queries to keep the
		// volume counts accurate. There are more efficient ways to do this
		updateVolumeStmt, err := tx.Prepare(`UPDATE volume_sectors SET sector_id=null WHERE sector_id=$1 RETURNING volume_id;`)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
		defer updateVolumeStmt.Close()

		deleteStmt, err := tx.Prepare(`DELETE FROM stored_sectors WHERE id=$1;`)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
		defer deleteStmt.Close()

		// decrement volume usage
		metaUpdateStmt, err := tx.Prepare(`UPDATE storage_volumes SET used_sectors=used_sectors-1 WHERE id=$1;`)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
		defer metaUpdateStmt.Close()

		for _, sector := range sectors {
			var volumeID int64
			if err := updateVolumeStmt.QueryRow(sector.ID).Scan(&volumeID); err != nil {
				return fmt.Errorf("failed to get volume id for sector: %w", err)
			} else if _, err := deleteStmt.Exec(sector.ID); err != nil {
				return fmt.Errorf("failed to delete sector: %w", err)
			} else if _, err := metaUpdateStmt.Exec(volumeID); err != nil {
				return fmt.Errorf("failed to update volume metadata: %w", err)
			}
		}

		s.log.Debug("deleted unreferenced sectors", zap.Int("deleted", len(sectors)), zap.Array("sectors", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, sector := range sectors {
				enc.AppendString(sector.Root.String())
			}
			return nil
		})))

		if err := incrementNumericStat(tx, metricPhysicalSectors, -len(sectors), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		count += len(sectors)
		return nil
	})
	return count, done, err
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
	// delete in batches to avoid holding a lock on the table for too long
	for {
		done, err := s.batchExpireTempSectors(height)
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if done {
			return nil
		}
		time.Sleep(time.Millisecond) // allow other transactions to run
	}
}

// PruneSectors removes the metadata of any sectors that are not locked or referenced
// by a contract.
func (s *Store) PruneSectors() (int, error) {
	// delete in batches to avoid holding a lock on the table for too long
	var total int
	for {
		count, done, err := s.batchPruneSectors()
		if err != nil {
			return total, fmt.Errorf("failed to prune sectors: %w", err)
		}
		total += count
		if done {
			return total, nil
		}
		time.Sleep(time.Millisecond) // allow other transactions to run
	}
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

func expiredTempSectors(tx txn, height uint64, limit int) (sectors []sectorRef, _ error) {
	const query = `SELECT ts.id, ss.sector_root FROM temp_storage_sector_roots ts
INNER JOIN stored_sectors ss ON (ts.sector_id=ss.id)
WHERE expiration_height <= $1 LIMIT $2;`
	rows, err := tx.Query(query, height, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sector sectorRef
		if err := rows.Scan(&sector.ID, (*sqlHash256)(&sector.Root)); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectors = append(sectors, sector)
	}
	return
}

func sectorsForDeletion(tx txn, limit int) (sectors []sectorRef, _ error) {
	rows, err := tx.Query(`SELECT id, sector_root FROM stored_sectors WHERE id NOT IN (
SELECT sector_id FROM contract_sector_roots
UNION
SELECT vs.sector_id FROM locked_volume_sectors ls INNER JOIN volume_sectors vs ON (ls.volume_sector_id=vs.id)
UNION
SELECT sector_id FROM temp_storage_sector_roots
) LIMIT $1;`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sector sectorRef
		if err := rows.Scan(&sector.ID, (*sqlHash256)(&sector.Root)); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		sectors = append(sectors, sector)
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

// unlockLocation unlocks a locked sector location. It is safe to call
// multiple times.
func unlockLocation(tx txn, id int64) error {
	_, err := tx.Exec(`DELETE FROM locked_volume_sectors WHERE id=?;`, id)
	return err
}

// unlockLocationBatch unlocks multiple locked sector locations. It is safe to
// call multiple times.
func unlockLocationBatch(tx txn, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}

	query := `DELETE FROM locked_volume_sectors WHERE id IN (` + queryPlaceHolders(len(ids)) + `);`
	_, err := tx.Exec(query, queryArgs(ids)...)
	return err
}
