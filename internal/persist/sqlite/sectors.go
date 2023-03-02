package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
)

const pruneBatchSize = (10 << 30) / (1 << 22) // 10GiB

// unlockLocationFn returns a function that unlocks a sector when called.
func (s *Store) unlockLocationFn(id int64) func() error {
	return func() error { return unlockLocation(&dbTxn{s}, id) }
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	var dbID int64
	const query = `UPDATE volume_sectors SET sector_id=null WHERE sector_id IN (SELECT id FROM stored_sectors WHERE sector_root=$1) RETURNING id;`
	err = s.queryRow(query, sqlHash256(root)).Scan(&dbID)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrSectorNotFound
	}
	return
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

// Prune removes the metadata of any sectors that are not locked or referenced
// by a contract.
func (s *Store) Prune() error {
	// delete in batches to avoid holding a lock on the table for too long
	var done bool
	for {
		if done {
			return nil
		}
		err := s.transaction(func(tx txn) error {
			sectorIDs, err := sectorsForDeletion(tx, pruneBatchSize)
			if err != nil {
				return fmt.Errorf("failed to select sectors: %w", err)
			} else if len(sectorIDs) == 0 {
				done = true
				return nil
			}

			query := `DELETE FROM stored_sectors WHERE sector_id IN (` + queryPlaceHolders(len(sectorIDs)) + `);`
			if _, err := tx.Exec(query, queryArgs(sectorIDs)...); err != nil {
				return fmt.Errorf("failed to delete sectors: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		}
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

func sectorsForDeletion(tx txn, max int) (ids []int64, _ error) {
	rows, err := tx.Query(`SELECT id FROM stored_sectors WHERE id NOT IN (
SELECT sector_id FROM contract_sectors
UNION
SELECT sector_id FROM locked_volume_sectors
UNION
SELECT sector_id FROM temp_storage_sectors
) LIMIT $1;`, max)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan sector id: %w", err)
		}
		ids = append(ids, id)
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
	query := `DELETE FROM locked_volume_sectors WHERE id=$1`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err := stmt.Exec(id); err != nil {
			return fmt.Errorf("failed to unlock sector %v: %w", id, err)
		}
	}
	return nil
}
