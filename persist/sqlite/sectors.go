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

func deleteTempSectors(tx *txn, height uint64) (sectorIDs []int64, err error) {
	const query = `DELETE FROM temp_storage_sector_roots
WHERE id IN (SELECT id FROM temp_storage_sector_roots WHERE expiration_height <= $1 LIMIT $2)
RETURNING sector_id;`

	rows, err := tx.Query(query, height, sqlSectorBatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
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

func (s *Store) batchExpireTempSectors(height uint64) (expired int, err error) {
	err = s.transaction(func(tx *txn) error {
		sectorIDs, err := deleteTempSectors(tx, height)
		if err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}
		expired = len(sectorIDs)

		// decrement the temp sectors metric
		if err := incrementNumericStat(tx, metricTempSectors, -len(sectorIDs), time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return err
	})
	return
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	return s.transaction(func(tx *txn) error {
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
		} else if err := incrementNumericStat(tx, metricLostSectors, 1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
}

// CacheSubtrees stores the cached subtree roots for a sector
func (s *Store) CacheSubtrees(root types.Hash256, subtrees []types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`UPDATE stored_sectors SET cached_subtree_roots=$1 WHERE sector_root=$2;`, encode(subtrees), encode(root))
		return err
	})
}

// SectorMetadata returns the location of a sector or an error if the
// sector is not found.
func (s *Store) SectorMetadata(root types.Hash256) (meta storage.SectorMetadata, err error) {
	err = s.transaction(func(tx *txn) error {
		sectorID, err := sectorDBID(tx, root)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}
		meta.Location, err = sectorLocation(tx, sectorID, root)
		if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}

		err = tx.QueryRow(`SELECT cached_subtree_roots FROM stored_sectors WHERE id=$1;`, sectorID).Scan(decodeNullable(&meta.CachedSubtrees))
		if err != nil {
			return fmt.Errorf("failed to get cached subtrees: %w", err)
		}
		return nil
	})
	return
}

// SectorLocation returns the location of a sector or an error if the
// sector is not found.
func (s *Store) SectorLocation(root types.Hash256) (location storage.SectorLocation, err error) {
	err = s.transaction(func(tx *txn) error {
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
		return nil
	})
	return
}

// AddTempSector adds a sector to temporary storage. The sectors will be deleted
// after the expiration height
func (s *Store) AddTempSector(root types.Hash256, expiration uint64) error {
	return s.transaction(func(tx *txn) error {
		// ensure the sector is written to a volume
		var sectorID int64
		err := tx.QueryRow(`SELECT ss.id FROM stored_sectors ss
INNER JOIN volume_sectors vs ON (vs.sector_id = ss.id)
WHERE ss.sector_root=$1`, encode(root)).Scan(&sectorID)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrSectorNotFound
		} else if err != nil {
			return fmt.Errorf("failed to find sector: %w", err)
		} else if err := incrementNumericStat(tx, metricTempSectors, 1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		_, err = tx.Exec(`INSERT INTO temp_storage_sector_roots (sector_id, expiration_height) VALUES ($1, $2)`, sectorID, expiration)
		return err
	})
}

// AddTemporarySectors adds the roots of sectors that are temporarily stored
// on the host. The sectors will be deleted after the expiration height.
//
// Deprecated: use AddTempSector
func (s *Store) AddTemporarySectors(sectors []storage.TempSector) error {
	return s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`INSERT INTO temp_storage_sector_roots (sector_id, expiration_height) SELECT id, $1 FROM stored_sectors WHERE sector_root=$2 RETURNING id;`)
		if err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
		defer stmt.Close()
		for _, sector := range sectors {
			var dbID int64
			err := stmt.QueryRow(sector.Expiration, encode(sector.Root)).Scan(&dbID)
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
	log := s.log.Named("ExpireTempSectors").With(zap.Uint64("height", height))
	// delete in batches to avoid holding a lock on the table for too long
	for i := 0; ; i++ {
		expired, err := s.batchExpireTempSectors(height)
		if err != nil {
			return fmt.Errorf("failed to expire sectors: %w", err)
		} else if expired == 0 {
			return nil
		}
		log.Debug("expired temp sectors", zap.Int("expired", expired), zap.Int("batch", i))
		jitterSleep(50 * time.Millisecond) // allow other transactions to run
	}
}

// SectorReferences returns the references, if any of a sector root
func (s *Store) SectorReferences(root types.Hash256) (refs storage.SectorReference, err error) {
	err = s.transaction(func(tx *txn) error {
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
		return nil
	})
	return
}

// HasSector returns true if the sector root is stored on the host
func (s *Store) HasSector(root types.Hash256) (exists bool, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT ss.id
FROM stored_sectors ss
WHERE ss.sector_root=$1 AND (EXISTS (SELECT 1 FROM contract_sector_roots csr WHERE ss.id = csr.sector_id)
       OR EXISTS (SELECT 1 FROM contract_v2_sector_roots csr2 WHERE ss.id = csr2.sector_id)
       OR EXISTS (SELECT 1 FROM temp_storage_sector_roots tsr WHERE ss.id = tsr.sector_id));`

		var sectorID int64
		err := tx.QueryRow(query, encode(root)).Scan(&sectorID)
		if err == nil {
			exists = true
			return nil
		} else if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return
}

type volumeSectorRef struct {
	VolumeID       int64
	VolumeSectorID int64
}

func updatePruneableVolumeSectors(tx *txn, lastAccess time.Time) (refs []volumeSectorRef, err error) {
	const query = `
UPDATE volume_sectors SET sector_id=null
WHERE id IN (
	SELECT vs.id FROM volume_sectors vs
	INNER JOIN stored_sectors ss ON vs.sector_id=ss.id
	LEFT JOIN contract_sector_roots csr ON ss.id=csr.sector_id
	LEFT JOIN contract_v2_sector_roots csr2 ON ss.id=csr2.sector_id
	LEFT JOIN temp_storage_sector_roots tsr ON ss.id=tsr.sector_id
	WHERE ss.last_access_timestamp < $1 AND csr.sector_id IS NULL AND csr2.sector_id IS NULL AND tsr.sector_id IS NULL
	LIMIT $2
) RETURNING id, volume_id;
`

	rows, err := tx.Query(query, encode(lastAccess), sqlSectorBatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to select volume sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ref volumeSectorRef
		if err := rows.Scan(&ref.VolumeSectorID, &ref.VolumeID); err != nil {
			return nil, fmt.Errorf("failed to scan volume sector: %w", err)
		}
		refs = append(refs, ref)
	}
	return
}

// PruneSectors removes volume references for sectors that have not been accessed since the provided
// timestamp and are no longer referenced by a contract or temp storage.
func (s *Store) PruneSectors(ctx context.Context, lastAccess time.Time) error {
	// note: last access can be removed after v2 when sectors are immediately committed to temp storage
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var done bool
		err := s.transaction(func(tx *txn) error {
			refs, err := updatePruneableVolumeSectors(tx, lastAccess)
			if err != nil {
				return fmt.Errorf("failed to select volume sectors: %w", err)
			} else if len(refs) == 0 {
				done = true
				return nil
			}

			volumeDeltas := make(map[int64]int)
			for _, ref := range refs {
				volumeDeltas[ref.VolumeID]--
			}

			for volumeID, delta := range volumeDeltas {
				if err := incrementVolumeUsage(tx, volumeID, delta); err != nil {
					return fmt.Errorf("failed to update volume %d usage: %w", volumeID, err)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if done {
			return nil
		}
		jitterSleep(50 * time.Millisecond)
	}
}

func contractSectorRefs(tx *txn, sectorID int64) (contractIDs []types.FileContractID, err error) {
	rows, err := tx.Query(`SELECT DISTINCT contract_id FROM contract_sector_roots WHERE sector_id=$1;`, sectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to select contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var contractID types.FileContractID
		if err := rows.Scan(decode(&contractID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract id: %w", err)
		}
		contractIDs = append(contractIDs, contractID)
	}
	return
}

func getTempStorageCount(tx *txn, sectorID int64) (n int, err error) {
	err = tx.QueryRow(`SELECT COUNT(*) FROM temp_storage_sector_roots WHERE sector_id=$1;`, sectorID).Scan(&n)
	return
}

func incrementVolumeUsage(tx *txn, volumeID int64, delta int) error {
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
