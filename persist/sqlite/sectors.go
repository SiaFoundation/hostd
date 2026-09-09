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

func deleteTempSectors(tx *txn, height uint64) ([]int64, error) {
	const query = `DELETE FROM temp_storage_sector_roots
WHERE id IN (SELECT id FROM temp_storage_sector_roots WHERE expiration_height <= $1 LIMIT $2)
RETURNING sector_id;`

	rows, err := tx.Query(query, height, sqlSectorBatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to select sectors: %w", err)
	}
	return collectRows(rows, func(s scanner) (sectorID int64, err error) {
		err = s.Scan(&sectorID)
		return sectorID, err
	})
}

func (s *Store) batchExpireTempSectors(height uint64) (expired int, err error) {
	err = s.writeTransaction(func(tx *txn) error {
		sectorIDs, err := deleteTempSectors(tx, height)
		if err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		}
		expired = len(sectorIDs)
		if expired == 0 {
			return nil
		}

		// decrement the temp sectors metric
		if err := incrementNumericStat(tx, metricTempSectors, -expired, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric: %w", err)
		}
		return nil
	})
	return
}

// RemoveSector removes the metadata of a sector and returns its
// location in the volume.
func (s *Store) RemoveSector(root types.Hash256) (err error) {
	return s.writeTransaction(func(tx *txn) error {
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
	return s.writeTransaction(func(tx *txn) error {
		const query = `INSERT INTO sector_subtree_cache (sector_id, subtree_roots)
SELECT id, $1 FROM stored_sectors WHERE sector_root=$2
ON CONFLICT (sector_id) DO UPDATE SET subtree_roots=EXCLUDED.subtree_roots;`
		_, err := tx.Exec(query, encode(subtrees), encode(root))
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
		var result storage.SectorMetadata
		result.Location, err = sectorLocation(tx, sectorID, root)
		if err != nil {
			return fmt.Errorf("failed to get sector location: %w", err)
		}

		err = tx.QueryRow(`SELECT subtree_roots FROM sector_subtree_cache WHERE sector_id=$1;`, sectorID).Scan(decode(&result.CachedSubtrees))
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to get cached subtrees: %w", err)
		}
		meta = result
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

// AddTemporarySectors adds the roots of sectors that are temporarily stored
// on the host. The sectors will be deleted after the expiration height.
//
// Deprecated: use AddTempSector
func (s *Store) AddTemporarySectors(sectors []storage.TempSector) error {
	return s.writeTransaction(func(tx *txn) error {
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

type pruneCandidate struct {
	SectorID       int64
	VolumeSectorID sql.NullInt64
	VolumeID       sql.NullInt64
	Referenced     bool
}

// removePruneCandidates releases the locations of the unreferenced candidates
// and deletes their metadata. A candidate the reference tables still point at
// has its count recomputed instead.
func removePruneCandidates(tx *txn, log *zap.Logger, candidates []pruneCandidate) error {
	var repairIDs, sectorIDs, volumeSectorIDs []any
	volumeDeltas := make(map[int64]int)
	for _, c := range candidates {
		if c.Referenced {
			repairIDs = append(repairIDs, c.SectorID)
			continue
		}
		sectorIDs = append(sectorIDs, c.SectorID)
		if c.VolumeSectorID.Valid {
			volumeSectorIDs = append(volumeSectorIDs, c.VolumeSectorID.Int64)
			volumeDeltas[c.VolumeID.Int64]--
		}
	}

	if len(repairIDs) > 0 {
		log.Warn("repairing sector reference counts", zap.Int("sectors", len(repairIDs)))
		repairQuery := `UPDATE stored_sectors SET ref_count=(SELECT COUNT(*) FROM contract_sector_roots WHERE sector_id=stored_sectors.id)
	+(SELECT COUNT(*) FROM contract_v2_sector_roots WHERE sector_id=stored_sectors.id)
	+(SELECT COUNT(*) FROM temp_storage_sector_roots WHERE sector_id=stored_sectors.id)
WHERE id IN (` + queryPlaceHolders(len(repairIDs)) + `)`
		if _, err := tx.Exec(repairQuery, repairIDs...); err != nil {
			return fmt.Errorf("failed to repair sector reference counts: %w", err)
		}
	}
	if len(sectorIDs) == 0 {
		return nil
	}

	if len(volumeSectorIDs) > 0 {
		updateQuery := `UPDATE volume_sectors SET sector_id=NULL WHERE id IN (` + queryPlaceHolders(len(volumeSectorIDs)) + `)`
		if _, err := tx.Exec(updateQuery, volumeSectorIDs...); err != nil {
			return fmt.Errorf("failed to release volume sectors: %w", err)
		}
		for volumeID, delta := range volumeDeltas {
			if err := incrementVolumeUsage(tx, volumeID, delta); err != nil {
				return fmt.Errorf("failed to update volume %d usage: %w", volumeID, err)
			}
		}
	}

	deleteQuery := `DELETE FROM stored_sectors WHERE id IN (` + queryPlaceHolders(len(sectorIDs)) + `)`
	if _, err := tx.Exec(deleteQuery, sectorIDs...); err != nil {
		return fmt.Errorf("failed to delete stored sectors: %w", err)
	}
	return nil
}

// pruneSectorBatch removes up to sqlSectorBatchSize unreferenced sectors and
// returns the number of candidates it considered.
func pruneSectorBatch(tx *txn, log *zap.Logger) (int, error) {
	const query = `SELECT ss.id, vs.id, vs.volume_id,
	EXISTS (SELECT 1 FROM contract_sector_roots csr WHERE csr.sector_id=ss.id)
	OR EXISTS (SELECT 1 FROM contract_v2_sector_roots csr2 WHERE csr2.sector_id=ss.id)
	OR EXISTS (SELECT 1 FROM temp_storage_sector_roots tsr WHERE tsr.sector_id=ss.id)
FROM stored_sectors ss
LEFT JOIN volume_sectors vs ON vs.sector_id=ss.id
WHERE ss.ref_count=0
	AND NOT EXISTS (SELECT 1 FROM volume_sector_locks l WHERE l.volume_sector_id=vs.id)
ORDER BY ss.id
LIMIT $1`
	rows, err := tx.Query(query, sqlSectorBatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to select sectors: %w", err)
	}
	candidates, err := collectRows(rows, func(s scanner) (c pruneCandidate, err error) {
		err = s.Scan(&c.SectorID, &c.VolumeSectorID, &c.VolumeID, &c.Referenced)
		return c, err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to scan sectors: %w", err)
	} else if err := removePruneCandidates(tx, log, candidates); err != nil {
		return 0, err
	}
	return len(candidates), nil
}

// PruneSectors removes sectors that are no longer referenced by a contract or
// temp storage.
func (s *Store) PruneSectors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var n int
		err := s.writeTransaction(func(tx *txn) (err error) {
			n, err = pruneSectorBatch(tx, s.log)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if n == 0 {
			return nil
		}
		jitterSleep(50 * time.Millisecond)
	}
}

func contractSectorRefs(tx *txn, sectorID int64) ([]types.FileContractID, error) {
	rows, err := tx.Query(`SELECT DISTINCT contract_id FROM contract_sector_roots WHERE sector_id=$1;`, sectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to select contracts: %w", err)
	}
	return collectRows(rows, func(s scanner) (contractID types.FileContractID, err error) {
		err = s.Scan(decode(&contractID))
		return contractID, err
	})
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
