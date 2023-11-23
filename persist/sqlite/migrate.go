package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/internal/migrate"
)

// SiadMigrateHostKey overrides the host's private key.
func (s *Store) SiadMigrateHostKey(pk types.PrivateKey) error {
	return s.transaction(func(tx txn) error {
		_, err := tx.Exec(`UPDATE global_settings SET host_key=$1`, pk)
		return err
	})
}

// SiadMigrateContract adds a contract and its sector roots to the database.
func (s *Store) SiadMigrateContract(revision contracts.SignedRevision, negotiationHeight uint64, formationSet []types.Transaction, sectors []types.Hash256) error {
	return s.transaction(func(tx txn) error {
		dbID, err := contractDBID(tx, revision.Revision.ParentID)
		if errors.Is(err, sql.ErrNoRows) {
			// contract doesn't exist yet, create it
			dbID, err = insertContract(tx, revision, formationSet, types.ZeroCurrency, contracts.Usage{}, negotiationHeight)
			if err != nil {
				return fmt.Errorf("failed to insert contract: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get contract id: %w", err)
		}

		_, err = tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1`, dbID)
		if err != nil {
			return fmt.Errorf("failed to delete contract sectors: %w", err)
		}

		insertSectorStmt, err := tx.Prepare(`INSERT INTO stored_sectors (sector_root, last_access_timestamp) VALUES ($1, $2) ON CONFLICT (sector_root) DO UPDATE SET last_access_timestamp=EXCLUDED.last_access_timestamp RETURNING id`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer insertSectorStmt.Close()

		appendSectorStmt, err := tx.Prepare(`INSERT INTO contract_sector_roots (contract_id, sector_id, root_index) VALUES ($1, $2, $3)`)
		if err != nil {
			return fmt.Errorf("failed to prepare append statement: %w", err)
		}
		defer appendSectorStmt.Close()

		for i, sector := range sectors {
			var sectorID int64
			if err := insertSectorStmt.QueryRow(sqlHash256(sector), 0).Scan(&sectorID); err != nil {
				return fmt.Errorf("failed to insert sector: %w", err)
			} else if _, err := appendSectorStmt.Exec(dbID, sectorID, i); err != nil {
				return fmt.Errorf("failed to append sector: %w", err)
			}
		}
		return nil
	})
}

// SiadMigrateVolume adds a volume to the database and grows it to the specified
// size. If the volume already exists, the existing ID is returned.
func (s *Store) SiadMigrateVolume(localPath string, maxSectors uint64) (volumeID int64, err error) {
	err = s.transaction(func(tx txn) error {
		volumeID, err = volumeDBID(tx, localPath)
		if errors.Is(err, sql.ErrNoRows) {
			volumeID, err = addVolume(tx, localPath, false)
			if err != nil {
				return fmt.Errorf("failed to add volume: %w", err)
			}
		}

		if err := growVolume(tx, volumeID, maxSectors); err != nil {
			return fmt.Errorf("failed to grow volume: %w", err)
		}
		return nil
	})
	return
}

// SiadMigrateNextVolumeIndex returns the next available volume index for a
// volume.
func (s *Store) SiadMigrateNextVolumeIndex(volumeID int64) (volumeIndex int64, err error) {
	err = s.transaction(func(tx txn) error {
		err = tx.QueryRow(`SELECT volume_index FROM volume_sectors WHERE volume_id=$1 AND sector_id IS NULL ORDER BY volume_index ASC LIMIT 1`, volumeID).Scan(&volumeIndex)
		return err
	})
	return
}

// SiadMigrateStoredSector sets the volume index of a stored sector.
func (s *Store) SiadMigrateStoredSector(volumeID, volumeIndex int64, sectorID types.Hash256) (err error) {
	err = s.transaction(func(tx txn) error {
		sectorDBID, err := sectorDBID(tx, sectorID)
		if err != nil {
			return fmt.Errorf("failed to get sector id: %w", err)
		}

		// check if the sector is already stored
		err = tx.QueryRow(`SELECT volume_index FROM volume_sectors WHERE sector_id=$1`, sectorDBID).Scan(&volumeIndex)
		if err == nil {
			return migrate.ErrMigrateSectorStored
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to check if sector is stored: %w", err)
		}

		// find the next available index
		var dbID int64
		err = tx.QueryRow(`UPDATE volume_sectors SET sector_id=$1 WHERE volume_id=$2 AND volume_index=$3 RETURNING id`, sectorDBID, volumeID, volumeIndex).Scan(&dbID)
		if err != nil {
			return fmt.Errorf("failed to update volume sector: %w", err)
		}
		return err
	})
	return
}

// SiadUpdateMetrics recalculates the metrics that may have changed from
// migrating from siad.
func (s *Store) SiadUpdateMetrics() error {
	return s.transaction(func(tx txn) error {
		// recalculate the contract sectors metric
		var contractSectorCount int64
		if err := tx.QueryRow(`SELECT COUNT(*) FROM contract_sector_roots`).Scan(&contractSectorCount); err != nil {
			return fmt.Errorf("failed to query contract sector count: %w", err)
		} else if err := setNumericStat(tx, metricContractSectors, uint64(contractSectorCount), time.Now()); err != nil {
			return fmt.Errorf("failed to set contract sectors metric: %w", err)
		}

		// recalculate the physical sectors metric
		var physicalSectorsCount int64
		volumePhysicalSectorCount := make(map[int64]int64)
		rows, err := tx.Query(`SELECT volume_id, COUNT(*) FROM volume_sectors WHERE sector_id IS NOT NULL GROUP BY volume_id`)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to query volume sector count: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var volumeID, count int64
			if err := rows.Scan(&volumeID, &count); err != nil {
				return fmt.Errorf("failed to scan volume sector count: %w", err)
			}
			volumePhysicalSectorCount[volumeID] = count
			physicalSectorsCount += count
		}

		// update the physical sectors metric
		if err := setNumericStat(tx, metricPhysicalSectors, uint64(physicalSectorsCount), time.Now()); err != nil {
			return fmt.Errorf("failed to set contract sectors metric: %w", err)
		}

		// update the volume stats
		for volumeID, count := range volumePhysicalSectorCount {
			err := tx.QueryRow(`UPDATE storage_volumes SET used_sectors = $1 WHERE id = $2 RETURNING id`, count, volumeID).Scan(&volumeID)
			if err != nil {
				return fmt.Errorf("failed to update volume stats: %w", err)
			}
		}
		return nil
	})
}

func volumeDBID(tx txn, localPath string) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM storage_volumes WHERE disk_path=$1`, localPath).Scan(&id)
	return
}

func contractDBID(tx txn, contractID types.FileContractID) (id int64, err error) {
	err = tx.QueryRow(`SELECT id FROM contracts WHERE contract_id=$1`, sqlHash256(contractID)).Scan(&id)
	return
}
