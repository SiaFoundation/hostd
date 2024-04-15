package sqlite

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/metrics"
	"go.uber.org/zap"
)

func getMetrics(tx txn) (m metrics.Metrics, err error) {
	const query = `SELECT s.stat, s.stat_value
	FROM host_stats s
	JOIN (
		SELECT stat, MAX(date_created) AS most_recent
		FROM host_stats
		WHERE date_created <= $1
		GROUP BY stat
	) AS sub ON s.stat = sub.stat AND s.date_created = sub.most_recent;`
	rows, err := tx.Query(query, sqlTime(time.Now()))
	if err != nil {
		return metrics.Metrics{}, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var stat string
		var value []byte

		if err := rows.Scan(&stat, &value); err != nil {
			return metrics.Metrics{}, fmt.Errorf("failed to scan row: %w", err)
		}
		mustParseMetricValue(stat, value, &m)
	}
	m.Timestamp = time.Now()
	return
}

func getVolumeUsedSectors(tx txn) (map[int64]uint64, error) {
	volumes := make(map[int64]uint64)

	rows, err := tx.Query("SELECT volume_id, COUNT(*) FROM volume_sectors WHERE sector_id IS NOT NULL GROUP BY volume_id")
	if err != nil {
		return nil, fmt.Errorf("failed to query volume sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var volumeID int64
		var count uint64

		if err := rows.Scan(&volumeID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		volumes[volumeID] = count
	}
	return volumes, nil
}

func getVolumeCachedSectors(tx txn) (map[int64]uint64, error) {
	volumes := make(map[int64]uint64)

	rows, err := tx.Query("SELECT id, used_sectors FROM storage_volumes")
	if err != nil {
		return nil, fmt.Errorf("failed to query volume sectors: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var volumeID int64
		var count uint64

		if err := rows.Scan(&volumeID, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		volumes[volumeID] = count
	}
	return volumes, nil
}

func getAccountFunding(tx txn) (deposits []fundAmount, err error) {
	rows, err := tx.Query(`SELECT id, contract_id, amount FROM contract_account_funding`)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var deposit fundAmount
		if err := rows.Scan(&deposit.ID, &deposit.ContractID, (*sqlCurrency)(&deposit.Amount)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		deposits = append(deposits, deposit)
	}
	return deposits, rows.Err()
}

// VerifyAccountFunding checks the account_funding column for consitency with
// the recorded deposits.
func (s *Store) VerifyAccountFunding() error {
	log := s.log.Named("VerifyAccountFunding")
	return s.transaction(func(tx txn) error {
		fundSources, err := getAccountFunding(tx)
		if err != nil {
			return fmt.Errorf("failed to get fund sources: %w", err)
		}

		contractAccountFunding := make(map[int64]types.Currency)

		for _, source := range fundSources {
			contractAccountFunding[source.ContractID] = contractAccountFunding[source.ContractID].Add(source.Amount)
		}

		for contractID, expectedFunding := range contractAccountFunding {
			contract, err := getContract(tx, contractID)
			if err != nil {
				return fmt.Errorf("failed to get source contract %q: %w", contractID, err)
			} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
				log.Error("inconsistent account funding", zap.Stringer("contractID", contract.Revision.ParentID), zap.Stringer("expected", expectedFunding), zap.Stringer("actual", contract.Usage.AccountFunding))
			}
		}
		return nil
	})
}

// VerifyContractSectors verifies that all of the counts in the database are
// correct.
func (s *Store) VerifyContractSectors() error {
	return s.transaction(func(tx txn) error {
		// count used contract sectors
		var contractSectors uint64
		err := tx.QueryRow("SELECT COUNT(*) FROM contract_sector_roots").Scan(&contractSectors)
		if err != nil {
			return fmt.Errorf("failed to count contract sectors: %w", err)
		}

		// get contract sector metrics
		metrics, err := getMetrics(tx)
		if err != nil {
			return fmt.Errorf("failed to get metrics: %w", err)
		} else if metrics.Storage.ContractSectors != contractSectors {
			return fmt.Errorf("contract sector count mismatch: expected %v, got %v", metrics.Storage.ContractSectors, contractSectors)
		}

		// count used sectors
		volumeUsed, err := getVolumeUsedSectors(tx)
		if err != nil {
			return fmt.Errorf("failed to get volume sectors: %w", err)
		}

		volumeCached, err := getVolumeCachedSectors(tx)
		if err != nil {
			return fmt.Errorf("failed to get volume sectors: %w", err)
		}

		if len(volumeUsed) != len(volumeCached) {
			return fmt.Errorf("volume count mismatch: expected %v, got %v", len(volumeUsed), len(volumeCached))
		}

		var totalUsed, totalCached uint64
		for id, used := range volumeUsed {
			cached, ok := volumeCached[id]
			if !ok {
				return fmt.Errorf("volume %v not found in cached volumes", id)
			} else if used != cached {
				return fmt.Errorf("volume %v sector count mismatch: expected %v, got %v", id, used, cached)
			}

			totalUsed += used
			totalCached += cached
		}

		if totalUsed != metrics.Storage.PhysicalSectors {
			return fmt.Errorf("used sector count mismatch: expected %v, got %v", totalUsed, metrics.Storage.PhysicalSectors)
		} else if totalUsed != totalCached {
			return fmt.Errorf("cached sector count mismatch: expected %v, got %v", totalUsed, totalCached)
		}
		return nil
	})
}
