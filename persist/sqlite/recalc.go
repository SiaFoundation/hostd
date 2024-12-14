package sqlite

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.uber.org/zap"
)

func checkContractAccountFunding(tx *txn, log *zap.Logger) error {
	rows, err := tx.Query(`SELECT contract_id, amount FROM contract_account_funding`)
	if err != nil {
		return fmt.Errorf("failed to query contract account funding: %w", err)
	}
	defer rows.Close()

	contractFunding := make(map[int64]types.Currency)
	for rows.Next() {
		var contractID int64
		var amount types.Currency
		if err := rows.Scan(&contractID, decode(&amount)); err != nil {
			return fmt.Errorf("failed to scan contract account funding: %w", err)
		}
		contractFunding[contractID] = contractFunding[contractID].Add(amount)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate contract account funding: %w", err)
	} else if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close contract account funding: %w", err)
	}

	for contractID, amount := range contractFunding {
		var actualAmount types.Currency
		err := tx.QueryRow(`SELECT account_funding FROM contracts WHERE id=$1`, contractID).Scan(decode(&actualAmount))
		if err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		}

		if !actualAmount.Equals(amount) {
			log.Debug("incorrect contract account funding", zap.Int64("contractID", contractID), zap.Stringer("expected", amount), zap.Stringer("actual", actualAmount))
		}
	}
	return nil
}

func recalcContractAccountFunding(tx *txn, _ *zap.Logger) error {
	rows, err := tx.Query(`SELECT contract_id, amount FROM contract_account_funding`)
	if err != nil {
		return fmt.Errorf("failed to query contract account funding: %w", err)
	}
	defer rows.Close()

	contractFunding := make(map[int64]types.Currency)
	for rows.Next() {
		var contractID int64
		var amount types.Currency
		if err := rows.Scan(&contractID, decode(&amount)); err != nil {
			return fmt.Errorf("failed to scan contract account funding: %w", err)
		}
		contractFunding[contractID] = contractFunding[contractID].Add(amount)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate contract account funding: %w", err)
	} else if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close contract account funding: %w", err)
	}

	for contractID, amount := range contractFunding {
		res, err := tx.Exec(`UPDATE contracts SET account_funding=$1 WHERE id=$2`, encode(amount), contractID)
		if err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		} else if rowsAffected, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		} else if rowsAffected != 1 {
			return fmt.Errorf("failed to update contract account funding: %w", err)
		}
	}
	return nil
}

func recalcVolumeMetrics(tx *txn, log *zap.Logger) error {
	const query = `SELECT volume_id, COUNT(*) AS total_sectors, COUNT(CASE WHEN sector_id IS NOT NULL THEN 1 END) AS used_sectors
FROM volume_sectors
GROUP BY volume_id`

	type volCount struct {
		ID           int64
		UsedSectors  uint64
		TotalSectors uint64
	}
	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query volume metrics: %w", err)
	}
	defer rows.Close()

	var totalSectors, physicalSectors uint64
	var volumes []volCount
	for rows.Next() {
		var vol volCount
		if err := rows.Scan(&vol.ID, &vol.TotalSectors, &vol.UsedSectors); err != nil {
			return fmt.Errorf("failed to scan volume: %w", err)
		}
		volumes = append(volumes, vol)
		totalSectors += vol.TotalSectors
		physicalSectors += vol.UsedSectors
		log.Debug("calculated volume metrics", zap.Int64("id", vol.ID), zap.Uint64("used", vol.UsedSectors), zap.Uint64("total", vol.TotalSectors))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	stmt, err := tx.Prepare(`UPDATE storage_volumes SET used_sectors=$1, total_sectors=$2 WHERE id=$3`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, vol := range volumes {
		if _, err := stmt.Exec(vol.UsedSectors, vol.TotalSectors, vol.ID); err != nil {
			return fmt.Errorf("failed to update volume: %w", err)
		}
	}

	if err := setNumericStat(tx, metricTotalSectors, totalSectors, time.Now()); err != nil {
		return fmt.Errorf("failed to set total sectors: %w", err)
	} else if err := setNumericStat(tx, metricPhysicalSectors, physicalSectors, time.Now()); err != nil {
		return fmt.Errorf("failed to set physical sectors: %w", err)
	}
	return nil
}

func recalcContractMetrics(tx *txn, log *zap.Logger) error {
	var totalLocked types.Currency
	var totalPending, totalEarned contracts.Usage

	err := func() error {
		rows, err := tx.Query(`SELECT contract_status, locked_collateral, risked_collateral, rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, registry_read, registry_write FROM contracts WHERE contract_status IN (?, ?);`, contracts.ContractStatusActive, contracts.ContractStatusSuccessful)
		if err != nil {
			return fmt.Errorf("failed to query contracts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var status contracts.ContractStatus
			var lockedCollateral types.Currency
			var usage contracts.Usage

			if err := rows.Scan(&status, decode(&lockedCollateral), decode(&usage.RiskedCollateral), decode(&usage.RPCRevenue), decode(&usage.StorageRevenue), decode(&usage.IngressRevenue), decode(&usage.EgressRevenue), decode(&usage.AccountFunding), decode(&usage.RegistryRead), decode(&usage.RegistryWrite)); err != nil {
				return fmt.Errorf("failed to scan contract: %w", err)
			}

			switch status {
			case contracts.ContractStatusActive:
				totalLocked = totalLocked.Add(lockedCollateral)
				totalPending = totalPending.Add(usage)
			case contracts.ContractStatusSuccessful:
				totalEarned = totalEarned.Add(usage)
			}
		}
		return rows.Err()
	}()
	if err != nil {
		return fmt.Errorf("failed to calculate v1 metrics: %w", err)
	}

	err = func() error {
		rows, err := tx.Query(`SELECT contract_status, locked_collateral, risked_collateral, rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding FROM contracts_v2 WHERE contract_status IN (?, ?, ?);`, contracts.V2ContractStatusActive, contracts.V2ContractStatusSuccessful, contracts.V2ContractStatusRenewed)
		if err != nil {
			return fmt.Errorf("failed to query contracts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var status contracts.V2ContractStatus
			var lockedCollateral types.Currency
			var usage contracts.Usage

			if err := rows.Scan(&status, decode(&lockedCollateral), decode(&usage.RiskedCollateral), decode(&usage.RPCRevenue), decode(&usage.StorageRevenue), decode(&usage.IngressRevenue), decode(&usage.EgressRevenue), decode(&usage.AccountFunding), decode(&usage.RegistryRead), decode(&usage.RegistryWrite)); err != nil {
				return fmt.Errorf("failed to scan contract: %w", err)
			}

			switch status {
			case contracts.V2ContractStatusActive:
				totalLocked = totalLocked.Add(lockedCollateral)
				totalPending = totalPending.Add(usage)
			case contracts.V2ContractStatusSuccessful, contracts.V2ContractStatusRenewed:
				totalEarned = totalEarned.Add(usage)
			}
		}
		return rows.Err()
	}()
	if err != nil {
		return fmt.Errorf("failed to calculate v2 metrics: %w", err)
	}

	log.Debug("resetting contract metrics", zap.Stringer("lockedCollateral", totalLocked), zap.Stringer("riskedCollateral", totalPending.RiskedCollateral))

	if err := setCurrencyStat(tx, metricLockedCollateral, totalLocked, time.Now()); err != nil {
		return fmt.Errorf("failed to increment locked collateral: %w", err)
	} else if err := setCurrencyStat(tx, metricRiskedCollateral, totalPending.RiskedCollateral, time.Now()); err != nil {
		return fmt.Errorf("failed to increment risked collateral: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialRPCRevenue, totalPending.RPCRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment rpc revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialStorageRevenue, totalPending.StorageRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment storage revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialIngressRevenue, totalPending.IngressRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment ingress revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialEgressRevenue, totalPending.EgressRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment egress revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialRegistryReadRevenue, totalPending.RegistryRead, time.Now()); err != nil {
		return fmt.Errorf("failed to increment read registry revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricPotentialRegistryWriteRevenue, totalPending.RegistryWrite, time.Now()); err != nil {
		return fmt.Errorf("failed to increment write registry revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedRPCRevenue, totalEarned.RPCRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment rpc revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedStorageRevenue, totalEarned.StorageRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment storage revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedIngressRevenue, totalEarned.IngressRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment ingress revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedEgressRevenue, totalEarned.EgressRevenue, time.Now()); err != nil {
		return fmt.Errorf("failed to increment egress revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedRegistryReadRevenue, totalEarned.RegistryRead, time.Now()); err != nil {
		return fmt.Errorf("failed to increment read registry revenue: %w", err)
	} else if err := setCurrencyStat(tx, metricEarnedRegistryWriteRevenue, totalEarned.RegistryWrite, time.Now()); err != nil {
		return fmt.Errorf("failed to increment write registry revenue: %w", err)
	}
	return nil
}

// CheckContractAccountFunding checks that the contract account funding table
// is correct.
func (s *Store) CheckContractAccountFunding() error {
	return s.transaction(func(tx *txn) error {
		return checkContractAccountFunding(tx, s.log)
	})
}

// RecalcContractAccountFunding recalculates the contract account funding table.
func (s *Store) RecalcContractAccountFunding() error {
	return s.transaction(func(tx *txn) error {
		return recalcContractAccountFunding(tx, s.log)
	})
}

// Vacuum runs the VACUUM command on the database.
func (s *Store) Vacuum() error {
	_, err := s.db.Exec(`VACUUM`)
	return err
}
