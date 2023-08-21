package sqlite

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
)

// migrateVersion14 adds a no-op migration to ensure that VACUUM is triggered
// after migrations are complete.
func migrateVersion14(tx txn) error {
	_, err := tx.Exec(`SELECT true;`)
	return err
}

// migrateVersion13 adds an index to the storage table to speed up location selection
func migrateVersion13(tx txn) error {
	_, err := tx.Exec(`CREATE INDEX storage_volumes_read_only_available_used_sectors ON storage_volumes(available, read_only, used_sectors);`)
	return err
}

// migrateVersion12 adds an index to the contracts table to speed up sector pruning
func migrateVersion12(tx txn) error {
	_, err := tx.Exec(`CREATE INDEX contracts_window_end ON contracts(window_end);`)
	return err
}

// migrateVersion11 recalculates the contract collateral metrics for existing contracts.
func migrateVersion11(tx txn) error {
	rows, err := tx.Query(`SELECT locked_collateral, risked_collateral FROM contracts WHERE contract_status IN (?, ?)`, contracts.ContractStatusPending, contracts.ContractStatusActive)
	if err != nil {
		return fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()
	var totalLocked, totalRisked types.Currency
	for rows.Next() {
		var locked, risked types.Currency
		if err := rows.Scan((*sqlCurrency)(&locked), (*sqlCurrency)(&risked)); err != nil {
			return fmt.Errorf("failed to scan contract: %w", err)
		}
		totalLocked = totalLocked.Add(locked)
		totalRisked = totalRisked.Add(risked)
	}

	if err := setCurrencyStat(tx, metricLockedCollateral, totalLocked, time.Now()); err != nil {
		return fmt.Errorf("failed to increment locked collateral: %w", err)
	} else if err := setCurrencyStat(tx, metricRiskedCollateral, totalRisked, time.Now()); err != nil {
		return fmt.Errorf("failed to increment risked collateral: %w", err)
	}
	return nil
}

// migrateVersion10 drops the log_lines table.
func migrateVersion10(tx txn) error {
	_, err := tx.Exec(`DROP TABLE log_lines;`)
	return err
}

// migrateVersion9 recalculates the contract metrics for existing contracts.
func migrateVersion9(tx txn) error {
	rows, err := tx.Query(`SELECT contract_status, COUNT(*) FROM contracts GROUP BY contract_status`)
	if err != nil {
		return fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status contracts.ContractStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return fmt.Errorf("failed to scan contract: %w", err)
		}

		// set the metric value
		var metric string
		switch status {
		case contracts.ContractStatusPending:
			metric = metricPendingContracts
		case contracts.ContractStatusRejected:
			metric = metricRejectedContracts
		case contracts.ContractStatusActive:
			metric = metricActiveContracts
		case contracts.ContractStatusSuccessful:
			metric = metricSuccessfulContracts
		case contracts.ContractStatusFailed:
			metric = metricFailedContracts
		}

		if err := setNumericStat(tx, metric, uint64(count), time.Now()); err != nil {
			return fmt.Errorf("failed to update %v metric: %w", metric, err)
		}
	}
	return nil
}

// migrateVersion8 sets the initial values for the locked and risked collateral
// metrics for existing hosts
func migrateVersion8(tx txn) error {
	rows, err := tx.Query(`SELECT locked_collateral, risked_collateral FROM contracts WHERE contract_status IN (?, ?)`, contracts.ContractStatusPending, contracts.ContractStatusActive)
	if err != nil {
		return fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()
	var totalLocked, totalRisked types.Currency
	for rows.Next() {
		var locked, risked types.Currency
		if err := rows.Scan((*sqlCurrency)(&locked), (*sqlCurrency)(&risked)); err != nil {
			return fmt.Errorf("failed to scan contract: %w", err)
		}
		totalLocked = totalLocked.Add(locked)
		totalRisked = totalRisked.Add(risked)
	}

	if totalLocked.IsZero() && totalRisked.IsZero() {
		return nil
	}

	if err := incrementCurrencyStat(tx, metricLockedCollateral, totalLocked, false, time.Now()); err != nil {
		return fmt.Errorf("failed to increment locked collateral: %w", err)
	} else if err := incrementCurrencyStat(tx, metricRiskedCollateral, totalRisked, false, time.Now()); err != nil {
		return fmt.Errorf("failed to increment risked collateral: %w", err)
	}
	return nil
}

// migrateVersion7 adds the sector_cache_size column to the host_settings table
func migrateVersion7(tx txn) error {
	_, err := tx.Exec(`ALTER TABLE host_settings ADD COLUMN sector_cache_size INTEGER NOT NULL DEFAULT 0;`)
	return err
}

// migrateVersion6 fixes a bug where the physical sectors metric was not being
// properly decreased when a volume is force removed.
func migrateVersion6(tx txn) error {
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(id) FROM volume_sectors WHERE sector_id IS NOT NULL`).Scan(&count); err != nil {
		return fmt.Errorf("failed to count volume sectors: %w", err)
	}
	return setNumericStat(tx, metricPhysicalSectors, uint64(count), time.Now().Truncate(statInterval))
}

// migrateVersion5 fixes a bug where the contract sectors metric was not being
// properly increased when a contract is renewed. Unfortunately, this means that
// the contract sectors metric will drastically increase for existing hosts.
// This is unavoidable, as we have no way of knowing how many sectors were
// previously renewed.
func migrateVersion5(tx txn) error {
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM contract_sector_roots`).Scan(&count); err != nil {
		return fmt.Errorf("failed to count contract sector roots: %w", err)
	}

	return setNumericStat(tx, metricContractSectors, uint64(count), time.Now().Truncate(statInterval))
}

// migrateVersion4 changes the collateral setting to collateral_multiplier
func migrateVersion4(tx txn) error {
	const (
		newSettingsSchema = `CREATE TABLE host_settings (
			id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
			settings_revision INTEGER NOT NULL,
			accepting_contracts BOOLEAN NOT NULL,
			net_address TEXT NOT NULL,
			contract_price BLOB NOT NULL,
			base_rpc_price BLOB NOT NULL,
			sector_access_price BLOB NOT NULL,
			max_collateral BLOB NOT NULL,
			storage_price BLOB NOT NULL,
			egress_price BLOB NOT NULL,
			ingress_price BLOB NOT NULL,
			max_account_balance BLOB NOT NULL,
			collateral_multiplier REAL NOT NULL,
			max_account_age INTEGER NOT NULL,
			price_table_validity INTEGER NOT NULL,
			max_contract_duration INTEGER NOT NULL,
			window_size INTEGER NOT NULL,
			ingress_limit INTEGER NOT NULL,
			egress_limit INTEGER NOT NULL,
			ddns_provider TEXT NOT NULL,
			ddns_update_v4 BOOLEAN NOT NULL,
			ddns_update_v6 BOOLEAN NOT NULL,
			ddns_opts BLOB,
			registry_limit INTEGER NOT NULL
		);`
		migrateSettings = `INSERT INTO host_settings (id, settings_revision, accepting_contracts, net_address, 
contract_price, base_rpc_price, sector_access_price, collateral_multiplier, max_collateral, storage_price, egress_price, 
ingress_price, max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, 
ingress_limit, egress_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, registry_limit)
SELECT 0, settings_revision, accepting_contracts, net_address, contract_price, base_rpc_price, 
sector_access_price, 2.0, max_collateral, storage_price, egress_price, ingress_price, 
max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
egress_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, registry_limit FROM host_settings_old;`
	)

	if _, err := tx.Exec(`ALTER TABLE host_settings RENAME TO host_settings_old`); err != nil {
		return fmt.Errorf("failed to rename host_settings table: %w", err)
	} else if _, err := tx.Exec(newSettingsSchema); err != nil {
		return fmt.Errorf("failed to create new host_settings table: %w", err)
	}

	if _, err := tx.Exec(migrateSettings); err != nil {
		return fmt.Errorf("failed to migrate host_settings: %w", err)
	} else if _, err := tx.Exec(`DROP TABLE host_settings_old`); err != nil {
		return fmt.Errorf("failed to drop old host_settings table: %w", err)
	}
	// remove old collateral metrics
	_, err := tx.Exec(`DELETE FROM host_stats WHERE stat='collateral'`)
	return err
}

// migrateVersion3 adds a wallet hash to the global settings table to detect
// when the private key has changed.
func migrateVersion3(tx txn) error {
	_, err := tx.Exec(`ALTER TABLE global_settings ADD COLUMN wallet_hash BLOB;`)
	return err
}

// migrateVersion2 removes the min prefix from the price columns in host_settings
func migrateVersion2(tx txn) error {
	const (
		newSettingsSchema = `CREATE TABLE host_settings (
			id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
			settings_revision INTEGER NOT NULL,
			accepting_contracts BOOLEAN NOT NULL,
			net_address TEXT NOT NULL,
			contract_price BLOB NOT NULL,
			base_rpc_price BLOB NOT NULL,
			sector_access_price BLOB NOT NULL,
			collateral BLOB NOT NULL,
			max_collateral BLOB NOT NULL,
			storage_price BLOB NOT NULL,
			egress_price BLOB NOT NULL,
			ingress_price BLOB NOT NULL,
			max_account_balance BLOB NOT NULL,
			max_account_age INTEGER NOT NULL,
			price_table_validity INTEGER NOT NULL,
			max_contract_duration INTEGER NOT NULL,
			window_size INTEGER NOT NULL,
			ingress_limit INTEGER NOT NULL,
			egress_limit INTEGER NOT NULL,
			ddns_provider TEXT NOT NULL,
			ddns_update_v4 BOOLEAN NOT NULL,
			ddns_update_v6 BOOLEAN NOT NULL,
			ddns_opts BLOB,
			registry_limit INTEGER NOT NULL
		);`
		migrateSettings = `INSERT INTO host_settings (id, settings_revision, accepting_contracts, net_address, 
contract_price, base_rpc_price, sector_access_price, collateral, max_collateral, storage_price, egress_price, 
ingress_price, max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, 
ingress_limit, egress_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, registry_limit)
SELECT 0, settings_revision, accepting_contracts, net_address, contract_price, base_rpc_price, 
sector_access_price, collateral, max_collateral, min_storage_price, min_egress_price, min_ingress_price, 
max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
egress_limit, dyn_dns_provider, dns_update_v4, dns_update_v6, dyn_dns_opts, registry_limit FROM host_settings_old;`
	)

	if _, err := tx.Exec(`ALTER TABLE host_settings RENAME TO host_settings_old`); err != nil {
		return fmt.Errorf("failed to rename host_settings table: %w", err)
	} else if _, err := tx.Exec(newSettingsSchema); err != nil {
		return fmt.Errorf("failed to create new host_settings table: %w", err)
	}

	if _, err := tx.Exec(migrateSettings); err != nil {
		return fmt.Errorf("failed to migrate host_settings: %w", err)
	} else if _, err := tx.Exec(`DROP TABLE host_settings_old`); err != nil {
		return fmt.Errorf("failed to drop old host_settings table: %w", err)
	}
	return nil
}

// migrations is a list of functions that are run to migrate the database from
// one version to the next. Migrations are used to update existing databases to
// match the schema in init.sql.
var migrations = []func(tx txn) error{
	migrateVersion2,
	migrateVersion3,
	migrateVersion4,
	migrateVersion5,
	migrateVersion6,
	migrateVersion7,
	migrateVersion8,
	migrateVersion9,
	migrateVersion10,
	migrateVersion11,
	migrateVersion12,
	migrateVersion13,
	migrateVersion14,
}
