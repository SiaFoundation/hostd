package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
)

// migrateVersion22 recalculates the locked and risked collateral metrics
func migrateVersion22(tx txn) error {
	rows, err := tx.Query(`SELECT locked_collateral, risked_collateral FROM contracts WHERE contract_status IN (?, ?);`, contracts.ContractStatusPending, contracts.ContractStatusActive)
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

func migrateVersion21(tx txn) error {
	const query = `
ALTER TABLE global_settings ADD COLUMN last_announce_key BLOB;
ALTER TABLE global_settings ADD COLUMN settings_last_processed_change BLOB;
ALTER TABLE global_settings ADD COLUMN last_announce_id BLOB;
ALTER TABLE global_settings ADD COLUMN last_announce_height INTEGER;
ALTER TABLE global_settings ADD COLUMN settings_height INTEGER;
ALTER TABLE global_settings ADD COLUMN last_announce_address TEXT;
`
	_, err := tx.Exec(query)
	return err
}

// migrateVersion20 adds a compound index to the volume_sectors table
func migrateVersion20(tx txn) error {
	_, err := tx.Exec(`CREATE INDEX volume_sectors_volume_id_sector_id_volume_index_set_compound ON volume_sectors (volume_id, sector_id, volume_index) WHERE sector_id IS NOT NULL;`)
	return err
}

// migrateVersion19 adds a compound index to the volume_sectors table
func migrateVersion19(tx txn) error {
	const query = `
DROP INDEX storage_volumes_read_only_available;
CREATE INDEX storage_volumes_id_available_read_only ON storage_volumes(id, available, read_only);
CREATE INDEX volume_sectors_volume_id_sector_id_volume_index_compound ON volume_sectors(volume_id, sector_id, volume_index) WHERE sector_id IS NULL;
`
	_, err := tx.Exec(query)
	return err
}

// migrateVersion18 adds an index to the volume_sectors table to speed up
// empty sector selection.
func migrateVersion18(tx txn) error {
	const query = `CREATE INDEX volume_sectors_volume_id_sector_id ON volume_sectors(volume_id, sector_id);`
	_, err := tx.Exec(query)
	return err
}

// migrateVersion17 recalculates the indices of all contract sector roots.
// Fixes a bug where the indices were not being properly updated if more than
// one root was trimmed.
func migrateVersion17(tx txn) error {
	const query = `
-- create a temp table that contains the new indices
CREATE TEMP TABLE temp_contract_sector_roots AS 
SELECT * FROM (SELECT id, contract_id, root_index, ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY root_index ASC)-1 AS expected_root_index 
FROM contract_sector_roots) a WHERE root_index <> expected_root_index ORDER BY contract_id, root_index ASC;
-- update the contract_sector_roots table with the new indices
UPDATE contract_sector_roots
SET root_index = (SELECT expected_root_index FROM temp_contract_sector_roots WHERE temp_contract_sector_roots.id = contract_sector_roots.id)
WHERE id IN (SELECT id FROM temp_contract_sector_roots);
-- drop the temp table
DROP TABLE temp_contract_sector_roots;`

	_, err := tx.Exec(query)
	return err
}

// migrateVersion16 recalculates the contract and physical sector metrics.
func migrateVersion16(tx txn) error {
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
}

// migrateVersion15 adds the registry usage fields to the contracts table,
// removes the usage fields from the accounts table, and refactors the
// contract_account_funding table.
func migrateVersion15(tx txn) error {
	const query = `
-- drop the tables that are being removed or refactored
DROP TABLE account_financial_records;
DROP TABLE contract_financial_records;
DROP TABLE contract_account_funding;

-- create the new tables
CREATE TABLE contract_account_funding (
	id INTEGER PRIMARY KEY,
	contract_id INTEGER NOT NULL REFERENCES contracts(id),
	account_id INTEGER NOT NULL REFERENCES accounts(id),
	amount BLOB NOT NULL,
	UNIQUE (contract_id, account_id)
);

CREATE TABLE contracts_new (
	id INTEGER PRIMARY KEY,
	renter_id INTEGER NOT NULL REFERENCES contract_renters(id),
	renewed_to INTEGER REFERENCES contracts(id) ON DELETE SET NULL,
	renewed_from INTEGER REFERENCES contracts(id) ON DELETE SET NULL,
	contract_id BLOB UNIQUE NOT NULL,
	revision_number BLOB NOT NULL, -- stored as BLOB to support uint64_max on clearing revisions
	formation_txn_set BLOB NOT NULL, -- binary serialized transaction set
	locked_collateral BLOB NOT NULL,
	rpc_revenue BLOB NOT NULL,
	storage_revenue BLOB NOT NULL,
	ingress_revenue BLOB NOT NULL,
	egress_revenue BLOB NOT NULL,
	account_funding BLOB NOT NULL,
	registry_read BLOB NOT NULL,
	registry_write BLOB NOT NULL,
	risked_collateral BLOB NOT NULL,
	confirmed_revision_number BLOB, -- stored as BLOB to support uint64_max on clearing revisions
	host_sig BLOB NOT NULL,
	renter_sig BLOB NOT NULL,
	raw_revision BLOB NOT NULL, -- binary serialized contract revision
	formation_confirmed BOOLEAN NOT NULL, -- true if the contract has been confirmed on the blockchain
	resolution_height INTEGER, -- null if the storage proof/resolution has not been confirmed on the blockchain, otherwise the height of the block containing the storage proof/resolution
	negotiation_height INTEGER NOT NULL, -- determines if the formation txn should be rebroadcast or if the contract should be deleted
	window_start INTEGER NOT NULL,
	window_end INTEGER NOT NULL,
	contract_status INTEGER NOT NULL
);

-- copy the data from the old contracts table to the new contracts table
INSERT INTO contracts_new (id, renter_id, renewed_to, renewed_from, contract_id, revision_number, formation_txn_set, locked_collateral, rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, registry_read, registry_write, risked_collateral, confirmed_revision_number, host_sig, renter_sig, raw_revision, formation_confirmed, resolution_height, negotiation_height, window_start, window_end, contract_status) 
	SELECT id, renter_id, renewed_to, renewed_from, contract_id, revision_number, formation_txn_set, locked_collateral, $1, $1, $1, $1, $1, $1, $1, risked_collateral, confirmed_revision_number, host_sig, renter_sig, raw_revision, formation_confirmed, resolution_height, negotiation_height, window_start, window_end, contract_status FROM contracts;

-- drop the old contracts table and rename the new contracts table
DROP TABLE contracts;
ALTER TABLE contracts_new RENAME TO contracts;

-- recreate the indexes on the contracts table
CREATE INDEX contracts_contract_id ON contracts(contract_id);
CREATE INDEX contracts_renter_id ON contracts(renter_id);
CREATE INDEX contracts_renewed_to ON contracts(renewed_to);
CREATE INDEX contracts_renewed_from ON contracts(renewed_from);
CREATE INDEX contracts_negotiation_height ON contracts(negotiation_height);
CREATE INDEX contracts_window_start ON contracts(window_start);
CREATE INDEX contracts_window_end ON contracts(window_end);
CREATE INDEX contracts_contract_status ON contracts(contract_status);
CREATE INDEX contracts_formation_confirmed_resolution_height_window_start ON contracts(formation_confirmed, resolution_height, window_start);
CREATE INDEX contracts_formation_confirmed_resolution_height_window_end ON contracts(formation_confirmed, resolution_height, window_end);
CREATE INDEX contracts_formation_confirmed_window_start ON contracts(formation_confirmed, window_start);
CREATE INDEX contracts_formation_confirmed_negotiation_height ON contracts(formation_confirmed, negotiation_height);`

	// one query parameter to reset the contract's tracked revenue to zero
	if _, err := tx.Exec(query, sqlCurrency(types.ZeroCurrency)); err != nil {
		return fmt.Errorf("failed to migrate contracts table: %w", err)
	}

	// set the initial account metrics
	var accountCount int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM accounts`).Scan(&accountCount); err != nil {
		return fmt.Errorf("failed to query account count: %w", err)
	} else if err := setNumericStat(tx, metricActiveAccounts, uint64(accountCount), time.Now()); err != nil {
		return fmt.Errorf("failed to set active accounts metric: %w", err)
	}

	var accountBalance types.Currency
	rows, err := tx.Query(`SELECT balance FROM accounts`)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query account balance: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var balance types.Currency
		if err := rows.Scan((*sqlCurrency)(&balance)); err != nil {
			return fmt.Errorf("failed to scan account balance: %w", err)
		}
		accountBalance = accountBalance.Add(balance)
	}

	if err := setCurrencyStat(tx, metricAccountBalance, accountBalance, time.Now()); err != nil {
		return fmt.Errorf("failed to set account balance metric: %w", err)
	}

	return nil
}

// migrateVersion14 adds the locked_sectors table, recalculates the contract
// sectors metric, and recalculates the physical sectors metric.
func migrateVersion14(tx txn) error {
	// create the new locked sectors table
	const lockedSectorsTableQuery = `CREATE TABLE locked_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	sector_id INTEGER NOT NULL REFERENCES stored_sectors(id)
);
CREATE INDEX locked_sectors_sector_id ON locked_sectors(sector_id);`

	if _, err := tx.Exec(lockedSectorsTableQuery); err != nil {
		return fmt.Errorf("failed to create locked_sectors table: %w", err)
	}

	// recalculate the contract sectors metric
	var contractSectorCount int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM contract_sector_roots`).Scan(&contractSectorCount); err != nil {
		return fmt.Errorf("failed to query contract sector count: %w", err)
	} else if err := setNumericStat(tx, metricContractSectors, uint64(contractSectorCount), time.Now()); err != nil {
		return fmt.Errorf("failed to set contract sectors metric: %w", err)
	}

	// recalculate the physical sectors metric
	var physicalSectorsCount int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM volume_sectors WHERE sector_id IS NOT NULL`).Scan(&physicalSectorsCount); err != nil {
		return fmt.Errorf("failed to query contract sector count: %w", err)
	} else if err := setNumericStat(tx, metricPhysicalSectors, uint64(physicalSectorsCount), time.Now()); err != nil {
		return fmt.Errorf("failed to set contract sectors metric: %w", err)
	}
	return nil
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
	migrateVersion15,
	migrateVersion16,
	migrateVersion17,
	migrateVersion18,
	migrateVersion19,
	migrateVersion20,
	migrateVersion21,
	migrateVersion22,
}
