package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// nolint:misspell
const initialSchema = `/*
	When changing the schema, the version must be incremented at the bottom of
	this file and a migration added to migrations.go
*/

CREATE TABLE wallet_utxos (
	id BLOB PRIMARY KEY,
	amount BLOB NOT NULL,
	unlock_hash BLOB NOT NULL
);

CREATE TABLE wallet_transactions (
	id INTEGER PRIMARY KEY,
	transaction_id BLOB NOT NULL,
	block_id BLOB NOT NULL,
	inflow BLOB NOT NULL,
	outflow BLOB NOT NULL,
	raw_transaction BLOB NOT NULL, -- binary serialized transaction
	source TEXT NOT NULL,
	block_height INTEGER NOT NULL,
	date_created INTEGER NOT NULL
);
CREATE INDEX wallet_transactions_date_created_index ON wallet_transactions(date_created);
CREATE INDEX wallet_transactions_block_id ON wallet_transactions(block_id);
CREATE INDEX wallet_transactions_date_created ON wallet_transactions(date_created);
CREATE INDEX wallet_transactions_block_height_id ON wallet_transactions(block_height DESC, id);

CREATE TABLE stored_sectors (
	id INTEGER PRIMARY KEY,
	sector_root BLOB UNIQUE NOT NULL,
	last_access_timestamp INTEGER NOT NULL
);
CREATE INDEX stored_sectors_sector_root ON stored_sectors(sector_root);
CREATE INDEX stored_sectors_last_access ON stored_sectors(last_access_timestamp);

CREATE TABLE storage_volumes (
	id INTEGER PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	used_sectors INTEGER NOT NULL,
	total_sectors INTEGER NOT NULL,
	read_only BOOLEAN NOT NULL,
	available BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX storage_volumes_read_only_available ON storage_volumes(read_only, available);

CREATE TABLE volume_sectors (
	id INTEGER PRIMARY KEY,
	volume_id INTEGER NOT NULL REFERENCES storage_volumes (id), -- all sectors will need to be migrated first when deleting a volume
	volume_index INTEGER NOT NULL,
	sector_id INTEGER UNIQUE REFERENCES stored_sectors (id),
	UNIQUE (volume_id, volume_index)
);
-- careful with these indices, the empty sector query is fragile and relies on
-- the volume_index indice for performance.
CREATE INDEX volume_sectors_volume_id ON volume_sectors(volume_id);
CREATE INDEX volume_sectors_volume_index ON volume_sectors(volume_index ASC);
CREATE INDEX volume_sectors_sector_id ON volume_sectors(sector_id);

CREATE TABLE locked_volume_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	volume_sector_id INTEGER REFERENCES volume_sectors(id) ON DELETE CASCADE
);
CREATE INDEX locked_volume_sectors_sector_id ON locked_volume_sectors(volume_sector_id);

CREATE TABLE contract_renters (
	id INTEGER PRIMARY KEY,
	public_key BLOB UNIQUE NOT NULL
);

CREATE TABLE contracts (
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
CREATE INDEX contracts_contract_id ON contracts(contract_id);
CREATE INDEX contracts_renter_id ON contracts(renter_id);
CREATE INDEX contracts_renewed_to ON contracts(renewed_to);
CREATE INDEX contracts_renewed_from ON contracts(renewed_from);
CREATE INDEX contracts_negotiation_height ON contracts(negotiation_height);
CREATE INDEX contracts_window_start ON contracts(window_start);
CREATE INDEX contracts_contract_status ON contracts(contract_status);
CREATE INDEX contracts_formation_confirmed_resolution_height_window_start ON contracts(formation_confirmed, resolution_height, window_start);
CREATE INDEX contracts_formation_confirmed_resolution_height_window_end ON contracts(formation_confirmed, resolution_height, window_end);
CREATE INDEX contracts_formation_confirmed_window_start ON contracts(formation_confirmed, window_start);
CREATE INDEX contracts_formation_confirmed_negotation_height ON contracts(formation_confirmed, negotiation_height);

CREATE TABLE contract_sector_roots (
	id INTEGER PRIMARY KEY,
	contract_id INTEGER NOT NULl REFERENCES contracts(id),
	sector_id INTEGER NOT NULL REFERENCES stored_sectors(id),
	root_index INTEGER NOT NULL,
	UNIQUE(contract_id, root_index)
);
CREATE INDEX contract_sector_roots_sector_id ON contract_sector_roots(sector_id);
CREATE INDEX contract_sector_roots_contract_id_root_index ON contract_sector_roots(contract_id, root_index);

CREATE TABLE temp_storage_sector_roots (
	id INTEGER PRIMARY KEY,
	sector_id INTEGER NOT NULL REFERENCES stored_sectors(id),
	expiration_height INTEGER NOT NULL
);
CREATE INDEX temp_storage_sector_roots_sector_id ON temp_storage_sector_roots(sector_id);
CREATE INDEX temp_storage_sector_roots_expiration_height ON temp_storage_sector_roots(expiration_height);

CREATE TABLE registry_entries (
	registry_key BLOB PRIMARY KEY,
	revision_number BLOB NOT NULL, -- stored as BLOB to support uint64_max
	entry_data BLOB NOT NULL,
	entry_signature BLOB NOT NULL,
	entry_type INTEGER NOT NULL,
	expiration_height INTEGER NOT NULL
);
CREATE INDEX registry_entries_expiration_height ON registry_entries(expiration_height);

CREATE TABLE accounts (
	id INTEGER PRIMARY KEY,
	account_id BLOB UNIQUE NOT NULL,
	balance BLOB NOT NULL,
	expiration_timestamp INTEGER NOT NULL
);
CREATE INDEX accounts_expiration_timestamp ON accounts(expiration_timestamp);

CREATE TABLE account_financial_records (
	id INTEGER PRIMARY KEY,
	account_id INTEGER NOT NULL REFERENCES accounts(id),
	rpc_revenue BLOB NOT NULL,
	storage_revenue BLOB NOT NULL,
	ingress_revenue BLOB NOT NULL,
	egress_revenue BLOB NOT NULL,
	date_created INTEGER UNIQUE NOT NULL -- unique to limit growth
);
CREATE INDEX account_financial_records_account_id ON account_financial_records(account_id);
CREATE INDEX account_financial_records_date_created ON account_financial_records(date_created);

CREATE TABLE contract_financial_records (
	id INTEGER PRIMARY KEY,
	contract_id INTEGER NOT NULL REFERENCES contracts(id),
	rpc_revenue BLOB NOT NULL,
	storage_revenue BLOB NOT NULL,
	ingress_revenue BLOB NOT NULL,
	egress_revenue BLOB NOT NULL,
	date_created INTEGER UNIQUE NOT NULL -- unique to limit growth
);
CREATE INDEX contract_financial_records_contract_id ON contract_financial_records(contract_id);
CREATE INDEX contract_financial_records_date_created ON contract_financial_records(date_created);

CREATE TABLE contract_account_funding ( -- tracks the funding of accounts from contracts, necessary to reduce account revenue during reorgs
	id INTEGER PRIMARY KEY,
	contract_id INTEGER NOT NULL REFERENCES contracts(id),
	account_id INTEGER NOT NULL REFERENCES accounts(id),
	amount BLOB NOT NULL,
	date_created INTEGER NOT NULL
);

CREATE TABLE host_stats (
	date_created INTEGER NOT NULL,
	stat TEXT NOT NULL,
	stat_value BLOB NOT NULL,
	PRIMARY KEY(date_created, stat)
);
CREATE INDEX host_stats_stat_date_created ON host_stats(stat, date_created DESC);

CREATE TABLE host_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	settings_revision INTEGER NOT NULL,
	accepting_contracts BOOLEAN NOT NULL,
	net_address TEXT NOT NULL,
	contract_price BLOB NOT NULL,
	base_rpc_price BLOB NOT NULL,
	sector_access_price BLOB NOT NULL,
	collateral BLOB NOT NULL,
	max_collateral BLOB NOT NULL,
	min_storage_price BLOB NOT NULL,
	min_egress_price BLOB NOT NULL,
	min_ingress_price BLOB NOT NULL,
	max_account_balance BLOB NOT NULL,
	max_account_age INTEGER NOT NULL,
	price_table_validity INTEGER NOT NULL,
	max_contract_duration INTEGER NOT NULL,
	window_size INTEGER NOT NULL,
	ingress_limit INTEGER NOT NULL,
	egress_limit INTEGER NOT NULL,
	dyn_dns_provider TEXT NOT NULL,
	dns_update_v4 BOOLEAN NOT NULL,
	dns_update_v6 BOOLEAN NOT NULL,
	dyn_dns_opts BLOB,
	registry_limit INTEGER NOT NULL
);

CREATE TABLE log_lines (
	id INTEGER PRIMARY KEY,
	date_created INTEGER NOT NULL,
	log_level INTEGER NOT NULL,
	log_name TEXT NOT NULL,
	log_caller TEXT NOT NULL,
	log_message TEXT NOT NULL,
	log_fields BLOB NOT NULL
);
CREATE INDEX log_lines_date_created ON log_lines(date_created DESC);
CREATE INDEX log_lines_log_level ON log_lines(log_level);
CREATE INDEX log_lines_log_name ON log_lines(log_name);
CREATE INDEX log_lines_log_caller ON log_lines(log_caller);
CREATE INDEX log_lines_log_message ON log_lines(log_message);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	host_key BLOB, 
	wallet_last_processed_change BLOB, -- last processed consensus change for the wallet
	contracts_last_processed_change BLOB, -- last processed consensus change for the contract manager
	wallet_height INTEGER, -- height of the wallet as of the last processed change
	contracts_height INTEGER -- height of the contract manager as of the last processed change
);

INSERT INTO global_settings (id, db_version) VALUES (0, 1); -- version must be updated when the schema changes`

func TestMigrationConsistency(t *testing.T) {
	t.Skip("don't let this make it into production")

	fp := filepath.Join(t.TempDir(), "hostd.sqlite3")
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(initialSchema); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	expectedVersion := int64(len(migrations) + 1)
	log := zaptest.NewLogger(t)
	store, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	v := getDBVersion(store.db)
	if v != expectedVersion {
		t.Fatalf("expected version %d, got %d", expectedVersion, v)
	} else if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// ensure the database does not change version when opened again
	store, err = OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	v = getDBVersion(store.db)
	if v != expectedVersion {
		t.Fatalf("expected version %d, got %d", expectedVersion, v)
	}

	fp2 := filepath.Join(t.TempDir(), "hostd.sqlite3")
	baseline, err := OpenDatabase(fp2, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer baseline.Close()

	getTableIndices := func(db *sql.DB) (map[string]bool, error) {
		const query = `SELECT name, tbl_name, sql FROM sqlite_schema WHERE type='index'`
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		indices := make(map[string]bool)
		for rows.Next() {
			var name, table string
			var sqlStr sql.NullString // auto indices have no sql
			if err := rows.Scan(&name, &table, &sqlStr); err != nil {
				return nil, err
			}
			indices[fmt.Sprintf("%s.%s.%s", name, table, sqlStr.String)] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return indices, nil
	}

	// ensure the migrated database has the same indices as the baseline
	baselineIndices, err := getTableIndices(baseline.db)
	if err != nil {
		t.Fatal(err)
	}

	migratedIndices, err := getTableIndices(store.db)
	if err != nil {
		t.Fatal(err)
	}

	for k := range baselineIndices {
		if !migratedIndices[k] {
			t.Errorf("missing index %s", k)
		}
	}

	for k := range migratedIndices {
		if !baselineIndices[k] {
			t.Errorf("unexpected index %s", k)
		}
	}

	getTables := func(db *sql.DB) (map[string]bool, error) {
		const query = `SELECT name FROM sqlite_schema WHERE type='table'`
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		tables := make(map[string]bool)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			tables[name] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return tables, nil
	}

	// ensure the migrated database has the same tables as the baseline
	baselineTables, err := getTables(baseline.db)
	if err != nil {
		t.Fatal(err)
	}

	migratedTables, err := getTables(store.db)
	if err != nil {
		t.Fatal(err)
	}

	for k := range baselineTables {
		if !migratedTables[k] {
			t.Errorf("missing table %s", k)
		}
	}
	for k := range migratedTables {
		if !baselineTables[k] {
			t.Errorf("unexpected table %s", k)
		}
	}

	// ensure each table has the same columns as the baseline
	getTableColumns := func(db *sql.DB, table string) (map[string]bool, error) {
		query := fmt.Sprintf(`PRAGMA table_info(%s)`, table) // cannot use parameterized query for PRAGMA statements
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		columns := make(map[string]bool)
		for rows.Next() {
			var cid int
			var name, colType string
			var defaultValue sql.NullString
			var notNull bool
			var primaryKey int // composite keys are indices
			if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &primaryKey); err != nil {
				return nil, err
			}
			// column ID is ignored since it may not match between the baseline and migrated databases
			key := fmt.Sprintf("%s.%s.%s.%t.%d", name, colType, defaultValue.String, notNull, primaryKey)
			columns[key] = true
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return columns, nil
	}

	for k := range baselineTables {
		baselineColumns, err := getTableColumns(baseline.db, k)
		if err != nil {
			t.Fatal(err)
		}
		migratedColumns, err := getTableColumns(store.db, k)
		if err != nil {
			t.Fatal(err)
		}

		for c := range baselineColumns {
			if !migratedColumns[c] {
				t.Errorf("missing column %s.%s", k, c)
			}
		}

		for c := range migratedColumns {
			if !baselineColumns[c] {
				t.Errorf("unexpected column %s.%s", k, c)
			}
		}
	}
}
