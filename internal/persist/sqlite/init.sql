/*
	When changing the schema, the version must be incremented at the bottom of
	this file and a migration added to migrations.go
*/

CREATE TABLE wallet_utxos (
	id TEXT PRIMARY KEY,
	amount TEXT NOT NULL,
	unlock_hash TEXT NOT NULL
);

CREATE TABLE wallet_transactions (
	id TEXT PRIMARY KEY,
	source TEXT NOT NULL,
	block_id TEXT NOT NULL,
	inflow TEXT NOT NULL,
	outflow TEXT NOT NULL,
	block_height UNSIGNED BIG INT NOT NULL,
	block_index UNSIGNED BIG INT NOT NULL,
	raw_data BLOB NOT NULL, -- binary serialized transaction
	date_created UNSIGNED BIG INT NOT NULL,
	UNIQUE(block_height, block_index)
);
CREATE INDEX wallet_transactions_date_created_index ON wallet_transactions(date_created);

CREATE TABLE wallet_settings (
	id INT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	last_processed_change TEXT NOT NULL DEFAULT ""
);

CREATE TABLE accounts (
	id TEXT PRIMARY KEY,
	balance TEXT NOT NULL,
	expiration_height UNSIGNED BIG INT NOT NULL
);

CREATE TABLE storage_volumes (
	id TEXT PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	max_sectors UNSIGNED BIG INT NOT NULL,
	writeable BOOLEAN NOT NULL
);

CREATE TABLE volume_sectors (
	id INTEGER PRIMARY KEY,
	volume_id TEXT NOT NULL REFERENCES storage_volumes, -- on delete, all sectors will need to be migrated first
	volume_index UNSIGNED BIG INT NOT NULL,
	sector_root TEXT UNIQUE, -- set null if the sector is not used
	UNIQUE (volume_id, volume_index)
);
CREATE INDEX volume_sectors_volume_id ON volume_sectors(volume_id);
CREATE INDEX volume_sectors_volume_index ON volume_sectors(volume_index);

CREATE TABLE locked_volume_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	volume_sector_id INTEGER REFERENCES volume_sectors(id) ON DELETE CASCADE
);

CREATE TABLE contracts (
	id TEXT PRIMARY KEY,
	renewed_from TEXT REFERENCES contracts ON DELETE SET NULL,
	host_signature TEXT NOT NULL,
	renter_signature TEXT NOT NULL,
	locked_collateral TEXT NOT NULL,
	revision_number UNSIGNED BIG INT NOT NULL,
	start_height UNISGNED BIG INT NOT NULL,
	window_start UNSIGNED BIG INT NOT NULL,
	window_end UNSIGNED BIG INT NOT NULL,
	final_revision_confirmed BOOLEAN NOT NULL,
	proof_confirmed BOOLEAN NOT NULL,
	raw_revision BLOB NOT NULL -- binary serialized contract revision
);

CREATE INDEX contracts_window_start_index ON contracts(window_start);
CREATE INDEX contracts_window_end_index ON contracts(window_end);

CREATE TABLE contract_sector_roots (
	contract_id TEXT REFERENCES contracts ON DELETE CASCADE,
	sector_root TEXT NOT NULL,
	root_order UNSIGNED BIG INT NOT NULL,
	UNIQUE(contract_id, root_order)
);
CREATE INDEX contract_sector_roots_contract_id_index ON contract_sector_roots(contract_id);
CREATE INDEX contract_sector_roots_sector_root ON contract_sector_roots(sector_root);

CREATE TABLE temp_storage (
	sector_root TEXT PRIMARY KEY,
	expiration_height UNSIGNED BIG INT NOT NULL
);

CREATE TABLE financial_account_funding (
	source TEXT NOT NULL,
	destination TEXT NOT NULL,
	amount TEXT NOT NULL,
	reverted BOOLEAN NOT NULL,
	date_created UNSIGNED BIG INT NOT NULL
);
CREATE INDEX financial_account_funding_source ON financial_account_funding(source);
CREATE INDEX financial_account_funding_reverted ON financial_account_funding(reverted);
CREATE INDEX financial_account_funding_date_created ON financial_account_funding(date_created);

CREATE TABLE financial_records (
	source_id TEXT NOT NULL,
	egress_revenue TEXT NOT NULL,
	ingress_revenue TEXT NOT NULL,
	storage_revenue TEXT NOT NULL,
	fee_revenue TEXT NOT NULL,
	date_created UNSIGNED BIG INT NOT NULL
);
CREATE INDEX financial_records_source_id ON financial_records(source_id);
CREATE INDEX financial_records_date_created ON financial_records(date_created);

CREATE TABLE host_settings (
	id INT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	settings_revision UNSIGNED BIG INT NOT NULL DEFAULT 0,
	accepting_contracts BOOLEAN NOT NULL DEFAULT false,
	net_address TEXT NOT NULL DEFAULT "",
	contract_price TEXT NOT NULL DEFAULT "0",
	base_rpc_price TEXT NOT NULL DEFAULT "0",
	sector_access_price TEXT NOT NULL DEFAULT "0",
	collateral TEXT NOT NULL DEFAULT "0",
	max_collateral TEXT NOT NULL DEFAULT "0",
	min_storage_price TEXT NOT NULL DEFAULT "0",
	min_egress_price TEXT NOT NULL DEFAULT "0",
	min_ingress_price TEXT NOT NULL DEFAULT "0",
	max_account_balance TEXT NOT NULL DEFAULT "0",
	max_account_age UNSIGNED BIG INT NOT NULL DEFAULT 0,
	max_contract_duration UNSIGNED BIG INT NOT NULL DEFAULT 0,
	ingress_limit UNSIGNED BIG INT NOT NULL DEFAULT 0,
	egress_limit UNSIGNED BIG INT NOT NULL DEFAULT 0,
	last_processed_consensus_change BLOB NOT NULL DEFAULT ""
);

CREATE TABLE global_settings (
	id INT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version UNSIGNED BIG INT NOT NULL DEFAULT 0, -- used for migrations
	host_key TEXT NOT NULL DEFAULT "" -- host key will eventually be stored instead of passed into the CLI, this will make migrating from siad easier
);

INSERT INTO global_settings (db_version) VALUES (1); -- version must be updated when the schema changes