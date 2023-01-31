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
	block_height INTEGER NOT NULL,
	block_index INTEGER NOT NULL,
	raw_data BLOB NOT NULL, -- binary serialized transaction
	date_created INTEGER NOT NULL,
	UNIQUE(block_height, block_index)
);
CREATE INDEX wallet_transactions_date_created_index ON wallet_transactions(date_created);

CREATE TABLE accounts (
	id TEXT PRIMARY KEY,
	balance TEXT NOT NULL,
	expiration_height INTEGER NOT NULL
);

CREATE TABLE storage_volumes (
	id INTEGER PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	writeable BOOLEAN NOT NULL
);

CREATE TABLE volume_sectors (
	id INTEGER PRIMARY KEY,
	volume_id INTEGER NOT NULL REFERENCES storage_volumes, -- all sectors will need to be migrated first when deleting a volume
	volume_index INTEGER NOT NULL,
	sector_root TEXT UNIQUE, -- set null if the sector is not used
	UNIQUE (volume_id, volume_index)
);
CREATE INDEX volume_sectors_volume_id ON volume_sectors(volume_id, volume_index);

CREATE TABLE locked_volume_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	volume_sector_id INTEGER REFERENCES volume_sectors(id) ON DELETE CASCADE
);

CREATE TABLE contracts (
	id TEXT PRIMARY KEY,
	renewed_from TEXT REFERENCES contracts ON DELETE SET NULL,
	renewed_to TEXT REFERENCES contracts ON DELETE SET NULL,
	locked_collateral TEXT NOT NULL,
	contract_error TEXT,
	revision_number TEXT NOT NULL, -- stored as text to support uint64_max on clearing revisions
	confirmed_revision_number TEXT DEFAULT '0', -- determines if the final revision should be broadcast; stored as text to support uint64_max on clearing revisions
	formation_confirmed BOOLEAN NOT NULL DEFAULT false, -- true if the contract has been confirmed on the blockchain
	resolution_confirmed BOOLEAN NOT NULL DEFAULT false, -- true if the storage proof/resolution has been confirmed on the blockchain
	negotiation_height INTEGER NOT NULL, -- determines if the formation txn should be rebroadcast or if the contract should be deleted
	window_start INTEGER NOT NULL,
	window_end INTEGER NOT NULL,
	formation_txn_set BLOB NOT NULL, -- binary serialized transaction set
	raw_revision BLOB NOT NULL, -- binary serialized contract revision
	host_sig BLOB NOT NULL,
	renter_sig BLOB NOT NULL
);
CREATE INDEX contracts_window_start_index ON contracts(window_start);
CREATE INDEX contracts_window_end_index ON contracts(window_end);

CREATE TABLE contract_sector_roots (
	id INTEGER PRIMARY KEY,
	contract_id TEXT REFERENCES contracts ON DELETE CASCADE,
	sector_root TEXT NOT NULL,
	root_index INTEGER NOT NULL,
	UNIQUE(contract_id, root_index)
);
CREATE INDEX contract_sector_roots_contract_id_root_index ON contract_sector_roots(contract_id, root_index);
CREATE INDEX contract_sector_roots_sector_root ON contract_sector_roots(sector_root);

CREATE TABLE temp_storage (
	sector_root TEXT PRIMARY KEY,
	expiration_height INTEGER NOT NULL
);

CREATE TABLE financial_account_funding (
	source TEXT NOT NULL,
	destination TEXT NOT NULL,
	amount TEXT NOT NULL,
	reverted BOOLEAN NOT NULL,
	date_created INTEGER NOT NULL
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
	reverted BOOLEAN NOT NULL,
	date_created INTEGER NOT NULL
);
CREATE INDEX financial_records_source_id ON financial_records(source_id);
CREATE INDEX financial_records_date_created ON financial_records(date_created);

CREATE TABLE host_settings (
	id INT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	settings_revision INTEGER NOT NULL DEFAULT 0,
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
	max_account_age INTEGER NOT NULL DEFAULT 0,
	max_contract_duration INTEGER NOT NULL DEFAULT 0,
	ingress_limit INTEGER NOT NULL DEFAULT 0,
	egress_limit INTEGER NOT NULL DEFAULT 0,
	last_processed_consensus_change BLOB NOT NULL DEFAULT ""
);

CREATE TABLE global_settings (
	id INT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL DEFAULT 0, -- used for migrations
	host_key TEXT NOT NULL DEFAULT "", -- host key will eventually be stored instead of passed into the CLI, this will make migrating from siad easier
	wallet_last_processed_change TEXT NOT NULL DEFAULT "", -- last processed consensus change for the wallet
	contracts_last_processed_change TEXT NOT NULL DEFAULT "" -- last processed consensus change for the contract manager
);

INSERT INTO global_settings (db_version) VALUES (1); -- version must be updated when the schema changes