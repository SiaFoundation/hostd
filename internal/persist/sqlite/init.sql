/*
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
	sector_root BLOB UNIQUE NOT NULL
);
CREATE INDEX stored_sectors_sector_root ON stored_sectors(sector_root);

CREATE TABLE storage_volumes (
	id INTEGER PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	read_only BOOLEAN NOT NULL,
	available BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX storage_volumes_read_only_available ON storage_volumes(read_only, available);

CREATE TABLE volume_sectors (
	id INTEGER PRIMARY KEY,
	volume_id INTEGER NOT NULL REFERENCES storage_volumes (id), -- all sectors will need to be migrated first when deleting a volume
	volume_index INTEGER NOT NULL,
	sector_id INTEGER UNIQUE REFERENCES stored_sectors (id) ON DELETE SET NULL,
	UNIQUE (volume_id, volume_index)
);
CREATE INDEX volume_sectors_volume_id ON volume_sectors(volume_id);
CREATE INDEX volume_sectors_volume_index ON volume_sectors(volume_index ASC);
CREATE INDEX volume_sectors_sector_id ON volume_sectors(sector_id);

CREATE TABLE locked_volume_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	volume_sector_id INTEGER REFERENCES volume_sectors(id) ON DELETE CASCADE
);
CREATE INDEX locked_volume_sectors_sector_id ON locked_volume_sectors(volume_sector_id);

CREATE TABLE contracts (
	id INTEGER PRIMARY KEY,
	renewed_to INTEGER REFERENCES contracts(id) ON DELETE SET NULL,
	contract_id BLOB UNIQUE NOT NULL,
	revision_number BLOB NOT NULL, -- stored as BLOB to support uint64_max on clearing revisions
	formation_txn_set BLOB NOT NULL, -- binary serialized transaction set
	locked_collateral BLOB NOT NULL,
	rpc_revenue BLOB NOT NULL,
	storage_revenue BLOB NOT NULL,
	ingress_revenue BLOB NOT NULL,
	egress_revenue BLOB NOT NULL,
	account_funding BLOB NOT NULL,
	confirmed_revision_number BLOB, -- stored as BLOB to support uint64_max on clearing revisions
	host_sig BLOB NOT NULL,
	renter_sig BLOB NOT NULL,
	raw_revision BLOB NOT NULL, -- binary serialized contract revision
	contract_error TEXT,
	formation_confirmed BOOLEAN NOT NULL, -- true if the contract has been confirmed on the blockchain
	resolution_confirmed BOOLEAN NOT NULL, -- true if the storage proof/resolution has been confirmed on the blockchain
	negotiation_height INTEGER NOT NULL, -- determines if the formation txn should be rebroadcast or if the contract should be deleted
	window_start INTEGER NOT NULL,
	window_end INTEGER NOT NULL
);
CREATE INDEX contracts_contract_id ON contracts(contract_id);
CREATE INDEX contracts_renewed_to ON contracts(renewed_to);
CREATE INDEX contracts_formation_confirmed_resolution_confirmed_window_start ON contracts(formation_confirmed, resolution_confirmed, window_start);
CREATE INDEX contracts_formation_confirmed_resolution_confirmed_window_end ON contracts(formation_confirmed, resolution_confirmed, window_end);
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

CREATE TABLE accounts (
	id INTEGER PRIMARY KEY,
	account_id BLOB UNIQUE NOT NULL,
	balance BLOB NOT NULL,
	expiration_timestamp INTEGER NOT NULL
);
CREATE INDEX accounts_expiration_timestamp ON accounts(expiration_timestamp);

CREATE TABLE registry_entries (
	registry_key BLOB PRIMARY KEY,
	revision_number BLOB NOT NULL, -- stored as BLOB to support uint64_max
	entry_data BLOB NOT NULL,
	entry_signature BLOB NOT NULL,
	entry_type INTEGER NOT NULL,
	expiration_height INTEGER NOT NULL
);
CREATE INDEX registry_entries_expiration_height ON registry_entries(expiration_height);

CREATE TABLE financial_account_funding (
	source BLOB NOT NULL,
	destination BLOB NOT NULL,
	amount BLOB NOT NULL,
	reverted BOOLEAN NOT NULL,
	date_created INTEGER NOT NULL
);
CREATE INDEX financial_account_funding_source ON financial_account_funding(source);
CREATE INDEX financial_account_funding_reverted ON financial_account_funding(reverted);
CREATE INDEX financial_account_funding_date_created ON financial_account_funding(date_created);

CREATE TABLE financial_records (
	source_id BLOB NOT NULL,
	egress_revenue BLOB NOT NULL,
	ingress_revenue BLOB NOT NULL,
	storage_revenue BLOB NOT NULL,
	fee_revenue BLOB NOT NULL,
	reverted BOOLEAN NOT NULL,
	date_created INTEGER NOT NULL
);
CREATE INDEX financial_records_source_id ON financial_records(source_id);
CREATE INDEX financial_records_date_created ON financial_records(date_created);

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
	max_contract_duration INTEGER NOT NULL,
	ingress_limit INTEGER NOT NULL,
	egress_limit INTEGER NOT NULL,
	registry_limit INTEGER NOT NULL
);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	host_key BLOB, -- host key will eventually be stored instead of passed into the CLI, this will make migrating from siad easier
	wallet_last_processed_change BLOB, -- last processed consensus change for the wallet
	contracts_last_processed_change BLOB, -- last processed consensus change for the contract manager
	wallet_height INTEGER, -- height of the wallet as of the last processed change
	contracts_height INTEGER -- height of the contract manager as of the last processed change
);

INSERT INTO global_settings (id, db_version) VALUES (0, 1); -- version must be updated when the schema changes