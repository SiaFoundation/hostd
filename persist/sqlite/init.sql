/*
	When changing the schema, a new migration function must be added to
	migrations.go
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

CREATE TABLE locked_sectors ( -- should be cleared at startup. currently persisted for simplicity, but may be moved to memory
	id INTEGER PRIMARY KEY,
	sector_id INTEGER NOT NULL REFERENCES stored_sectors(id)
);
CREATE INDEX locked_sectors_sector_id ON locked_sectors(sector_id);

CREATE TABLE storage_volumes (
	id INTEGER PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	used_sectors INTEGER NOT NULL,
	total_sectors INTEGER NOT NULL,
	read_only BOOLEAN NOT NULL,
	available BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX storage_volumes_id_available_read_only ON storage_volumes(id, available, read_only);
CREATE INDEX storage_volumes_read_only_available_used_sectors ON storage_volumes(available, read_only, used_sectors);

CREATE TABLE volume_sectors (
	id INTEGER PRIMARY KEY,
	volume_id INTEGER NOT NULL REFERENCES storage_volumes (id), -- all sectors will need to be migrated first when deleting a volume
	volume_index INTEGER NOT NULL,
	sector_id INTEGER UNIQUE REFERENCES stored_sectors (id),
	UNIQUE (volume_id, volume_index)
);
CREATE INDEX volume_sectors_volume_id_sector_id_volume_index_compound ON volume_sectors(volume_id, sector_id, volume_index) WHERE sector_id IS NULL;
CREATE INDEX volume_sectors_volume_id_sector_id_volume_index_set_compound ON volume_sectors (volume_id, sector_id, volume_index) WHERE sector_id IS NOT NULL;
CREATE INDEX volume_sectors_volume_id_sector_id ON volume_sectors(volume_id, sector_id);
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
CREATE INDEX contracts_formation_confirmed_negotiation_height ON contracts(formation_confirmed, negotiation_height);

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

CREATE TABLE contract_account_funding (
	id INTEGER PRIMARY KEY,
	contract_id INTEGER NOT NULL REFERENCES contracts(id),
	account_id INTEGER NOT NULL REFERENCES accounts(id),
	amount BLOB NOT NULL,
	UNIQUE (contract_id, account_id)
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
	registry_limit INTEGER NOT NULL,
	sector_cache_size INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE webhooks (
	id INTEGER PRIMARY KEY,
	callback_url TEXT UNIQUE NOT NULL,
	scopes TEXT NOT NULL,
	secret_key TEXT UNIQUE NOT NULL
);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	host_key BLOB,
	last_announce_key BLOB, -- public key of the last host announcement
	wallet_hash BLOB, -- used to prevent wallet seed changes
	wallet_last_processed_change BLOB, -- last processed consensus change for the wallet
	contracts_last_processed_change BLOB, -- last processed consensus change for the contract manager
	settings_last_processed_change BLOG, -- last processed consensus change for the settings manager
	last_announce_id BLOB, -- chain index of the last host announcement
	last_announce_height INTEGER, -- height of the last host announcement
	wallet_height INTEGER, -- height of the wallet as of the last processed change
	contracts_height INTEGER, -- height of the contract manager as of the last processed change
	settings_height INTEGER, -- height of the settings manager as of the last processed change
	last_announce_address TEXT -- address of the last host announcement
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed
