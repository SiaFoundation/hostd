package postgres

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// initialSchema is a frozen copy of the version 1 schema (init.sql at the time
// the first migration was introduced). It is used by TestMigrationConsistency to
// build a database at the oldest supported version so that the migration path up
// to the current schema can be exercised and compared against a database
// initialized directly from init.sql.
//
// IMPORTANT: this must NOT be updated when init.sql changes. Schema changes are
// applied via migrations in migrations.go; this constant preserves the starting
// point those migrations run against.
const initialSchema = `
CREATE TABLE wallet_siacoin_elements (
	id BYTEA PRIMARY KEY CHECK (LENGTH(id) = 32),
	siacoin_value NUMERIC(50,0) NOT NULL,
	sia_address BYTEA NOT NULL CHECK (LENGTH(sia_address) = 32),
	merkle_proof BYTEA NOT NULL, -- binary serialized []types.Hash256
	leaf_index BIGINT NOT NULL,
	maturity_height BIGINT NOT NULL
);

CREATE TABLE wallet_broadcasted_txnsets (
	id BYTEA PRIMARY KEY CHECK (LENGTH(id) = 32),
	basis BYTEA NOT NULL CHECK (LENGTH(basis) = 8+32), -- binary serialized chain index
	raw_transactions BYTEA NOT NULL, -- binary serialized transaction set
	date_created TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX wallet_broadcasted_txnsets_date_created ON wallet_broadcasted_txnsets(date_created);

CREATE TABLE wallet_events (
	id BYTEA PRIMARY KEY CHECK (LENGTH(id) = 32),
	chain_index BYTEA NOT NULL CHECK (LENGTH(chain_index) = 8+32),
	maturity_height BIGINT NOT NULL,
	event_type TEXT NOT NULL,
	raw_data BYTEA NOT NULL
);
CREATE INDEX wallet_events_chain_index ON wallet_events(chain_index);
CREATE INDEX wallet_events_maturity_height ON wallet_events(maturity_height DESC);

CREATE TABLE stored_sectors (
	id BIGSERIAL PRIMARY KEY,
	sector_root BYTEA UNIQUE NOT NULL CHECK (LENGTH(sector_root) = 32),
	cached_subtree_roots BYTEA,
	last_access_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX stored_sectors_sector_root ON stored_sectors(sector_root);
CREATE INDEX stored_sectors_last_access ON stored_sectors(last_access_timestamp);

CREATE TABLE storage_volumes (
	id BIGSERIAL PRIMARY KEY,
	disk_path TEXT UNIQUE NOT NULL,
	used_sectors BIGINT NOT NULL,
	total_sectors BIGINT NOT NULL,
	read_only BOOLEAN NOT NULL,
	available BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX storage_volumes_id_available_read_only ON storage_volumes(id, available, read_only);
CREATE INDEX storage_volumes_read_only_available_used_sectors ON storage_volumes(available, read_only, used_sectors);

CREATE TABLE volume_sectors (
	id BIGSERIAL PRIMARY KEY,
	volume_id BIGINT NOT NULL REFERENCES storage_volumes (id), -- all sectors will need to be migrated first when deleting a volume
	volume_index BIGINT NOT NULL,
	sector_id BIGINT UNIQUE REFERENCES stored_sectors (id),
	sector_writes BIGINT NOT NULL DEFAULT 0,
	UNIQUE (volume_id, volume_index)
);
CREATE INDEX volume_sectors_sector_writes_volume_id_sector_id_volume_index_compound ON volume_sectors(sector_writes ASC, volume_id, sector_id, volume_index) WHERE sector_id IS NULL;
CREATE INDEX volume_sectors_volume_id_sector_id ON volume_sectors(volume_id, sector_id);
CREATE INDEX volume_sectors_volume_id ON volume_sectors(volume_id);
CREATE INDEX volume_sectors_volume_index ON volume_sectors(volume_index ASC);
CREATE INDEX volume_sectors_sector_id ON volume_sectors(sector_id);

CREATE TABLE contract_renters (
	id BIGSERIAL PRIMARY KEY,
	public_key BYTEA UNIQUE NOT NULL CHECK (LENGTH(public_key) = 32)
);

CREATE TABLE contracts (
	id BIGSERIAL PRIMARY KEY,
	renter_id BIGINT NOT NULL REFERENCES contract_renters(id),
	renewed_to BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
	renewed_from BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
	contract_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(contract_id) = 32),
	revision_number NUMERIC(20,0) NOT NULL, -- uint64, supports uint64_max on clearing revisions
	formation_txn_set BYTEA NOT NULL, -- binary serialized transaction set
	locked_collateral NUMERIC(50,0) NOT NULL,
	rpc_revenue NUMERIC(50,0) NOT NULL,
	storage_revenue NUMERIC(50,0) NOT NULL,
	ingress_revenue NUMERIC(50,0) NOT NULL,
	egress_revenue NUMERIC(50,0) NOT NULL,
	account_funding NUMERIC(50,0) NOT NULL,
	registry_read NUMERIC(50,0) NOT NULL,
	registry_write NUMERIC(50,0) NOT NULL,
	risked_collateral NUMERIC(50,0) NOT NULL,
	confirmed_revision_number NUMERIC(20,0), -- uint64, supports uint64_max on clearing revisions
	host_sig BYTEA NOT NULL CHECK (LENGTH(host_sig) = 64),
	renter_sig BYTEA NOT NULL CHECK (LENGTH(renter_sig) = 64),
	raw_revision BYTEA NOT NULL, -- binary serialized contract revision
	formation_confirmed BOOLEAN NOT NULL, -- true if the contract has been confirmed on the blockchain
	resolution_height BIGINT, -- null if the storage proof/resolution has not been confirmed on the blockchain, otherwise the height of the block containing the storage proof/resolution
	negotiation_height BIGINT NOT NULL, -- determines if the formation txn should be rebroadcast or if the contract should be deleted
	window_start BIGINT NOT NULL,
	window_end BIGINT NOT NULL,
	contract_status SMALLINT NOT NULL
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
	id BIGSERIAL PRIMARY KEY,
	contract_id BIGINT NOT NULL REFERENCES contracts(id),
	sector_id BIGINT NOT NULL REFERENCES stored_sectors(id),
	root_index BIGINT NOT NULL,
	UNIQUE(contract_id, root_index)
);
CREATE INDEX contract_sector_roots_sector_id ON contract_sector_roots(sector_id);
CREATE INDEX contract_sector_roots_contract_id_root_index ON contract_sector_roots(contract_id, root_index);

CREATE TABLE contract_v2_roots_map (
	id BIGINT NOT NULL,
	revision_number BIGINT NOT NULL,
	PRIMARY KEY (id, revision_number)
);

CREATE TABLE contracts_v2 (
	id BIGSERIAL PRIMARY KEY,
	renter_id BIGINT NOT NULL REFERENCES contract_renters(id),
	renewed_to BIGINT REFERENCES contracts_v2(id) ON DELETE SET NULL,
	renewed_from BIGINT REFERENCES contracts_v2(id) ON DELETE SET NULL,
	contract_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(contract_id) = 32),
	revision_number NUMERIC(20,0) NOT NULL, -- uint64, supports uint64_max on clearing revisions
	formation_txn_set BYTEA NOT NULL, -- binary serialized transaction set
	formation_txn_set_basis BYTEA NOT NULL CHECK (LENGTH(formation_txn_set_basis) = 8+32),
	locked_collateral NUMERIC(50,0) NOT NULL,
	rpc_revenue NUMERIC(50,0) NOT NULL,
	storage_revenue NUMERIC(50,0) NOT NULL,
	ingress_revenue NUMERIC(50,0) NOT NULL,
	egress_revenue NUMERIC(50,0) NOT NULL,
	account_funding NUMERIC(50,0) NOT NULL,
	risked_collateral NUMERIC(50,0) NOT NULL,
	raw_revision BYTEA NOT NULL, -- binary serialized contract revision
	confirmation_index BYTEA CHECK (LENGTH(confirmation_index) = 8+32), -- null if the contract has not been confirmed on the blockchain, otherwise the chain index of the block containing the confirmation transaction
	negotiation_height BIGINT NOT NULL, -- determines if the formation txn should be rebroadcast or if the contract should be deleted
	proof_height BIGINT NOT NULL,
	expiration_height BIGINT NOT NULL,
	resolution_block_id BYTEA CHECK (LENGTH(resolution_block_id) = 32), -- null if the resolution has not been confirmed on the blockchain
	resolution_height BIGINT CHECK((resolution_height IS NULL) = (resolution_block_id IS NULL)), -- null if the resolution has not been confirmed on the blockchain
	contract_status TEXT NOT NULL,
	sector_count BIGINT NOT NULL, -- used for cleanup

	contract_v2_roots_map_id BIGINT NOT NULL,
	contract_v2_roots_map_revision_number BIGINT NOT NULL,
	FOREIGN KEY (contract_v2_roots_map_id, contract_v2_roots_map_revision_number) REFERENCES contract_v2_roots_map(id, revision_number)
);
CREATE INDEX contracts_v2_contract_id ON contracts_v2(contract_id);
CREATE INDEX contracts_v2_renter_id ON contracts_v2(renter_id);
CREATE INDEX contracts_v2_renewed_to ON contracts_v2(renewed_to);
CREATE INDEX contracts_v2_renewed_from ON contracts_v2(renewed_from);
CREATE INDEX contracts_v2_negotiation_height ON contracts_v2(negotiation_height);
CREATE INDEX contracts_v2_proof_height ON contracts_v2(proof_height);
CREATE INDEX contracts_v2_expiration_height ON contracts_v2(expiration_height);
CREATE INDEX contracts_v2_contract_status ON contracts_v2(contract_status);
CREATE INDEX contracts_v2_confirmation_index_resolution_block_id_proof_height ON contracts_v2(confirmation_index, resolution_block_id, proof_height);
CREATE INDEX contracts_v2_confirmation_index_resolution_block_id_expiration_height ON contracts_v2(confirmation_index, resolution_block_id, expiration_height);
CREATE INDEX contracts_v2_resolution_height ON contracts_v2(resolution_height);
CREATE INDEX contracts_v2_confirmation_index_proof_height ON contracts_v2(confirmation_index, proof_height);
CREATE INDEX contracts_v2_confirmation_index_negotiation_height ON contracts_v2(confirmation_index, negotiation_height);
CREATE INDEX contracts_v2_roots_map_id_contract_v2_roots_map_revision_number ON contracts_v2(contract_v2_roots_map_id, contract_v2_roots_map_revision_number);

CREATE TABLE contract_v2_state_elements (
	contract_id BIGINT PRIMARY KEY REFERENCES contracts_v2(id),
	leaf_index BIGINT NOT NULL,
	merkle_proof BYTEA NOT NULL,
	raw_contract BYTEA NOT NULL, -- binary serialized contract
	revision_number NUMERIC(20,0) NOT NULL -- for comparison
);

CREATE TABLE contracts_v2_chain_index_elements (
	id BYTEA PRIMARY KEY CHECK (LENGTH(id) = 32),
	height BIGINT NOT NULL,
	leaf_index BIGINT NOT NULL,
	merkle_proof BYTEA NOT NULL
);
CREATE INDEX contracts_v2_chain_index_elements_height ON contracts_v2_chain_index_elements(height);

CREATE TABLE contract_v2_sector_roots (
	id BIGSERIAL PRIMARY KEY,
	sector_id BIGINT NOT NULL REFERENCES stored_sectors(id),
	root_index BIGINT NOT NULL,
	contract_v2_roots_map_id BIGINT NOT NULL,
	contract_v2_roots_map_revision_number BIGINT NOT NULL,
	FOREIGN KEY (contract_v2_roots_map_id, contract_v2_roots_map_revision_number) REFERENCES contract_v2_roots_map(id, revision_number),
	UNIQUE(contract_v2_roots_map_id, contract_v2_roots_map_revision_number, root_index)
);
CREATE INDEX contract_v2_sector_roots_map_id_root_index_revision_number ON contract_v2_sector_roots(contract_v2_roots_map_id, root_index, contract_v2_roots_map_revision_number);
CREATE INDEX contract_v2_sector_roots_sector_id ON contract_v2_sector_roots(sector_id);

CREATE TABLE temp_storage_sector_roots (
	id BIGSERIAL PRIMARY KEY,
	sector_id BIGINT NOT NULL REFERENCES stored_sectors(id),
	expiration_height BIGINT NOT NULL
);
CREATE INDEX temp_storage_sector_roots_sector_id ON temp_storage_sector_roots(sector_id);
CREATE INDEX temp_storage_sector_roots_expiration_height ON temp_storage_sector_roots(expiration_height);

CREATE TABLE registry_entries (
	registry_key BYTEA PRIMARY KEY CHECK (LENGTH(registry_key) = 32),
	revision_number NUMERIC(20,0) NOT NULL, -- uint64, supports uint64_max
	entry_data BYTEA NOT NULL,
	entry_signature BYTEA NOT NULL CHECK (LENGTH(entry_signature) = 64),
	entry_type SMALLINT NOT NULL,
	expiration_height BIGINT NOT NULL
);
CREATE INDEX registry_entries_expiration_height ON registry_entries(expiration_height);

CREATE TABLE accounts (
	id BIGSERIAL PRIMARY KEY,
	account_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(account_id) = 32),
	balance NUMERIC(50,0) NOT NULL,
	expiration_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX accounts_expiration_timestamp ON accounts(expiration_timestamp);

CREATE TABLE contract_account_funding (
	id BIGSERIAL PRIMARY KEY,
	contract_id BIGINT NOT NULL REFERENCES contracts(id),
	account_id BIGINT NOT NULL REFERENCES accounts(id),
	amount NUMERIC(50,0) NOT NULL,
	UNIQUE (contract_id, account_id)
);

CREATE TABLE contract_v2_account_funding (
	id BIGSERIAL PRIMARY KEY,
	contract_id BIGINT NOT NULL REFERENCES contracts_v2(id),
	account_id BIGINT NOT NULL REFERENCES accounts(id),
	amount NUMERIC(50,0) NOT NULL,
	UNIQUE (contract_id, account_id)
);

CREATE TABLE rhp4_pools (
	id BIGSERIAL PRIMARY KEY,
	pool_id BYTEA UNIQUE NOT NULL CHECK (LENGTH(pool_id) = 32),
	balance NUMERIC(50,0) NOT NULL
);

CREATE TABLE contract_v2_pool_funding (
	id BIGSERIAL PRIMARY KEY,
	contract_id BIGINT NOT NULL REFERENCES contracts_v2(id),
	pool_id BIGINT NOT NULL REFERENCES rhp4_pools(id),
	amount NUMERIC(50,0) NOT NULL,
	UNIQUE (contract_id, pool_id)
);
CREATE INDEX contract_v2_pool_funding_pool_id ON contract_v2_pool_funding(pool_id);

CREATE TABLE rhp4_account_pool_attachments (
	id BIGSERIAL PRIMARY KEY,
	account_id BIGINT NOT NULL REFERENCES accounts(id),
	pool_id BIGINT NOT NULL REFERENCES rhp4_pools(id),
	UNIQUE (account_id, pool_id)
);
CREATE INDEX rhp4_account_pool_attachments_account_id_id ON rhp4_account_pool_attachments(account_id, id);
CREATE INDEX rhp4_account_pool_attachments_pool_id ON rhp4_account_pool_attachments(pool_id);

CREATE TABLE host_stats (
	date_created TIMESTAMP WITH TIME ZONE NOT NULL,
	stat TEXT NOT NULL,
	stat_value BYTEA NOT NULL, -- polymorphic: currency, uint64, or float64 bits depending on stat
	PRIMARY KEY(date_created, stat)
);
CREATE INDEX host_stats_stat_date_created ON host_stats(stat, date_created DESC);

CREATE TABLE host_settings (
	id BIGINT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	settings_revision BIGINT NOT NULL,
	accepting_contracts BOOLEAN NOT NULL,
	net_address TEXT NOT NULL,
	contract_price NUMERIC(50,0) NOT NULL,
	base_rpc_price NUMERIC(50,0) NOT NULL,
	sector_access_price NUMERIC(50,0) NOT NULL,
	max_collateral NUMERIC(50,0) NOT NULL,
	storage_price NUMERIC(50,0) NOT NULL,
	egress_price NUMERIC(50,0) NOT NULL,
	ingress_price NUMERIC(50,0) NOT NULL,
	max_account_balance NUMERIC(50,0) NOT NULL,
	collateral_multiplier DOUBLE PRECISION NOT NULL,
	max_account_age BIGINT NOT NULL, -- time.Duration
	price_table_validity BIGINT NOT NULL, -- time.Duration
	max_contract_duration BIGINT NOT NULL,
	window_size BIGINT NOT NULL,
	ingress_limit BIGINT NOT NULL,
	egress_limit BIGINT NOT NULL,
	syncer_ingress_limit BIGINT NOT NULL,
	syncer_egress_limit BIGINT NOT NULL,
	ddns_provider TEXT NOT NULL,
	ddns_update_v4 BOOLEAN NOT NULL,
	ddns_update_v6 BOOLEAN NOT NULL,
	ddns_opts BYTEA,
	registry_limit BIGINT NOT NULL,
	sector_cache_size BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE host_pinned_settings (
	id BIGINT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	currency TEXT NOT NULL,
	threshold DOUBLE PRECISION NOT NULL,
	storage_pinned BOOLEAN NOT NULL,
	storage_price DOUBLE PRECISION NOT NULL,
	ingress_pinned BOOLEAN NOT NULL,
	ingress_price DOUBLE PRECISION NOT NULL,
	egress_pinned BOOLEAN NOT NULL,
	egress_price DOUBLE PRECISION NOT NULL,
	max_collateral_pinned BOOLEAN NOT NULL,
	max_collateral DOUBLE PRECISION NOT NULL
);

CREATE TABLE webhooks (
	id BIGSERIAL PRIMARY KEY,
	callback_url TEXT UNIQUE NOT NULL,
	scopes TEXT NOT NULL,
	secret_key TEXT UNIQUE NOT NULL
);

CREATE TABLE syncer_peers (
	peer_address TEXT PRIMARY KEY NOT NULL,
	first_seen TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE syncer_bans (
	net_cidr TEXT PRIMARY KEY NOT NULL,
	expiration TIMESTAMP WITH TIME ZONE NOT NULL,
	reason TEXT NOT NULL
);
CREATE INDEX syncer_bans_expiration_index_idx ON syncer_bans (expiration);

CREATE TABLE global_settings (
	id BIGINT PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version BIGINT NOT NULL, -- used for migrations
	host_key BYTEA CHECK (LENGTH(host_key) = 64), -- ed25519 private key
	wallet_hash BYTEA CHECK (LENGTH(wallet_hash) = 32), -- used to prevent wallet seed changes
	last_scanned_index BYTEA CHECK (LENGTH(last_scanned_index) = 8+32), -- chain index of the last scanned block
	last_announce_index BYTEA CHECK (LENGTH(last_announce_index) = 8+32), -- chain index of the last host announcement
	last_announce_address TEXT, -- address of the last host announcement
	last_v2_announce_hash BYTEA CHECK (LENGTH(last_v2_announce_hash) = 32) -- hash of the last v2 host announcement
);
`

// testConnInfo is the connection info for the embedded PostgreSQL server started
// in TestMain. Each test creates its own uniquely-named database.
var testConnInfo ConnectionInfo

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

// runMain starts an embedded PostgreSQL server, runs the tests and shuts the
// server down. It is a separate function so that deferred cleanup runs before
// os.Exit.
func runMain(m *testing.M) int {
	port, err := freePort()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to find free port: %v\n", err)
		return 1
	}

	runtimePath, err := os.MkdirTemp("", "hostd-postgres-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create runtime dir: %v\n", err)
		return 1
	}
	defer os.RemoveAll(runtimePath)

	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("postgres").
		Password("postgres").
		Database("postgres").
		Port(uint32(port)).
		RuntimePath(runtimePath).
		DataPath(filepath.Join(runtimePath, "data")).
		Logger(io.Discard))
	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start embedded postgres: %v\n", err)
		return 1
	}
	defer func() {
		if err := pg.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to stop embedded postgres: %v\n", err)
		}
	}()

	testConnInfo = ConnectionInfo{
		Host:     "localhost",
		Port:     port,
		User:     "postgres",
		Password: "postgres",
		Database: "postgres",
		SSLMode:  "disable",
	}
	return m.Run()
}

// freePort returns a free TCP port on the loopback interface.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// newConnInfo returns connection info for a fresh, uniquely-named database on
// the embedded server.
func newConnInfo() ConnectionInfo {
	ci := testConnInfo
	ci.Database = "hostd_" + hex.EncodeToString(frand.Bytes(8))
	return ci
}

// openTestStore opens a store backed by a fresh database on the embedded server
// and registers cleanup to drop it.
func openTestStore(t *testing.T, log *zap.Logger) *Store {
	t.Helper()
	ci := newConnInfo()
	store, err := OpenDatabase(context.Background(), ci, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		dropDatabase(t, ci)
	})
	return store
}

// dropDatabase drops the database referenced by ci by connecting to the default
// postgres database.
func dropDatabase(t *testing.T, ci ConnectionInfo) {
	t.Helper()
	admin := ci
	admin.Database = "postgres"
	pool, err := pgxpool.New(context.Background(), admin.connString())
	if err != nil {
		t.Fatalf("failed to open admin connection: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{ci.Database}.Sanitize())); err != nil {
		t.Fatalf("failed to drop database %q: %v", ci.Database, err)
	}
}

func TestMigrationConsistency(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	ci := newConnInfo()
	if err := ensureDatabase(ctx, ci); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropDatabase(t, ci) })

	setupPool, err := pgxpool.New(ctx, ci.connString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setupPool.Exec(ctx, initialSchema); err != nil {
		setupPool.Close()
		t.Fatalf("failed to apply initial schema: %v", err)
	} else if _, err := setupPool.Exec(ctx, `INSERT INTO global_settings (id, db_version) VALUES (0, 1)`); err != nil {
		setupPool.Close()
		t.Fatalf("failed to set initial version: %v", err)
	}
	setupPool.Close()

	// open the store, migrating the initial schema up to the target version
	store, err := OpenDatabase(ctx, ci, log)
	if err != nil {
		t.Fatal(err)
	}

	expectedVersion := int64(len(migrations) + 1)
	if v := getDBVersion(ctx, store.pool); v != expectedVersion {
		store.Close()
		t.Fatalf("expected version %d, got %d", expectedVersion, v)
	}

	// ensure the database does not change version when opened again
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenDatabase(ctx, ci, log)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if v := getDBVersion(ctx, store.pool); v != expectedVersion {
		t.Fatalf("expected version %d after reopen, got %d", expectedVersion, v)
	}

	// prepare a baseline database initialized directly from init.sql
	baseline := openTestStore(t, log)

	// ensure the migrated database has the same indices as the baseline
	baselineIndices, err := getTableIndices(ctx, baseline.pool)
	if err != nil {
		t.Fatal(err)
	}
	migratedIndices, err := getTableIndices(ctx, store.pool)
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

	// ensure the migrated database has the same tables as the baseline
	baselineTables, err := getTables(ctx, baseline.pool)
	if err != nil {
		t.Fatal(err)
	}
	migratedTables, err := getTables(ctx, store.pool)
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
	for k := range baselineTables {
		baselineColumns, err := getTableColumns(ctx, baseline.pool, k)
		if err != nil {
			t.Fatal(err)
		}
		migratedColumns, err := getTableColumns(ctx, store.pool, k)
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

	// ensure the migrated database has the same key constraints as the baseline
	baselineKeyConstraints, err := getKeyConstraints(ctx, baseline.pool)
	if err != nil {
		t.Fatal(err)
	}
	migratedKeyConstraints, err := getKeyConstraints(ctx, store.pool)
	if err != nil {
		t.Fatal(err)
	}
	for kc := range baselineKeyConstraints {
		if !migratedKeyConstraints[kc] {
			t.Errorf("missing key constraint %s", kc)
		}
	}
	for kc := range migratedKeyConstraints {
		if !baselineKeyConstraints[kc] {
			t.Errorf("unexpected key constraint %s", kc)
		}
	}

	// ensure the migrated database has the same check constraints as the baseline
	baselineCheckConstraints, err := getCheckConstraints(ctx, baseline.pool)
	if err != nil {
		t.Fatal(err)
	}
	migratedCheckConstraints, err := getCheckConstraints(ctx, store.pool)
	if err != nil {
		t.Fatal(err)
	}
	for cc := range baselineCheckConstraints {
		if !migratedCheckConstraints[cc] {
			t.Errorf("missing check constraint %s", cc)
		}
	}
	for cc := range migratedCheckConstraints {
		if !baselineCheckConstraints[cc] {
			t.Errorf("unexpected check constraint %s", cc)
		}
	}
}

func getTableIndices(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	// https://www.postgresql.org/docs/current/view-pg-indexes.html
	const query = `SELECT schemaname, tablename, indexname, tablespace, indexdef FROM pg_indexes WHERE schemaname = 'public'`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indices := make(map[string]bool)
	for rows.Next() {
		var schema, table, index, def string
		var tablespace *string // tablespace is null if default for the database
		if err := rows.Scan(&schema, &table, &index, &tablespace, &def); err != nil {
			return nil, err
		}
		var ts string
		if tablespace != nil {
			ts = *tablespace
		}
		indices[fmt.Sprintf("%s.%s.%s.%s.%s", schema, table, index, ts, def)] = true
	}
	return indices, rows.Err()
}

func getTables(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	// https://www.postgresql.org/docs/current/infoschema-tables.html
	const query = `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`
	rows, err := pool.Query(ctx, query)
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
	return tables, rows.Err()
}

func getTableColumns(ctx context.Context, pool *pgxpool.Pool, table string) (map[string]bool, error) {
	// https://www.postgresql.org/docs/current/infoschema-columns.html
	const query = `SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`
	rows, err := pool.Query(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var name, colType, nullable string
		var colDefault *string
		if err := rows.Scan(&name, &colType, &colDefault, &nullable); err != nil {
			return nil, err
		}
		var def string
		if colDefault != nil {
			def = *colDefault
		}
		columns[fmt.Sprintf("%s.%s.%s.%s", name, colType, def, nullable)] = true
	}
	return columns, rows.Err()
}

func getKeyConstraints(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	// https://www.postgresql.org/docs/current/infoschema-key-column-usage.html
	const query = `SELECT constraint_schema, constraint_name, table_name, column_name FROM information_schema.key_column_usage WHERE constraint_schema = 'public'`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraints := make(map[string]bool)
	for rows.Next() {
		var schema, name, table, column string
		if err := rows.Scan(&schema, &name, &table, &column); err != nil {
			return nil, err
		}
		constraints[fmt.Sprintf("%s.%s.%s.%s", schema, name, table, column)] = true
	}
	return constraints, rows.Err()
}

func getCheckConstraints(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	// https://www.postgresql.org/docs/current/infoschema-check-constraints.html
	const query = `SELECT constraint_schema, check_clause FROM information_schema.check_constraints WHERE constraint_schema = 'public'`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraints := make(map[string]bool)
	for rows.Next() {
		var schema, clause string
		if err := rows.Scan(&schema, &clause); err != nil {
			return nil, err
		}
		// the constraint name is ignored since it doesn't match between databases
		constraints[fmt.Sprintf("%s.%s", schema, clause)] = true
	}
	return constraints, rows.Err()
}
