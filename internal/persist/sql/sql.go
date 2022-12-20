package sql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // import sqlite3 driver
)

type (
	tx interface {
		Exec(query string, args ...any) (sql.Result, error)
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
		Query(query string, args ...any) (*sql.Rows, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRow(query string, args ...any) *sql.Row
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
		Stmt(stmt *sql.Stmt) *sql.Stmt
		StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
	}

	// A Store is a persistent store that uses a SQL database as its backend.
	Store struct {
		db *sql.DB
	}
)

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed.
func (s *Store) transaction(ctx context.Context, fn func(tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if err := fn(tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return fmt.Errorf("failed to execute transaction: %w", err)
	} else if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// getDBVersion returns the current version of the database.
func getDBVersion(tx tx) (version uint64) {
	const query = `SELECT db_version FROM global_settings;`
	err := tx.QueryRow(query).Scan(&version)
	if err != nil {
		return 0
	}
	return
}

// setDBVersion sets the current version of the database.
func setDBVersion(tx tx, version uint64) error {
	const query = `INSERT INTO global_settings (db_version) VALUES (?) ON CONFLICT (id) DO UPDATE SET db_version=excluded.db_version;`
	_, err := tx.Exec(query, version)
	return err
}

// NewSQLiteStore creates a new SQLiteStore and initializes the database
func NewSQLiteStore(fp string) (*Store, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_busy_timeout=5000&_journal_mode=WAL", fp))
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return store, nil
}
