package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // import sqlite3 driver
)

type (
	txn interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}

	// A Store is a persistent store that uses a SQL database as its backend.
	Store struct {
		db *sql.DB
	}
)

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed.
func (s *Store) transaction(ctx context.Context, fn func(context.Context, txn) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return fmt.Errorf("failed to execute transaction: %w", err)
	} else if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// exclusiveTransaction executes a function within an exclusive transaction.
//
// note: the sqlite3 library does not support setting BEGIN EXCLUSIVE at the
// transaction level, so it's done manually here. It may be preferable to make
// all transactions exclusive.
func (s *Store) exclusiveTransaction(ctx context.Context, fn func(context.Context, txn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return fmt.Errorf("failed to begin exclusive transaction: %w", err)
	} else if err := fn(ctx, conn); err != nil {
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
		return err
	} else if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// getDBVersion returns the current version of the database.
func getDBVersion(tx txn) (version uint64) {
	const query = `SELECT db_version FROM global_settings;`
	err := tx.QueryRowContext(context.Background(), query).Scan(&version)
	if err != nil {
		return 0
	}
	return
}

// setDBVersion sets the current version of the database.
func setDBVersion(tx txn, version uint64) error {
	const query = `INSERT INTO global_settings (db_version) VALUES (?) ON CONFLICT (id) DO UPDATE SET db_version=excluded.db_version;`
	_, err := tx.ExecContext(context.Background(), query, version)
	return err
}

// OpenDatabase creates a new SQLite store and initializes the database. If the
// database does not exist, it is created.
func OpenDatabase(fp string) (*Store, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%v?_busy_timeout=30000&_journal_mode=WAL&_foreign_keys=true", fp))
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return store, nil
}
