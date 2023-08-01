package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

const defaultTxnTimeout = 2 * time.Minute

type (
	// A Store is a persistent store that uses a SQL database as its backend.
	Store struct {
		db  *sql.DB
		log *zap.Logger
	}
)

// exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (s *Store) exec(query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := s.db.Exec(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

// prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (s *Store) prepare(query string) (*loggedStmt, error) {
	start := time.Now()
	stmt, err := s.db.Prepare(query)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	} else if err != nil {
		return nil, err
	}
	return &loggedStmt{
		Stmt:  stmt,
		query: query,
		log:   s.log.Named("statement"),
	}, nil
}

// query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (s *Store) query(query string, args ...any) (*loggedRows, error) {
	start := time.Now()
	rows, err := s.db.Query(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow query", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRows{rows, s.log.Named("rows")}, err
}

// queryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (s *Store) queryRow(query string, args ...any) *loggedRow {
	start := time.Now()
	row := s.db.QueryRow(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRow{row, s.log.Named("row")}
}

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed.
func (s *Store) transaction(ctx context.Context, fn func(txn) error) error {
	var tx *sql.Tx
	var err error
	start := time.Now()
	for i := 1; i <= 10; i++ {
		tx, err = s.db.BeginTx(ctx, nil)
		// no error, break out of the loop
		if err == nil {
			break
		} else if sqliteErr, ok := err.(sqlite3.Error); !ok || sqliteErr.Code != sqlite3.ErrBusy {
			// if the error is not a busy error, return immediately
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		s.log.Debug("database locked", zap.Int("attempt", i), zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"))
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	ltx := &loggedTxn{
		Tx:  tx,
		log: s.log.Named("transaction"),
	}
	start = time.Now()
	err = fn(ltx)
	if err != nil {
		return err
	}
	// log the transaction if it took longer than txn duration
	if time.Since(start) > longTxnDuration {
		ltx.log.Debug("long transaction", zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"))
	}

	// commit the transaction
	commitStart := time.Now()
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// log the commit if it took longer than commit duration
	if time.Since(commitStart) > longQueryDuration {
		ltx.log.Debug("long transaction commit", zap.Duration("elapsed", time.Since(commitStart)), zap.Duration("totalElapsed", time.Since(start)), zap.Stack("stack"))
	}
	return nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

func sqliteFilepath(fp string) string {
	params := []string{
		"_busy_timeout=10000", // 10s
		"_foreign_keys=true",
		"_journal_mode=WAL",
		"_secure_delete=false",
		"_cache_size=-65536", // 64MiB
	}
	return "file:" + fp + "?" + strings.Join(params, "&")
}

// OpenDatabase creates a new SQLite store and initializes the database. If the
// database does not exist, it is created.
func OpenDatabase(fp string, log *zap.Logger) (*Store, error) {
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		return nil, err
	}
	store := &Store{
		db:  db,
		log: log,
	}
	if err := store.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	} else if _, err := db.Exec(clearLockedSectors); err != nil {
		// clear any locked sectors, metadata not synced to disk is safe to
		// overwrite.
		return nil, fmt.Errorf("failed to clear locked sectors table: %w", err)
	}
	return store, nil
}
