package sqlite

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

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
	if err != nil {
		return nil, err
	} else if dur := time.Since(start); dur > longQueryDuration {
		s.log.Debug("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
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
// transaction is committed. If the transaction fails due to a busy error, it is
// retried up to 15 times before returning.
func (s *Store) transaction(fn func(txn) error) error {
	var err error
	txnID := hex.EncodeToString(frand.Bytes(4))
	log := s.log.Named("transaction").With(zap.String("id", txnID))
	start := time.Now()
	attempt := 1
	for ; attempt < maxRetryAttempts; attempt++ {
		attemptStart := time.Now()
		log := log.With(zap.Int("attempt", attempt))
		err = doTransaction(s.db, log, fn)
		if err == nil {
			// no error, break out of the loop
			return nil
		}

		// return immediately if the error is not a busy error
		if !strings.Contains(err.Error(), "database is locked") {
			break
		}
		// exponential backoff
		sleep := time.Duration(math.Pow(factor, float64(attempt))) * time.Millisecond
		if sleep > maxBackoff {
			sleep = maxBackoff
		}
		log.Debug("database locked", zap.Duration("elapsed", time.Since(attemptStart)), zap.Duration("totalElapsed", time.Since(start)), zap.Stack("stack"), zap.Duration("retry", sleep))
		jitterSleep(sleep)
	}
	return fmt.Errorf("transaction failed (attempt %d): %w", attempt, err)
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

func sqliteFilepath(fp string) string {
	params := []string{
		fmt.Sprintf("_busy_timeout=%d", busyTimeout),
		"_foreign_keys=true",
		"_journal_mode=WAL",
		"_secure_delete=false",
		"_cache_size=-65536", // 64MiB
	}
	return "file:" + fp + "?" + strings.Join(params, "&")
}

// doTransaction is a helper function to execute a function within a transaction. If fn returns
// an error, the transaction is rolled back. Otherwise, the transaction is
// committed.
func doTransaction(db *sql.DB, log *zap.Logger, fn func(tx txn) error) error {
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	defer func() {
		// log the transaction if it took longer than txn duration
		if time.Since(start) > longTxnDuration {
			log.Debug("long transaction", zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"), zap.Bool("failed", err != nil))
		}
	}()

	ltx := &loggedTxn{
		Tx:  tx,
		log: log,
	}
	if err := fn(ltx); err != nil {
		return err
	} else if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func clearLockedSectors(tx txn, log *zap.Logger) error {
	rows, err := tx.Query(`DELETE FROM locked_sectors RETURNING sector_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var sectorIDs []int64
	for rows.Next() {
		var sectorID int64
		if err := rows.Scan(&sectorID); err != nil {
			return fmt.Errorf("failed to scan sector id: %w", err)
		}
	}

	removed, err := pruneSectors(tx, sectorIDs)
	if err != nil {
		return fmt.Errorf("failed to prune sectors: %w", err)
	}
	log.Debug("cleared locked sectors", zap.Int("locked", len(sectorIDs)), zap.Stringers("removed", removed))
	return nil
}

func clearLockedLocations(tx txn) error {
	_, err := tx.Exec(`DELETE FROM locked_volume_sectors`)
	return err
}

func (s *Store) clearLocks() error {
	return s.transaction(func(tx txn) error {
		if err := clearLockedLocations(tx); err != nil {
			return fmt.Errorf("failed to clear locked locations: %w", err)
		} else if err = clearLockedSectors(tx, s.log.Named("clearLockedSectors")); err != nil {
			return fmt.Errorf("failed to clear locked sectors: %w", err)
		}
		return nil
	})
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
	} else if err = store.clearLocks(); err != nil {
		// clear any locked sectors, metadata not synced to disk is safe to
		// overwrite.
		return nil, fmt.Errorf("failed to clear locked sectors table: %w", err)
	}
	sqliteVersion, _, _ := sqlite3.Version()
	log.Debug("database initialized", zap.String("sqliteVersion", sqliteVersion), zap.Int("schemaVersion", len(migrations)+1), zap.String("path", fp))
	return store, nil
}
