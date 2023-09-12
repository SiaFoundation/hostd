package sqlite

import (
	"context"
	"database/sql"
	"math/rand"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // import sqlite3 driver
	"go.uber.org/zap"
)

const (
	longQueryDuration = 4 * time.Millisecond
	longTxnDuration   = 10 * time.Millisecond
)

type (
	// A scanner is an interface that wraps the Scan method of sql.Rows and sql.Row
	scanner interface {
		Scan(dest ...any) error
	}

	// A txn is an interface for executing queries within a transaction.
	txn interface {
		// Exec executes a query without returning any rows. The args are for
		// any placeholder parameters in the query.
		Exec(query string, args ...any) (sql.Result, error)
		// Prepare creates a prepared statement for later queries or executions.
		// Multiple queries or executions may be run concurrently from the
		// returned statement. The caller must call the statement's Close method
		// when the statement is no longer needed.
		Prepare(query string) (*loggedStmt, error)
		// Query executes a query that returns rows, typically a SELECT. The
		// args are for any placeholder parameters in the query.
		Query(query string, args ...any) (*loggedRows, error)
		// QueryRow executes a query that is expected to return at most one row.
		// QueryRow always returns a non-nil value. Errors are deferred until
		// Row's Scan method is called. If the query selects no rows, the *Row's
		// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
		// first selected row and discards the rest.
		QueryRow(query string, args ...any) *loggedRow
	}

	// A dbTxn wraps a Store and implements the txn interface.
	dbTxn struct {
		store *Store
	}

	loggedStmt struct {
		*sql.Stmt
		query string
		log   *zap.Logger
	}

	loggedTxn struct {
		*sql.Tx
		log *zap.Logger
	}

	loggedRow struct {
		*sql.Row
		log *zap.Logger
	}

	loggedRows struct {
		*sql.Rows
		log *zap.Logger
	}
)

func (lr *loggedRows) Next() bool {
	start := time.Now()
	next := lr.Rows.Next()
	if dur := time.Since(start); dur > longQueryDuration {
		lr.log.Debug("slow next", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return next
}

func (lr *loggedRows) Scan(dest ...any) error {
	start := time.Now()
	err := lr.Rows.Scan(dest...)
	if dur := time.Since(start); dur > longQueryDuration {
		lr.log.Debug("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

func (lr *loggedRow) Scan(dest ...any) error {
	start := time.Now()
	err := lr.Row.Scan(dest...)
	if dur := time.Since(start); dur > longQueryDuration {
		lr.log.Debug("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

func (ls *loggedStmt) Exec(args ...any) (sql.Result, error) {
	return ls.ExecContext(context.Background(), args...)
}

func (ls *loggedStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := ls.Stmt.ExecContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		ls.log.Debug("slow exec", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

func (ls *loggedStmt) Query(args ...any) (*sql.Rows, error) {
	return ls.QueryContext(context.Background(), args...)
}

func (ls *loggedStmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := ls.Stmt.QueryContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		ls.log.Debug("slow query", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return rows, err
}

func (ls *loggedStmt) QueryRow(args ...any) *loggedRow {
	return ls.QueryRowContext(context.Background(), args...)
}

func (ls *loggedStmt) QueryRowContext(ctx context.Context, args ...any) *loggedRow {
	start := time.Now()
	row := ls.Stmt.QueryRowContext(ctx, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		ls.log.Debug("slow query row", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRow{row, ls.log.Named("row")}
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (lt *loggedTxn) Exec(query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := lt.Tx.Exec(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		lt.log.Debug("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (lt *loggedTxn) Prepare(query string) (*loggedStmt, error) {
	start := time.Now()
	stmt, err := lt.Tx.Prepare(query)
	if dur := time.Since(start); dur > longQueryDuration {
		lt.log.Debug("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	} else if err != nil {
		return nil, err
	}
	return &loggedStmt{
		Stmt:  stmt,
		query: query,
		log:   lt.log.Named("statement"),
	}, nil
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (lt *loggedTxn) Query(query string, args ...any) (*loggedRows, error) {
	start := time.Now()
	rows, err := lt.Tx.Query(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		lt.log.Debug("slow query", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRows{rows, lt.log.Named("rows")}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (lt *loggedTxn) QueryRow(query string, args ...any) *loggedRow {
	start := time.Now()
	row := lt.Tx.QueryRow(query, args...)
	if dur := time.Since(start); dur > longQueryDuration {
		lt.log.Debug("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRow{row, lt.log.Named("row")}
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (dt *dbTxn) Exec(query string, args ...any) (sql.Result, error) {
	return dt.store.exec(query, args...)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (dt *dbTxn) Prepare(query string) (*loggedStmt, error) {
	return dt.store.prepare(query)
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (dt *dbTxn) Query(query string, args ...any) (*loggedRows, error) {
	return dt.store.query(query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (dt *dbTxn) QueryRow(query string, args ...any) *loggedRow {
	return dt.store.queryRow(query, args...)
}

func queryPlaceHolders(n int) string {
	if n == 0 {
		return ""
	} else if n == 1 {
		return "?"
	}
	var b strings.Builder
	b.Grow(((n - 1) * 2) + 1) // ?,?
	for i := 0; i < n-1; i++ {
		b.WriteString("?,")
	}
	b.WriteString("?")
	return b.String()
}

func queryArgs[T any](args []T) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg
	}
	return out
}

// getDBVersion returns the current version of the database.
func getDBVersion(db *sql.DB) (version int64) {
	// error is ignored -- the database may not have been initialized yet.
	db.QueryRow(`SELECT db_version FROM global_settings;`).Scan(&version)
	return
}

// setDBVersion sets the current version of the database.
func setDBVersion(tx txn, version int64) error {
	const query = `UPDATE global_settings SET db_version=$1 RETURNING id;`
	var dbID int64
	return tx.QueryRow(query, version).Scan(&dbID)
}

// jitterSleep sleeps for a random duration between t and t*1.5.
func jitterSleep(t time.Duration) {
	time.Sleep(t + time.Duration(rand.Int63n(int64(t/2))))
}
