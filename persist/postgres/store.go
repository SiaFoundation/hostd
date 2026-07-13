package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	factor           = 1.8              // factor ^ retryAttempts = backoff time in milliseconds
	maxBackoff       = 15 * time.Second // max backoff time
	maxRetryAttempts = 30               // max number of retry attempts
)

type (
	// ConnectionInfo contains the information needed to connect to a
	// PostgreSQL database.
	ConnectionInfo struct {
		Host        string `json:"host" yaml:"host"`
		Port        int    `json:"port" yaml:"port"`
		User        string `json:"user" yaml:"user"`
		Password    string `json:"password" yaml:"password"`
		Database    string `json:"database" yaml:"database"`
		SSLMode     string `json:"sslmode" yaml:"sslmode"`
		SSLRootCert string `json:"sslrootcert" yaml:"sslrootcert"`
	}

	// A Store is a persistent store that uses a PostgreSQL database as its
	// backend.
	Store struct {
		pool *pgxpool.Pool
		log  *zap.Logger
	}
)

// escapeConnValue escapes a value for use inside a single-quoted libpq
// connection-string parameter. Per the libpq keyword/value syntax, single
// quotes and backslashes within a value must be backslash-escaped. Escaping
// prevents malformed strings and parameter injection for values containing
// these characters (e.g. a password with a single quote).
func escapeConnValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return v
}

// connString returns the libpq connection string for the given ConnectionInfo.
// It contains the plaintext password and must never be logged; use String for a
// redacted representation.
func (ci ConnectionInfo) connString() string {
	params := []string{
		fmt.Sprintf("host='%s'", escapeConnValue(ci.Host)),
		fmt.Sprintf("port='%d'", ci.Port),
		fmt.Sprintf("user='%s'", escapeConnValue(ci.User)),
		fmt.Sprintf("password='%s'", escapeConnValue(ci.Password)),
		fmt.Sprintf("dbname='%s'", escapeConnValue(ci.Database)),
	}
	// only include sslmode when set; an empty sslmode is invalid and differs
	// from omitting it (which lets the libpq/pgx default apply).
	if ci.SSLMode != "" {
		params = append(params, fmt.Sprintf("sslmode='%s'", escapeConnValue(ci.SSLMode)))
	}
	if ci.SSLRootCert != "" {
		params = append(params, fmt.Sprintf("sslrootcert='%s'", escapeConnValue(ci.SSLRootCert)))
	}
	return strings.Join(params, " ")
}

// String implements fmt.Stringer. The password is omitted so that accidentally
// logging or formatting a ConnectionInfo does not leak credentials.
func (ci ConnectionInfo) String() string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s", ci.Host, ci.Port, ci.User, ci.Database, ci.SSLMode)
}

// Close closes the underlying database connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed. If the transaction fails due to a serialization or
// deadlock error, it is retried up to maxRetryAttempts times before returning.
//
// Because fn may be invoked more than once, it must be idempotent and must not
// rely on side effects outside the transaction (e.g. mutating captured state or
// incrementing counters), as those would be repeated on each retry.
func (s *Store) transaction(fn func(context.Context, *txn) error) error {
	var err error
	txnID := hex.EncodeToString(frand.Bytes(4))
	log := s.log.Named("transaction").With(zap.String("id", txnID))
	start := time.Now()
	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		attemptStart := time.Now()
		log := log.With(zap.Int("attempt", attempt))
		err = s.doTransaction(log, fn)
		if err == nil {
			// no error, transaction succeeded
			return nil
		}

		// return immediately if the error is not retryable. non-pg errors are
		// never retried, as they may be context cancellations or other
		// unexpected errors.
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || !isRetryablePgError(pgErr.Code) {
			return fmt.Errorf("transaction failed (attempt %d): %w", attempt, err)
		}

		// don't back off after the final attempt
		if attempt == maxRetryAttempts {
			break
		}

		// exponential backoff
		sleep := min(time.Duration(math.Pow(factor, float64(attempt)))*time.Millisecond, maxBackoff)
		log.Debug("retryable database error", zap.Duration("elapsed", time.Since(attemptStart)), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("retry", sleep), zap.Error(err))
		time.Sleep(sleep + time.Duration(rand.Int63n(int64(sleep/2))))
	}
	return fmt.Errorf("transaction failed after %d attempts: %w", maxRetryAttempts, err)
}

// isRetryablePgError reports whether a PostgreSQL error code represents a
// transient failure that is worth retrying.
func isRetryablePgError(code string) bool {
	switch code {
	case "40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03": // lock_not_available
		return true
	default:
		return false
	}
}

// doTransaction is a helper function to execute a function within a
// transaction. If fn returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
func (s *Store) doTransaction(log *zap.Logger, fn func(context.Context, *txn) error) error {
	ctx := context.Background()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	failed := true
	start := time.Now()
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			log.Error("failed to rollback transaction", zap.Error(rollbackErr))
		}
		// log the transaction if it took longer than txn duration
		if time.Since(start) > longTxnDuration {
			log.Debug("long transaction", zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"), zap.Bool("failed", failed))
		}
	}()

	if err := fn(ctx, &txn{tx, log}); err != nil {
		return err
	} else if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	failed = false
	return nil
}

// ensureDatabase creates the configured database if it does not already exist.
func ensureDatabase(ctx context.Context, ci ConnectionInfo) error {
	// return early if we're connecting to the default database
	if ci.Database == "postgres" {
		return nil
	}
	db := ci.Database
	ci.Database = "postgres"

	// connect to the postgres database
	pool, err := pgxpool.New(ctx, ci.connString())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer pool.Close()

	// check if the database exists
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", db).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	} else if exists {
		return nil
	}

	// create the database if it does not exist
	query := "CREATE DATABASE " + pgx.Identifier{db}.Sanitize()
	if _, err := pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// OpenDatabase creates a new PostgreSQL store and initializes the database. If
// the configured database does not exist, it is created. The passed-in context
// is used to establish the connection pool and to ensure the database exists; it
// does not cancel schema initialization or migrations, which run on a background
// context.
func OpenDatabase(ctx context.Context, ci ConnectionInfo, log *zap.Logger) (*Store, error) {
	if err := ensureDatabase(ctx, ci); err != nil {
		return nil, fmt.Errorf("failed to ensure database %q exists: %w", ci.Database, err)
	}

	pool, err := pgxpool.New(ctx, ci.connString())
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	} else if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	store := &Store{
		pool: pool,
		log:  log,
	}
	if err := store.init(int64(len(migrations) + 1)); err != nil {
		pool.Close()
		return nil, err
	}
	log.Debug("database initialized", zap.String("database", ci.Database), zap.String("host", ci.Host), zap.Int("port", ci.Port), zap.Int("schemaVersion", len(migrations)+1))
	return store, nil
}
