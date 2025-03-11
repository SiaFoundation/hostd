package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
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

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed. If the transaction fails due to a busy error, it is
// retried up to 10 times before returning.
func (s *Store) transaction(fn func(*txn) error) error {
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

// Backup creates a backup of the database at the specified path. The backup is
// created using the SQLite backup API, which is safe to use with a
// live database.
//
// This function should be used if the database is already open in the current
// process. If the database is not already open, use Backup.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	return backupDB(ctx, s.db, destPath)
}

func sqliteFilepath(fp string) string {
	params := []string{
		fmt.Sprintf("_busy_timeout=%d", busyTimeout),
		"_foreign_keys=true",
		"_journal_mode=WAL",
		"_secure_delete=false",
		"_auto_vacuum=INCREMENTAL",
		"_cache_size=-65536", // 64MiB
	}
	return "file:" + fp + "?" + strings.Join(params, "&")
}

// doTransaction is a helper function to execute a function within a transaction. If fn returns
// an error, the transaction is rolled back. Otherwise, the transaction is
// committed.
func doTransaction(db *sql.DB, log *zap.Logger, fn func(tx *txn) error) error {
	dbtx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	start := time.Now()
	defer func() {
		if err := dbtx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("failed to rollback transaction", zap.Error(err))
		}
		// log the transaction if it took longer than txn duration
		if time.Since(start) > longTxnDuration {
			log.Debug("long transaction", zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"), zap.Bool("failed", err != nil))
		}
	}()

	tx := &txn{
		Tx:  dbtx,
		log: log,
	}
	if err := fn(tx); err != nil {
		return err
	} else if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func sqlConn(ctx context.Context, db *sql.DB) (c *sqlite3.SQLiteConn, err error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	raw, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}
	err = raw.Raw(func(driverConn any) error {
		var ok bool
		c, ok = driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return errors.New("connection is not a SQLiteConn")
		}
		return nil
	})
	return
}

// backupDB is a helper function that creates a backup of the source database at
// the specified path. The backup is created using the SQLite backup API, which
// is safe to use with a live database.
func backupDB(ctx context.Context, src *sql.DB, destPath string) (err error) {
	// create the destination database
	dest, err := sql.Open("sqlite3", sqliteFilepath(destPath))
	if err != nil {
		return fmt.Errorf("failed to open destination database: %w", err)
	}
	defer func() {
		// errors are ignored
		dest.Close()
		if err != nil {
			// remove the destination file if an error occurred during backup
			os.Remove(destPath)
		}
	}()

	// initialize the source conn
	sc, err := sqlConn(ctx, src)
	if err != nil {
		return fmt.Errorf("failed to create source connection: %w", err)
	}
	defer sc.Close()

	// initialize the destination conn
	dc, err := sqlConn(ctx, dest)
	if err != nil {
		return fmt.Errorf("failed to create destination connection: %w", err)
	}
	defer dc.Close()

	// start the backup
	backup, err := dc.Backup("main", sc, "main")
	if err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}
	// ensure the backup is closed
	defer func() {
		if err := backup.Finish(); err != nil {
			panic(fmt.Errorf("failed to finish backup: %w", err))
		}
	}()

	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if done, err := backup.Step(100); err != nil {
			return fmt.Errorf("backup step %d failed: %w", step, err)
		} else if done {
			break
		}
	}
	if _, err := dest.Exec("VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum destination database: %w", err)
	}
	return nil
}

// Backup creates a backup of the database at the specified path. The backup is
// created using the SQLite backup API, which is safe to use with a
// live database.
//
// This function should be used if the database is not already open in the
// current process. If the database is already open, use Store.Backup.
func Backup(ctx context.Context, srcPath, destPath string) (err error) {
	// ensure the source file exists
	if _, err := os.Stat(srcPath); err != nil {
		return fmt.Errorf("source file does not exist: %w", err)
	}

	// prevent overwriting the destination file
	if _, err := os.Stat(destPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("destination file already exists")
	} else if destPath == "" {
		return errors.New("empty destination path")
	}

	// open a new connection to the source database. We don't want to run
	// any migrations or other operations on the source database since it
	// might be open in another process.
	src, err := sql.Open("sqlite3", sqliteFilepath(srcPath))
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	return backupDB(ctx, src, destPath)
}

// IntegrityCheck runs a PRAGMA integrity_check on the database and logs any
// integrity errors. If any errors are found, an error is returned.
func IntegrityCheck(ctx context.Context, fp string, log *zap.Logger) error {
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("failed to run integrity check: %w", err)
	}
	defer rows.Close()
	var hasErrors bool
	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("failed to scan integrity check result: %w", err)
		} else if result != "ok" {
			log.Error("integrity check failed", zap.String("result", result))
			hasErrors = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate integrity check results: %w", err)
	} else if hasErrors {
		return errors.New("integrity check failed")
	}
	return nil
}

// ForeignKeyCheck runs a PRAGMA foreign_key_check on the database and logs any
// foreign key constraint violations. If any violations are found, an error is
// returned.
func ForeignKeyCheck(ctx context.Context, fp string, log *zap.Logger) error {
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("failed to run foreign key check: %w", err)
	}
	defer rows.Close()
	var hasErrors bool
	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var table string
		var rowid sql.NullInt64
		var fkTable string
		var fkRowid sql.NullInt64

		if err := rows.Scan(&table, &rowid, &fkTable, &fkRowid); err != nil {
			return fmt.Errorf("failed to scan foreign key check result: %w", err)
		}
		hasErrors = true
		log.Error("foreign key constraint violated", zap.String("table", table), zap.Int64("rowid", rowid.Int64), zap.String("fkTable", fkTable), zap.Int64("fkRowid", fkRowid.Int64))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate foreign key check results: %w", err)
	} else if hasErrors {
		return errors.New("foreign key constraint violated")
	}
	return nil
}

// OpenDatabase creates a new SQLite store and initializes the database. If the
// database does not exist, it is created.
func OpenDatabase(fp string, log *zap.Logger) (*Store, error) {
	db, err := sql.Open("sqlite3", sqliteFilepath(fp))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	store := &Store{
		db:  db,
		log: log,
	}
	if err := store.init(); err != nil {
		return nil, err
	}
	sqliteVersion, _, _ := sqlite3.Version()
	log.Debug("database initialized", zap.String("sqliteVersion", sqliteVersion), zap.Int("schemaVersion", len(migrations)+1), zap.String("path", fp))
	return store, nil
}

var _ interface {
	wallet.SingleAddressStore
	contracts.ContractStore
	storage.VolumeStore
	settings.Store
} = (*Store)(nil)
