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
		path string

		writerDB *sql.DB
		readerDB *sql.DB
		log      *zap.Logger
	}
)

// Close closes the underlying database.
func (s *Store) Close() error {
	return errors.Join(s.readerDB.Close(), s.writerDB.Close())
}

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed. If the transaction fails due to a busy error, it is
// retried up to 10 times before returning.
func (s *Store) transaction(fn func(*txn) error) error {
	txnID := hex.EncodeToString(frand.Bytes(4))
	log := s.log.Named("transaction").With(zap.String("id", txnID))
	return retryTransaction(s.readerDB, log, fn)
}

func (s *Store) writeTransaction(fn func(*txn) error) error {
	txnID := hex.EncodeToString(frand.Bytes(4))
	log := s.log.Named("writeTransaction").With(zap.String("id", txnID))
	return retryTransaction(s.writerDB, log, fn)
}

// Backup creates a backup of the open database. The backup is created using
// the SQLite backup API, which is safe to use with a live database.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	return Backup(ctx, s.path, destPath)
}

var sqliteBaseParams = []string{
	fmt.Sprintf("_busy_timeout=%d", time.Minute.Milliseconds()),
	"_foreign_keys=true",
	"_journal_mode=WAL",
	"_secure_delete=false",
	"_auto_vacuum=INCREMENTAL",
	"_cache_size=-65536", // 64MiB
}

func readerFilepath(fp string) string {
	return "file:" + fp + "?" + strings.Join(sqliteBaseParams, "&")
}

func writerFilepath(fp string) string {
	params := append(sqliteBaseParams, "_txlock=immediate")
	return "file:" + fp + "?" + strings.Join(params, "&")
}

func isBusy(err error) bool {
	sqliteErr, ok := errors.AsType[sqlite3.Error](err)
	return ok && sqliteErr.Code == sqlite3.ErrBusy
}

// retryTransaction retries a transaction that failed due to a SQLite database is locked error
// up to [maxRetryAttempts] times before returning the last error.
func retryTransaction(db *sql.DB, log *zap.Logger, fn func(tx *txn) error) error {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		attemptStart := time.Now()
		log := log.With(zap.Int("attempt", attempt))
		err := doTransaction(db, log, fn)
		switch {
		case err == nil:
			return nil
		case !isBusy(err):
			return err
		case attempt >= maxRetryAttempts:
			return err
		}
		// exponential backoff
		sleep := min(time.Duration(math.Pow(factor, float64(attempt)))*time.Millisecond, maxBackoff)
		log.Debug("database locked", zap.Duration("elapsed", time.Since(attemptStart)), zap.Duration("totalElapsed", time.Since(start)), zap.Stack("stack"), zap.Duration("retry", sleep))
		jitterSleep(sleep)
	}
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
	dest, err := sql.Open("sqlite3", readerFilepath(destPath))
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

		if done, err := backup.Step(-1); err != nil {
			return fmt.Errorf("backup step %d failed: %w", step, err)
		} else if done {
			break
		}
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
	src, err := sql.Open("sqlite3", readerFilepath(srcPath))
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer src.Close()

	return backupDB(ctx, src, destPath)
}

// IntegrityCheck runs a PRAGMA integrity_check on the database and logs any
// integrity errors. If any errors are found, an error is returned.
func IntegrityCheck(ctx context.Context, fp string, log *zap.Logger) error {
	db, err := sql.Open("sqlite3", readerFilepath(fp))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("failed to run integrity check: %w", err)
	}
	results, err := collectRows(rows, func(s scanner) (result string, err error) {
		err = s.Scan(&result)
		return result, err
	})
	if err != nil {
		return fmt.Errorf("failed to iterate integrity check results: %w", err)
	}
	var hasErrors bool
	for _, result := range results {
		if result != "ok" {
			log.Error("integrity check failed", zap.String("result", result))
			hasErrors = true
		}
	}
	if hasErrors {
		return errors.New("integrity check failed")
	}
	return nil
}

// ForeignKeyCheck runs a PRAGMA foreign_key_check on the database and logs any
// foreign key constraint violations. If any violations are found, an error is
// returned.
func ForeignKeyCheck(ctx context.Context, fp string, log *zap.Logger) error {
	db, err := sql.Open("sqlite3", readerFilepath(fp))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("failed to run foreign key check: %w", err)
	}
	violations, err := collectRows(rows, scanFKViolation)
	if err != nil {
		return fmt.Errorf("failed to iterate foreign key check results: %w", err)
	}
	for _, v := range violations {
		log.Error("foreign key constraint violated", zap.String("table", v.table), zap.Int64("rowid", v.rowid.Int64), zap.String("fkTable", v.fkTable), zap.Int64("fkRowid", v.fkRowid.Int64))
	}
	if len(violations) > 0 {
		return errors.New("foreign key constraint violated")
	}
	return nil
}

// OpenDatabase creates a new SQLite store and initializes the database. If the
// database does not exist, it is created.
func OpenDatabase(fp string, log *zap.Logger) (*Store, error) {
	readerDB, err := sql.Open("sqlite3", readerFilepath(fp))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// mattn/sqlite3 does not support per transaction isolation
	// levels like a nice SQL driver. Workaround it by having
	// separate reader and writer conns.
	writerDB, err := sql.Open("sqlite3", writerFilepath(fp))
	if err != nil {
		defer readerDB.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	writerDB.SetMaxOpenConns(1) // SQLite has at most one writer

	store := &Store{
		path:     fp, // used for backups
		readerDB: readerDB,
		writerDB: writerDB,
		log:      log,
	}
	if err := store.init(int64(len(migrations) + 1)); err != nil {
		defer readerDB.Close()
		defer writerDB.Close()
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
