package sqlite

import (
	"database/sql"
	_ "embed" // for init.sql
	"errors"
	"time"

	"fmt"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (s *Store) initNewDatabase(target int64) error {
	return s.transaction(func(tx *txn) error {
		if _, err := tx.Exec(initDatabase); err != nil {
			return err
		} else if err := setDBVersion(tx, target); err != nil {
			return fmt.Errorf("failed to set initial database version: %w", err)
		} else if err = generateHostKey(tx); err != nil {
			return fmt.Errorf("failed to generate host key: %w", err)
		}
		return nil
	})
}

func (s *Store) upgradeDatabase(current, target int64) error {
	log := s.log.Named("migrations")
	log.Info("migrating database", zap.Int64("current", current), zap.Int64("target", target))

	return s.transaction(func(tx *txn) error {
		if _, err := tx.Exec("PRAGMA defer_foreign_keys=ON"); err != nil {
			return fmt.Errorf("failed to enable foreign key deferral: %w", err)
		}
		for _, fn := range migrations[current-1:] {
			current++
			start := time.Now()
			if err := fn(tx, log.With(zap.Int64("version", current))); err != nil {
				return fmt.Errorf("failed to migrate database to version %v: %w", current, err)
			}
			// check that no foreign key constraints were violated
			if err := tx.QueryRow("PRAGMA foreign_key_check").Scan(); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to check foreign key constraints after migration to version %v: %w", current, err)
			} else if err == nil {
				return fmt.Errorf("foreign key constraint violated after migration to version %v: %w", current, err)
			}
			log.Debug("migration complete", zap.Int64("current", current), zap.Int64("target", target), zap.Duration("elapsed", time.Since(start)))
		}

		// set the final database version
		return setDBVersion(tx, target)
	})
}

func (s *Store) init() error {
	// calculate the expected final database version
	target := int64(len(migrations) + 1)
	version := getDBVersion(s.db)
	switch {
	case version == 0:
		if err := s.initNewDatabase(target); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
	case version < target:
		if err := s.upgradeDatabase(version, target); err != nil {
			return fmt.Errorf("failed to upgrade database: %w", err)
		}
	case version > target:
		return fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
	}
	// nothing to do
	return nil
}

func generateHostKey(tx *txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(`UPDATE global_settings SET host_key=? RETURNING id`, key).Scan(&dbID)
	return
}
