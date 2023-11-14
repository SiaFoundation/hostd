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
	return s.transaction(func(tx txn) error {
		if _, err := tx.Exec(initDatabase); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
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

	// disable foreign key constraints during migration
	if _, err := s.db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("failed to disable foreign key constraints: %w", err)
	}
	defer func() {
		// re-enable foreign key constraints
		if _, err := s.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
			log.Panic("failed to enable foreign key constraints", zap.Error(err))
		}
	}()

	return s.transaction(func(tx txn) error {
		for _, fn := range migrations[current-1:] {
			current++
			start := time.Now()
			if err := fn(tx, log.With(zap.Int64("version", current))); err != nil {
				return fmt.Errorf("failed to migrate database to version %v: %w", current, err)
			}
			// check that no foreign key constraints were violated
			if err := tx.QueryRow("PRAGMA foreign_key_check").Scan(); !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("foreign key constraints are not satisfied")
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
	// disable foreign key constraints during migration
	if _, err := s.db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("failed to disable foreign key constraints: %w", err)
	}

	version := getDBVersion(s.db)
	switch {
	case version == 0:
		return s.initNewDatabase(target)
	case version < target:
		return s.upgradeDatabase(version, target)
	case version > target:
		return fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
	}
	// nothing to do
	return nil
}

func generateHostKey(tx txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(`UPDATE global_settings SET host_key=? RETURNING id`, key).Scan(&dbID)
	return
}
