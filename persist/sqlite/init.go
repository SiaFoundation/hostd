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
	log := s.log.Named("migrations").With(zap.Int64("target", target))
	for ; current < target; current++ {
		version := current + 1 // initial schema is version 1, migration 0 is version 2, etc.
		log := log.With(zap.Int64("version", version))
		start := time.Now()
		fn := migrations[current-1]
		err := s.transaction(func(tx *txn) error {
			if _, err := tx.Exec("PRAGMA defer_foreign_keys=ON"); err != nil {
				return fmt.Errorf("failed to enable foreign key deferral: %w", err)
			} else if err := fn(tx, log); err != nil {
				return err
			} else if err := checkFKConsistency(tx, log); err != nil {
				return fmt.Errorf("failed foreign key check: %w", err)
			}
			return setDBVersion(tx, version)
		})
		if err != nil {
			return fmt.Errorf("migration %d failed: %w", version, err)
		}
		log.Info("migration complete", zap.Duration("elapsed", time.Since(start)))
	}
	return nil
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
		s.log.Info("database version is out of date;", zap.Int64("version", version), zap.Int64("target", target))
		if err := s.upgradeDatabase(version, target); err != nil {
			return fmt.Errorf("failed to upgrade database: %w", err)
		}
	case version > target:
		return fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
	}
	// nothing to do
	return nil
}

func checkFKConsistency(txn *txn, log *zap.Logger) error {
	rows, err := txn.Query("PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("failed to run foreign key check: %w", err)
	}
	defer rows.Close()
	var hasErrors bool
	for rows.Next() {
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

func generateHostKey(tx *txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(`UPDATE global_settings SET host_key=? RETURNING id`, key).Scan(&dbID)
	return
}
