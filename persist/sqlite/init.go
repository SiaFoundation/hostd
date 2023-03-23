package sqlite

import (
	_ "embed" // for init.sql
	"time"

	"fmt"

	"go.uber.org/zap"
)

const clearLockedSectors = `DELETE FROM locked_volume_sectors;`

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (s *Store) init() error {
	// calculate the expected final database version
	targetVersion := int64(1 + len(migrations))
	return s.transaction(func(tx txn) error {
		// check the current database version and perform any necessary
		// migrations
		version := getDBVersion(tx)
		if version == 0 {
			if _, err := tx.Exec(initDatabase); err != nil {
				return fmt.Errorf("failed to initialize database: %w", err)
			}
			return nil
		} else if version == targetVersion {
			return nil
		}
		logger := s.log.Named("migrations")
		logger.Debug("migrating database", zap.Int64("current", version), zap.Int64("target", targetVersion))
		for _, fn := range migrations[version-1 : targetVersion] {
			version++
			start := time.Now()
			if err := fn(tx); err != nil {
				return fmt.Errorf("failed to migrate database to version %v: %w", version, err)
			}
			logger.Debug("migration complete", zap.Int64("current", version), zap.Int64("target", targetVersion), zap.Duration("elapsed", time.Since(start)))
		}
		// clear any locked sectors, metadata not synced to disk is safe to
		// overwrite.
		if _, err := tx.Exec(clearLockedSectors); err != nil {
			return fmt.Errorf("failed to clear locked sectors table: %w", err)
		}
		return setDBVersion(tx, targetVersion)
	})
}