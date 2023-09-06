package sqlite

import (
	_ "embed" // for init.sql
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

func (s *Store) init() error {
	// calculate the expected final database version
	target := int64(len(migrations) + 1)
	return s.transaction(func(tx txn) error {
		// check the current database version and perform any necessary
		// migrations
		version := getDBVersion(tx)
		if version == 0 {
			if _, err := tx.Exec(initDatabase); err != nil {
				return fmt.Errorf("failed to initialize database: %w", err)
			} else if err := setDBVersion(tx, target); err != nil {
				return fmt.Errorf("failed to set initial database version: %w", err)
			} else if err = generateHostKey(tx); err != nil {
				return fmt.Errorf("failed to generate host key: %w", err)
			}
			return nil
		} else if version > target {
			return fmt.Errorf("database version %v is newer than expected %v. database downgrades are not supported", version, target)
		} else if version == target {
			return nil
		}
		logger := s.log.Named("migrations")
		logger.Info("migrating database", zap.Int64("current", version), zap.Int64("target", target))
		for _, fn := range migrations[version-1:] {
			version++
			start := time.Now()
			if err := fn(tx); err != nil {
				return fmt.Errorf("failed to migrate database to version %v: %w", version, err)
			}
			logger.Debug("migration complete", zap.Int64("current", version), zap.Int64("target", target), zap.Duration("elapsed", time.Since(start)))
		}
		return setDBVersion(tx, target)
	})
}

func generateHostKey(tx txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(`UPDATE global_settings SET host_key=? RETURNING id`, key).Scan(&dbID)
	return
}
