package sqlite

import (
	_ "embed" // for init.sql
	"time"

	"fmt"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	clearLockedSectors = `DELETE FROM locked_volume_sectors;`

	incrementalVacuumInterval = 2 * time.Hour
)

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (s *Store) init() error {
	var needsVacuum bool
	// calculate the expected final database version
	target := int64(len(migrations) + 1)
	err := s.transaction(func(tx txn) error {
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
		} else if version == target {
			return nil
		}
		// perform migrations
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
		needsVacuum = true // full vacuum after migrations
		return setDBVersion(tx, target)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	if needsVacuum {
		log := s.log.Named("vacuum")
		log.Info("performing full vacuum after migrations")
		start := time.Now()
		// perform a full vacuum after migrations
		if _, err := s.db.Exec("VACUUM;"); err != nil {
			return fmt.Errorf("failed to vacuum database: %w", err)
		}
		log.Info("full vacuum complete", zap.Duration("elapsed", time.Since(start)))
	} else {
		// setup incremental vacuuming
		if err := s.partialVacuum(); err != nil {
			return fmt.Errorf("failed to vacuum database: %w", err)
		}
	}
	go func() {
		t := time.NewTicker(incrementalVacuumInterval)
		defer t.Stop()

		log := s.log.Named("vacuum")

		for {
			select {
			case <-t.C:
				start := time.Now()
				if err := s.partialVacuum(); err != nil {
					s.log.Error("failed to vacuum database", zap.Error(err))
				}
				log.Debug("incremental vacuum complete", zap.Duration("elapsed", time.Since(start)))
			case <-s.closed:
				return
			}
		}
	}()

	return nil
}

func (s *Store) partialVacuum() error {
	start := time.Now()
	if _, err := s.exec(`PRAGMA incremental_vacuum(1024);`); err != nil {
		return err
	}
	s.log.Named("vacuum").Debug("incremental vacuum complete", zap.Duration("elapsed", time.Since(start)))
	return nil
}

func generateHostKey(tx txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(`UPDATE global_settings SET host_key=? RETURNING id`, key).Scan(&dbID)
	return
}
