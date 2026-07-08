package postgres

import (
	"context"
	_ "embed" // for init.sql
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (s *Store) initNewDatabase(target int64) error {
	return s.transaction(func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, initDatabase); err != nil {
			return err
		} else if err := setDBVersion(ctx, tx, target); err != nil {
			return fmt.Errorf("failed to set initial database version: %w", err)
		} else if err := generateHostKey(ctx, tx); err != nil {
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
		err := s.transaction(func(ctx context.Context, tx *txn) error {
			if err := fn(ctx, tx, log); err != nil {
				return err
			}
			return setDBVersion(ctx, tx, version)
		})
		if err != nil {
			return fmt.Errorf("migration %d failed: %w", version, err)
		}
		log.Info("migration complete", zap.Duration("elapsed", time.Since(start)))
	}
	return nil
}

func (s *Store) init(target int64) error {
	version := getDBVersion(context.Background(), s.pool)
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

func generateHostKey(ctx context.Context, tx *txn) (err error) {
	key := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	var dbID int64
	err = tx.QueryRow(ctx, `UPDATE global_settings SET host_key=$1 RETURNING id`, []byte(key)).Scan(&dbID)
	return
}
