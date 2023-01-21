package sqlite

import (
	_ "embed"

	"fmt"
)

const clearLockedSectors = `DELETE FROM locked_volume_sectors;`

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (s *Store) init() error {
	// calculate the expected final database version
	dbVersion := uint64(1 + len(migrations))
	return s.transaction(func(tx txn) error {
		// check the current database version and perform any necessary
		// migrations
		version := getDBVersion(tx)
		if version == 0 {
			if _, err := tx.Exec(initDatabase); err != nil {
				return fmt.Errorf("failed to initialize database: %w", err)
			}
			return nil
		} else if version == dbVersion {
			return nil
		}
		for _, fn := range migrations[version-1 : dbVersion] {
			version++
			if err := fn(tx); err != nil {
				return fmt.Errorf("failed to migrate database to version %v: %w", version, err)
			}
		}
		// clear any locked sectors, metadata not synced to disk is safe to
		// overwrite.
		if _, err := tx.Exec(clearLockedSectors); err != nil {
			return fmt.Errorf("failed to clear locked sectors table: %w", err)
		}
		return setDBVersion(tx, dbVersion)
	})
}
