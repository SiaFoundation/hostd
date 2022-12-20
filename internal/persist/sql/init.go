package sql

import (
	"context"
	_ "embed"

	"fmt"
)

// dbVersion is the current version of the database. It is used to determine
// if the database needs to be upgraded. It must be incremented if any of
// the init queries are changed.
const dbVersion = 1

// init queries are run when the database is first created.
//
//go:embed init.sql
var initDatabase string

func (ss *SQLStore) init() error {
	return ss.transaction(context.Background(), func(tx tx) error {
		version := getDBVersion(tx)
		if version == 0 {
			if _, err := tx.Exec(initDatabase, dbVersion); err != nil {
				return fmt.Errorf("failed to initialize database: %w", err)
			}
		} else if version == dbVersion {
			return nil
		}
		// note: run migrations before updating the version
		return setDBVersion(tx, dbVersion)
	})
}
