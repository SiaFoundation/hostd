package sql

import (
	"context"
	"path/filepath"
	"testing"
)

func TestInit(t *testing.T) {
	db, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var version uint64
	err = db.transaction(context.Background(), func(tx tx) (err error) {
		version = getDBVersion(tx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if version != dbVersion {
		t.Fatalf("expected version %d, got %d", dbVersion, version)
	}
}
