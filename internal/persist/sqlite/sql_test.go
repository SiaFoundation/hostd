package sqlite

import (
	"path/filepath"
	"testing"
)

func TestInit(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDatabase(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var version uint64
	err = db.transaction(func(tx txn) (err error) {
		version = getDBVersion(tx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if version == 0 {
		t.Fatalf("expected non-zero version, got %v", version)
	}
}
