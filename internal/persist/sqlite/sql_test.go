package sqlite

import (
	"context"
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
	err = db.transaction(context.Background(), func(_ context.Context, tx txn) (err error) {
		version = getDBVersion(tx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if version == 0 {
		t.Fatalf("expected non-zero version, got %v", version)
	}
}
