package sqlite

import (
	"path/filepath"
	"testing"

	"go.uber.org/zap/zaptest"
)

func TestInit(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "test.db")
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(fp, log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var version int64
	err = db.transaction(func(tx txn) error {
		version = getDBVersion(tx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if version == 0 {
		t.Fatalf("expected non-zero version, got %v", version)
	}
}
