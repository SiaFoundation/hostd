package sqlite

import (
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

func TestInit(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "test.db")
	log, err := zap.NewDevelopmentConfig().Build()
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(fp, log)
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
