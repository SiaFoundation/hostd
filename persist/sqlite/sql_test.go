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

	version := getDBVersion(db.db)
	if version == 0 {
		t.Fatalf("expected non-zero version, got %v", version)
	}
}
