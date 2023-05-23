package logging_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"go.sia.tech/hostd/logging"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestLogSyncer(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	core := zapcore.NewTee(logging.Core(db, zapcore.DebugLevel), log.Core())
	l := zap.New(core).With(zap.String("foo", "bar")).Named("test")
	l.Info("hello world")
	if err := l.Sync(); err != nil { // force the log to be written to the database
		t.Fatal(err)
	}

	// check that the log entry was written to the database
	entries, count, err := db.LogEntries(logging.Filter{
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 log entry, got", len(entries))
	} else if count != 1 {
		t.Fatalf("expected count to be 1, got %v", count)
	} else if entries[0].Message != "hello world" {
		t.Fatal("unexpected message:", entries[0].Message)
	}

	fields := make(map[string]any)
	if err := json.Unmarshal(entries[0].Fields, &fields); err != nil {
		t.Fatalf("unexpected fields %q: %s", entries[0].Fields, err)
	} else if fields["foo"] != "bar" {
		t.Fatal("unexpected field:", fields["foo"])
	}

	// check that pagination works as expected
	entries, count, err = db.LogEntries(logging.Filter{
		Limit:  10,
		Offset: 10,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatal("expected 0 log entries, got", len(entries))
	} else if count != 1 {
		t.Fatalf("expected count to be 1, got %v", count)
	}

	// check that filtering works as expected
	entries, count, err = db.LogEntries(logging.Filter{
		Names:  []string{"none"},
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatal("expected 0 log entries, got", len(entries))
	} else if count != 0 {
		t.Fatalf("expected count to be 0, got %v", count)
	}
}
