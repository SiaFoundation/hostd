package logging_test

import (
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
	core := zapcore.NewTee(logging.Core(db, zapcore.DebugLevel), log.Core())
	l := zap.New(core).With(zap.String("foo", "bar")).Named("test")
	l.Info("hello world")
	l.Sync() // force the log to be written to the database

	// check that the log entry was written to the database
	entries, err := db.LogEntries(logging.Filter{})
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatal("expected 1 log entry, got", len(entries))
	} else if entries[0].Message != "hello world" {
		t.Fatal("unexpected message:", entries[0].Message)
	} else if entries[0].Fields["foo"] != "bar" {
		t.Fatal("unexpected field:", entries[0].Fields["foo"])
	}

	entries, err = db.LogEntries(logging.Filter{Names: []string{"none"}})
	if err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatal("expected 0 log entries, got", len(entries))
	}
}
