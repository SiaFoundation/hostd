package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

func TestTransactionRetry(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.transaction(context.Background(), func(tx txn) error { return nil }) // start a new empty transaction, should succeed immediately
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan struct{}, 1) // channel to synchronize the transaction goroutine

	// start a transaction in a goroutine and hold it open for 10 seconds
	// this should allow for the next transaction to be retried once
	go func() {
		err := db.transaction(context.Background(), func(tx txn) error {
			ch <- struct{}{}
			time.Sleep(10 * time.Second)
			return nil
		})
		if err != nil {
			panic(err)
		}
		ch <- struct{}{}
	}()

	<-ch // wait for the transaction to start

	err = db.transaction(context.Background(), func(tx txn) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	<-ch // wait for the transaction to finish
}
