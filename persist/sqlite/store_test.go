package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestTransactionRetry(t *testing.T) {
	t.Skip("This test is flaky and needs to be fixed")

	t.Run("transaction retry", func(t *testing.T) {
		log := zaptest.NewLogger(t)
		db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		err = db.transaction(func(tx *txn) error { return nil }) // start a new empty transaction, should succeed immediately
		if err != nil {
			t.Fatal(err)
		}

		ch := make(chan struct{}, 1) // channel to synchronize the transaction goroutine

		// start a transaction in a goroutine and hold it open for 5 seconds
		// this should allow for the next transaction to be retried a few times
		go func() {
			err := db.transaction(func(tx *txn) error {
				_, err := tx.Exec(`UPDATE global_settings SET host_key=?`, `foo`) // upgrade the transaction to an exclusive lock;
				if err != nil {
					return err
				}
				ch <- struct{}{}
				time.Sleep(500 * time.Millisecond)
				return nil
			})
			if err != nil {
				panic(err)
			}
			ch <- struct{}{}
		}()

		<-ch // wait for the transaction to start

		err = db.transaction(func(tx *txn) error {
			_, err = tx.Exec(`UPDATE global_settings SET host_key=?`, `bar`) // should fail and be retried
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		<-ch // wait for the transaction to finish
	})

	t.Run("transaction timeout", func(t *testing.T) {
		log := zaptest.NewLogger(t)
		db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		err = db.transaction(func(tx *txn) error { return nil }) // start a new empty transaction, should succeed immediately
		if err != nil {
			t.Fatal(err)
		}

		ch := make(chan struct{}, 1) // channel to synchronize the transaction goroutine

		go func() {
			err := db.transaction(func(tx *txn) error {
				_, err := tx.Exec(`UPDATE global_settings SET host_key=?`, `foo`) // upgrade the transaction to an exclusive lock;
				if err != nil {
					return err
				}
				ch <- struct{}{}
				time.Sleep(5 * time.Second)
				return nil
			})
			if err != nil {
				panic(err)
			}
			ch <- struct{}{}
		}()

		<-ch // wait for the transaction to start

		err = db.transaction(func(tx *txn) error {
			_, err := tx.Exec(`UPDATE global_settings SET host_key=?`, `bar`) // should fail and be retried
			if err != nil {
				return err
			}
			return nil
		})

		// verify the returned error is the busy error
		var sqliteErr sqlite3.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrBusy {
			t.Fatalf("expected busy error, got %v", err)
		}

		<-ch // wait for the transaction to finish
	})
}

func TestClearLockedSectors(t *testing.T) {
	const sectors = 100
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id, err := db.AddVolume("foo", false)
	if err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err = db.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

	assertSectors := func(locked, temp int) {
		t.Helper()
		// check that the sectors are locked
		var dbLocked, dbTemp int
		err := db.transaction(func(tx *txn) error {
			if err := tx.QueryRow(`SELECT COUNT(*) FROM locked_volume_sectors`).Scan(&dbLocked); err != nil {
				return fmt.Errorf("query locked sectors: %w", err)
			} else if err := tx.QueryRow(`SELECT COUNT(*) FROM temp_storage_sector_roots`).Scan(&dbTemp); err != nil {
				return fmt.Errorf("query temp sectors: %w", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		} else if dbLocked != locked {
			t.Fatalf("expected %v locked sectors, got %v", locked, dbLocked)
		} else if dbTemp != temp {
			t.Fatalf("expected %v temp sectors, got %v", temp, dbTemp)
		}

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if m.Storage.TempSectors != uint64(temp) {
			t.Fatalf("expected %v temp sectors, got %v", temp, m.Storage.TempSectors)
		}
	}

	// write temp sectors to the database
	for i := 1; i <= sectors; i++ {
		sectorRoot := frand.Entropy256()
		_, err := db.StoreSector(sectorRoot, func(storage.SectorLocation, bool) error {
			return nil
		})
		if err != nil {
			t.Fatal("add sector", i, err)
		}

		// only store the first half of the sectors as temp sectors
		if i > sectors/2 {
			continue
		}

		err = db.AddTemporarySectors([]storage.TempSector{
			{Root: sectorRoot, Expiration: uint64(i + 1)},
		})
		if err != nil {
			t.Fatal("add temp sector", i, err)
		}
	}

	// check that the sectors have been stored and locked
	assertSectors(sectors, sectors/2)

	// clear the locked sectors
	if err = db.clearLocks(); err != nil {
		t.Fatal(err)
	}

	// check that all the locks were removed and half the sectors deleted
	assertSectors(0, sectors/2)
}
