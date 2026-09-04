package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// TestConcurrentSectorAccess drives writes, references, reads and pruning
// from many goroutines at once to exercise transaction retries.
func TestConcurrentSectorAccess(t *testing.T) {
	const workers, perWorker = 8, 40

	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := addTestVolume(db, "test", workers*perWorker); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers+1)
	roots := make([][]types.Hash256, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				root := frand.Entropy256()
				if err := db.StoreSector(root, func(storage.SectorLocation) error { return nil }); err != nil {
					errs <- fmt.Errorf("store: %w", err)
					return
				} else if err := db.AddTempSector(root, 100); err != nil {
					errs <- fmt.Errorf("add temp: %w", err)
					return
				} else if ok, err := db.HasSector(root); err != nil || !ok {
					errs <- fmt.Errorf("has sector: ok=%t err=%v", ok, err)
					return
				} else if _, err := db.SectorMetadata(root); err != nil {
					errs <- fmt.Errorf("metadata: %w", err)
					return
				}
				roots[w] = append(roots[w], root)
			}
		}()
	}

	stop := make(chan struct{})
	pruneDone := make(chan struct{})
	go func() {
		defer close(pruneDone)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if err := db.PruneSectors(context.Background(), time.Now().Add(-time.Minute)); err != nil && !errors.Is(err, context.Canceled) {
				errs <- fmt.Errorf("prune: %w", err)
				return
			}
		}
	}()

	wg.Wait()
	close(stop)
	<-pruneDone
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	expected := workers * perWorker
	var stored, referenced int
	if err := db.db.QueryRow(`SELECT (SELECT COUNT(*) FROM stored_sectors), (SELECT COUNT(*) FROM temp_storage_sector_roots)`).Scan(&stored, &referenced); err != nil {
		t.Fatal(err)
	} else if stored != expected || referenced != expected {
		t.Fatalf("expected %d stored and referenced sectors, got %d stored, %d referenced", expected, stored, referenced)
	}
	used, _, err := db.StorageUsage()
	if err != nil {
		t.Fatal(err)
	} else if used != uint64(expected) {
		t.Fatalf("expected %d used sectors, got %d", expected, used)
	}
	m, err := db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if m.Storage.PhysicalSectors != uint64(expected) || m.Storage.TempSectors != uint64(expected) {
		t.Fatalf("expected %d physical and temp sectors, got %d physical, %d temp", expected, m.Storage.PhysicalSectors, m.Storage.TempSectors)
	}
	for _, rs := range roots {
		for _, root := range rs {
			if _, err := db.SectorLocation(root); err != nil {
				t.Fatalf("sector %v: %s", root, err)
			}
		}
	}
}
