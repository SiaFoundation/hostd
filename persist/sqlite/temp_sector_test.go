package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/storage"
	"go.uber.org/zap"
)

func assertTempSectorState(t *testing.T, db *Store, sectors, references, locks int) {
	t.Helper()
	var stored, assigned, refs, held, counted int
	err := db.readerDB.QueryRow(`SELECT
(SELECT COUNT(*) FROM stored_sectors),
(SELECT COUNT(*) FROM volume_sectors WHERE sector_id IS NOT NULL),
(SELECT COUNT(*) FROM temp_storage_sector_roots),
(SELECT COUNT(*) FROM volume_sector_locks),
(SELECT COALESCE(SUM(ref_count), 0) FROM stored_sectors)`).Scan(&stored, &assigned, &refs, &held, &counted)
	if err != nil {
		t.Fatal(err)
	} else if stored != sectors || assigned != sectors || refs != references || counted != references || held != locks {
		t.Fatalf("unexpected state: stored=%d assigned=%d refs=%d ref_count=%d locks=%d", stored, assigned, refs, counted, held)
	}
	m, err := db.Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if m.Storage.PhysicalSectors != uint64(sectors) || m.Storage.TempSectors != uint64(references) {
		t.Fatalf("unexpected storage metrics: %+v", m.Storage)
	}
}

func TestAddTempSectorPublication(t *testing.T) {
	for _, outcome := range []string{"success", "write failure", "reference failure"} {
		t.Run(outcome, func(t *testing.T) {
			db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := addTestVolume(db, "test", 1); err != nil {
				t.Fatal(err)
			}
			root := types.Hash256{1}
			writeErr := errors.New("write failed")
			err = db.AddTempSector(root, 100, func(loc storage.SectorLocation) error {
				assertTempSectorState(t, db, 0, 0, 1)
				if _, err := db.SectorLocation(root); !errors.Is(err, storage.ErrSectorNotFound) {
					t.Fatalf("expected unpublished root, got %v", err)
				} else if err := db.PruneSectors(context.Background()); err != nil {
					t.Fatal(err)
				}
				for _, force := range []bool{false, true} {
					if err := db.RemoveVolume(loc.Volume, force); err == nil {
						t.Fatal("removed a pending write reservation")
					}
				}
				if outcome == "write failure" {
					return writeErr
				} else if outcome == "reference failure" {
					return db.writeTransaction(func(tx *txn) error {
						_, err := tx.Exec(`CREATE TRIGGER fail_temp_reference BEFORE INSERT ON temp_storage_sector_roots
BEGIN SELECT RAISE(ABORT, 'injected reference failure'); END`)
						return err
					})
				}
				return nil
			})
			if outcome == "success" {
				if err != nil {
					t.Fatal(err)
				}
				assertTempSectorState(t, db, 1, 1, 0)
				// An existing copy can be referenced even when the volume is full.
				if err := db.AddTempSector(root, 200, func(_ storage.SectorLocation) error {
					t.Fatal("unexpected write for a stored sector")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				assertTempSectorState(t, db, 1, 2, 0)
				if err := db.ExpireTempSectors(100); err != nil {
					t.Fatal(err)
				}
				assertTempSectorState(t, db, 1, 1, 0)
			} else {
				if err == nil || outcome == "write failure" && !errors.Is(err, writeErr) {
					t.Fatalf("unexpected error: %v", err)
				}
				assertTempSectorState(t, db, 0, 0, 0)
			}
		})
	}
}

func TestAddTempSectorConcurrentRoot(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("failFirst=%v", failFirst), func(t *testing.T) {
			db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := addTestVolume(db, "test", 2); err != nil {
				t.Fatal(err)
			}
			root := types.Hash256{1}
			writeErr := errors.New("first write failed")
			var winner storage.SectorLocation
			err = db.AddTempSector(root, 100, func(first storage.SectorLocation) error {
				// Interleave a second upload that completes before the first.
				if err := db.AddTempSector(root, 200, func(second storage.SectorLocation) error {
					if second.ID == first.ID {
						t.Fatal("second upload reused unfinished data")
					}
					winner = second
					assertTempSectorState(t, db, 0, 0, 2)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				assertTempSectorState(t, db, 1, 1, 1)
				if failFirst {
					return writeErr
				}
				return nil
			})
			refs := 2
			if failFirst {
				refs = 1
				if !errors.Is(err, writeErr) {
					t.Fatalf("expected write error, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			assertTempSectorState(t, db, 1, refs, 0)
			if loc, err := db.SectorLocation(root); err != nil {
				t.Fatal(err)
			} else if loc.ID != winner.ID {
				t.Fatal("first upload replaced the completed copy")
			}
			// The redundant slot is immediately writable by another root.
			if err := db.AddTempSector(types.Hash256{2}, 100, func(storage.SectorLocation) error { return nil }); err != nil {
				t.Fatal(err)
			}
			assertTempSectorState(t, db, 2, refs+1, 0)
		})
	}
}
