package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/hostd/v2/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// TestRecalcVolumeMetricsEmptyVolume ensures that a volume with no
// volume_sectors rows is reset to zero. An interrupted force removal on an
// older version could delete every row of a volume while leaving the
// storage_volumes row behind with stale counts.
func TestRecalcVolumeMetricsEmptyVolume(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// one volume that keeps its sectors and one that loses all of its rows, so
	// the repair has to fix the second without disturbing the first
	kept, err := addTestVolume(db, "kept", 4)
	if err != nil {
		t.Fatal(err)
	}
	damaged, err := addTestVolume(db, "damaged", 4)
	if err != nil {
		t.Fatal(err)
	}

	// fill the volume being kept so it holds a nonzero count to preserve
	if err := db.SetReadOnly(damaged.ID, true); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := db.AddTempSector(frand.Entropy256(), 100, func(storage.SectorLocation) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetReadOnly(damaged.ID, false); err != nil {
		t.Fatal(err)
	}

	// reproduce the state a legacy interrupted force removal leaves: every
	// volume_sectors row gone, storage_volumes row still present with stale
	// counts.
	err = db.transaction(func(tx *txn) error {
		if _, err := tx.Exec(`DELETE FROM volume_sectors WHERE volume_id=$1`, damaged.ID); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE storage_volumes SET used_sectors=3, total_sectors=4 WHERE id=$1`, damaged.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.RecalcVolumeMetrics(); err != nil {
		t.Fatal(err)
	}

	// the damaged volume must be zeroed
	if v, err := db.Volume(damaged.ID); err != nil {
		t.Fatal(err)
	} else if v.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", v.UsedSectors)
	} else if v.TotalSectors != 0 {
		t.Fatalf("expected 0 total sectors, got %v", v.TotalSectors)
	}

	// the untouched volume must keep its counts
	if v, err := db.Volume(kept.ID); err != nil {
		t.Fatal(err)
	} else if v.UsedSectors != 4 {
		t.Fatalf("expected 4 used sectors, got %v", v.UsedSectors)
	} else if v.TotalSectors != 4 {
		t.Fatalf("expected 4 total sectors, got %v", v.TotalSectors)
	}

	// the global metrics must agree with the repaired volumes
	if usedSectors, totalSectors, err := db.StorageUsage(); err != nil {
		t.Fatal(err)
	} else if usedSectors != 4 {
		t.Fatalf("expected 4 used sectors, got %v", usedSectors)
	} else if totalSectors != 4 {
		t.Fatalf("expected 4 total sectors, got %v", totalSectors)
	}

	// the storage metrics must have been reset along with the volumes
	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.PhysicalSectors != 4 {
		t.Fatalf("expected 4 physical sectors, got %v", m.Storage.PhysicalSectors)
	} else if m.Storage.TotalSectors != 4 {
		t.Fatalf("expected 4 total sectors, got %v", m.Storage.TotalSectors)
	}
}
