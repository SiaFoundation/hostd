package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"go.sia.tech/hostd/host/storage"
	"lukechampine.com/frand"
)

func TestVolumeSetMaxSectors(t *testing.T) {
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	v, err := db.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	}

	old, err := db.SetMaxSectors(v.ID, 500)
	if err != nil {
		t.Fatal(err)
	} else if old != 1000 {
		t.Fatalf("expected old max of 1000, got %v", old)
	}

	volumes, err := db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].MaxSectors != 500 {
		t.Fatalf("expected max of 500, got %v", volumes[0].MaxSectors)
	}

	old, err = db.SetMaxSectors(v.ID, 1000)
	if err != nil {
		t.Fatal(err)
	} else if old != 500 {
		t.Fatalf("expected old max of 500, got %v", old)
	}

	volumes, err = db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].MaxSectors != 1000 {
		t.Fatalf("expected max of 1000, got %v", volumes[0].MaxSectors)
	}
}

func TestVolumeSetReadOnly(t *testing.T) {
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	} else if db.GrowVolume(vol.ID, 100) != nil {
		t.Fatal(err)
	}

	// try to add a sector to the volume
	release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	} else if err := release(); err != nil { // immediately release the sector so it can be used again
		t.Fatal(err)
	}

	// set the volume to read-only
	if err := db.SetReadOnly(vol.ID, true); err != nil {
		t.Fatal(err)
	}

	// try to add another sector to the volume, should fail with
	// ErrNotEnoughStorage
	_, err = db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestAddSector(t *testing.T) {
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	}

	root := frand.Entropy256()
	// try to store a sector in the empty volume, should return
	// ErrNotEnoughStorage
	_, err = db.StoreSector(root, func(storage.SectorLocation, bool) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// grow the volume to support a sector
	if err := db.GrowVolume(vol.ID, 1); err != nil {
		t.Fatal(err)
	}

	// store the sector
	release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
		// check that the sector was stored in the expected location
		if loc.Volume != vol.ID {
			t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
		} else if loc.Index != 0 {
			t.Fatalf("expected sector index 0, got %v", loc.Index)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Fatal("failed to release sector1:", err)
		}
	}()

	// check the location was added
	loc, release, err := db.SectorLocation(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Fatal("failed to release sector location:", err)
		}
	}()

	if loc.Volume != vol.ID {
		t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
	} else if loc.Index != 0 {
		t.Fatalf("expected sector index 0, got %v", loc.Index)
	}

	volumes, err := db.Volumes()
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].UsedSectors != 1 {
		t.Fatalf("expected 1 used sector, got %v", volumes[0].UsedSectors)
	}

	// store the sector again, exists should be true
	release, err = db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
		switch {
		case !exists:
			t.Fatal("sector does not exist")
		case loc.Volume != vol.ID:
			t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
		case loc.Index != 0:
			t.Fatalf("expected sector index 0, got %v", loc.Index)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Fatal("failed to release sector2:", err)
		}
	}()

	// try to store another sector in the volume, should return
	// ErrNotEnoughStorage
	_, err = db.StoreSector(frand.Entropy256(), func(storage.SectorLocation, bool) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestVolumeGrow(t *testing.T) {

}

func BenchmarkVolumeGrow(b *testing.B) {
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", uint64(b.N), false)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// grow the volume to b.N sectors
	if err := db.GrowVolume(vol.ID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkShrinkVolume(b *testing.B) {
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", uint64(b.N), false)
	if err != nil {
		b.Fatal(err)
	}

	// grow the volume to b.N sectors
	if err := db.GrowVolume(vol.ID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// remove all sectors from the volume
	if err := db.ShrinkVolume(vol.ID, 0); err != nil {
		b.Fatal(err)
	}
}
