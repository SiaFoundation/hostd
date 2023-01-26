package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"go.sia.tech/hostd/host/storage"
	"lukechampine.com/frand"
)

func TestVolumeSetReadOnly(t *testing.T) {
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", false)
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

	vol, err := db.AddVolume("test", false)
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
	} else if err := release(); err != nil {
		t.Fatal(err)
	}

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
	} else if err := release(); err != nil {
		t.Fatal(err)
	}

	// try to store another sector in the volume, should return
	// ErrNotEnoughStorage
	_, err = db.StoreSector(frand.Entropy256(), func(storage.SectorLocation, bool) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestVolumeGrow(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(vol.ID, initialSectors); err != nil {
		t.Fatal(err)
	}

	volumes, err := db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatal(err)
	} else if volumes[0].UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	} else if volumes[0].TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, volumes[0].TotalSectors)
	}

	// grow the volume
	if err := db.GrowVolume(vol.ID, initialSectors*2); err != nil {
		t.Fatal(err)
	}

	// check that the volume's available sectors match
	volumes, err = db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatal(err)
	} else if volumes[0].UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	} else if volumes[0].TotalSectors != initialSectors*2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors*2, volumes[0].TotalSectors)
	}
}

func TestVolumeShrink(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(vol.ID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// shrink the volume by half
	if err := db.ShrinkVolume(vol.ID, initialSectors/2); err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != vol.ID {
				t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index 0, got %v", loc.Index)
			} else if exists {
				t.Fatal("sector exists")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// check that the volume cannot be shrunk below the used sectors
	if err := db.ShrinkVolume(vol.ID, 2); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be shrunk to the used sectors
	if err := db.ShrinkVolume(vol.ID, 5); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveVolume(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(vol.ID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// check that the empty volume can be removed
	if err := db.RemoveVolume(vol.ID, false); err != nil {
		t.Fatal(err)
	}

	// add another volume
	vol, err = db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(vol.ID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != vol.ID {
				t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index 0, got %v", loc.Index)
			} else if exists {
				t.Fatal("sector exists")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// check that the volume cannot be removed
	if err := db.RemoveVolume(vol.ID, false); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be force removed
	if err := db.RemoveVolume(vol.ID, true); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkVolumeGrow(b *testing.B) {
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	vol, err := db.AddVolume("test", false)
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

	vol, err := db.AddVolume("test", false)
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
