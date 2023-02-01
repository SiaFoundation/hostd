package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
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

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if db.GrowVolume(volumeID, 100) != nil {
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
	if err := db.SetReadOnly(volumeID, true); err != nil {
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

	volumeID, err := db.AddVolume("test", false)
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
	if err := db.GrowVolume(volumeID, 1); err != nil {
		t.Fatal(err)
	}

	// store the sector
	release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
		// check that the sector was stored in the expected location
		if loc.Volume != volumeID {
			t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
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

	if loc.Volume != volumeID {
		t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
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
		case loc.Volume != volumeID:
			t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
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

func TestVolumeAdd(t *testing.T) {
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var volumes []storage.Volume
	for i := 1; i <= 5; i++ {
		localPath := fmt.Sprintf("test %v", i)
		volumeID, err := db.AddVolume(localPath, false)
		if err != nil {
			t.Fatal(err)
		}

		volume, err := db.Volume(volumeID)
		if err != nil {
			t.Fatal(err)
		} else if volume.ID != i {
			t.Fatalf("expected volume ID to be %v, got %v", i, volume.ID)
		} else if volume.LocalPath != localPath {
			t.Fatalf("expected local path to be %v, got %v", localPath, volume.LocalPath)
		}

		dbVolumes, err := db.Volumes()
		if err != nil {
			t.Fatal(err)
		} else if len(dbVolumes) != i {
			t.Fatalf("expected %v volumes, got %v", i, len(volumes))
		}

		added, err := db.Volume(volumeID)
		if err != nil {
			t.Fatal(err)
		}
		volumes = append(volumes, added)
	}

	dbVolumes, err := db.Volumes()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbVolumes) != len(volumes) {
		t.Fatalf("expected %v volumes, got %v", len(volumes), len(dbVolumes))
	}

	for i, volume := range volumes {
		dbVolume := dbVolumes[i]
		if !reflect.DeepEqual(volume, dbVolume) {
			t.Fatalf("expected volume to be %v, got %v", volume, dbVolume)
		}
	}
}

func TestVolumeGrow(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volumeID, initialSectors); err != nil {
		t.Fatal(err)
	}

	volumes, err := db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	} else if volumes[0].TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, volumes[0].TotalSectors)
	}

	// double the number of sectors in the volume
	if err := db.GrowVolume(volumeID, initialSectors*2); err != nil {
		t.Fatal(err)
	}

	// check that the volume's available sectors match
	volumes, err = db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	} else if volumes[0].TotalSectors != initialSectors*2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors*2, volumes[0].TotalSectors)
	}

	// add a second volume
	volume2ID, err := db.AddVolume("test2", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volume2ID, initialSectors/2); err != nil {
		t.Fatal(err)
	}

	if volume, err := db.Volume(volume2ID); err != nil {
		t.Fatal(err)
	} else if volume.ID != volume2ID {
		t.Fatalf("expected volume ID %v, got %v", volume2ID, volume.ID)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	} else if volume.TotalSectors != initialSectors/2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors/2, volume.TotalSectors)
	}

	volumes, err = db.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %v", len(volumes))
	} else if volumes[0].UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	} else if volumes[0].TotalSectors != initialSectors*2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors*2, volumes[0].TotalSectors)
	} else if volumes[1].TotalSectors != initialSectors/2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors/2, volumes[1].TotalSectors)
	}
}

func TestVolumeShrink(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volumeID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// shrink the volume by half
	if err := db.ShrinkVolume(volumeID, initialSectors/2); err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volumeID {
				t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
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
	if err := db.ShrinkVolume(volumeID, 2); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be shrunk to the used sectors
	if err := db.ShrinkVolume(volumeID, 5); err != nil {
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

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volumeID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// check that the empty volume can be removed
	if err := db.RemoveVolume(volumeID, false); err != nil {
		t.Fatal(err)
	}

	// add another volume
	volumeID, err = db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volumeID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volumeID {
				t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
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
	if err := db.RemoveVolume(volumeID, false); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be force removed
	if err := db.RemoveVolume(volumeID, true); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateSectors(t *testing.T) {
	const initialSectors = 64
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volumeID, initialSectors); err != nil {
		t.Fatal(err)
	}

	// store enough sectors to fill the volume
	roots := make([]storage.SectorRoot, initialSectors)
	for i := range roots {
		root := frand.Entropy256()
		roots[i] = root
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volumeID {
				t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index %v, got %v", i, loc.Index)
			} else if exists {
				t.Fatal("sector already exists")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// remove the first half of the sectors
	for _, root := range roots[:initialSectors/2] {
		if err := db.RemoveSector(root); err != nil {
			t.Fatal(err)
		}
	}

	roots = roots[initialSectors/2:]

	var i int
	// migrate the remaining sectors to the first half of the volume
	err = db.MigrateSectors(volumeID, initialSectors/2, func(locations []storage.SectorLocation) error {
		for _, loc := range locations {
			if loc.Volume != volumeID {
				t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index %v, got %v", i, loc.Index)
			} else if loc.Root != roots[i] {
				t.Fatalf("expected sector root %v, got %v", roots[i], loc.Root)
			}
			i++
		}
		// note: sync to disk
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if i != 32 {
		t.Fatalf("expected 32 sectors, got %v", i)
	}

	// check that the sector metadata has been updated
	for i, root := range roots {
		if loc, release, err := db.SectorLocation(root); err != nil {
			t.Fatal(err)
		} else if loc.Volume != volumeID {
			t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector index %v, got %v", i, loc.Index)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// add a second volume with only a quarter of the initial space
	volume2ID, err := db.AddVolume("test2", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(volume2ID, initialSectors/4); err != nil {
		t.Fatal(err)
	}

	// migrate the remaining sectors from the first volume; should partially complete
	err = db.MigrateSectors(volumeID, 0, func(locations []storage.SectorLocation) error {
		if len(locations) > initialSectors/4 {
			t.Fatalf("expected only %v migrations, got %v", initialSectors/4, len(locations))
		}
		return nil
	})
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// check that volume 2 is now full
	v2, err := db.Volume(volume2ID)
	if err != nil {
		t.Fatal(err)
	} else if v2.ID != volume2ID {
		t.Fatalf("expected volume ID %v, got %v", volume2ID, v2.ID)
	} else if v2.UsedSectors != v2.TotalSectors {
		t.Fatalf("expected volume to be full, got %v/%v", v2.UsedSectors, v2.TotalSectors)
	}

	// check that volume 1 was reduced by the same amount
	v1, err := db.Volume(volumeID)
	if err != nil {
		t.Fatal(err)
	} else if v1.ID != volumeID {
		t.Fatalf("expected volume ID %v, got %v", volumeID, v1.ID)
	} else if v1.UsedSectors != initialSectors/4 {
		t.Fatalf("expected volume to have %v sectors, got %v/%v", initialSectors/4, v1.UsedSectors, v1.TotalSectors)
	}

}

func BenchmarkVolumeGrow(b *testing.B) {
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// grow the volume to b.N sectors
	if err := db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkVolumeShrink(b *testing.B) {
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		b.Fatal(err)
	}

	// grow the volume to b.N sectors
	if err := db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// remove all sectors from the volume
	if err := db.ShrinkVolume(volumeID, 0); err != nil {
		b.Fatal(err)
	}
}
