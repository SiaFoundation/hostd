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

	vs := NewVolumeStore(db)

	v, err := vs.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	}

	old, err := vs.SetMaxSectors(v.ID, 500)
	if err != nil {
		t.Fatal(err)
	} else if old != 1000 {
		t.Fatalf("expected old max of 1000, got %v", old)
	}

	volumes, err := vs.Volumes()
	if err != nil {
		t.Fatal(err)
	} else if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].MaxSectors != 500 {
		t.Fatalf("expected max of 500, got %v", volumes[0].MaxSectors)
	}

	old, err = vs.SetMaxSectors(v.ID, 1000)
	if err != nil {
		t.Fatal(err)
	} else if old != 500 {
		t.Fatalf("expected old max of 500, got %v", old)
	}

	volumes, err = vs.Volumes()
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

	vs := NewVolumeStore(db)

	vol, err := vs.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	} else if vs.GrowVolume(vol.ID, 100) != nil {
		t.Fatal(err)
	}

	// try to add a sector to the volume
	_, release, err := vs.StoreSector(frand.Entropy256())
	if err != nil {
		t.Fatal(err)
	} else if err := release(); err != nil { // immediately release the sector so it can be used again
		t.Fatal(err)
	}

	// set the volume to read-only
	if err := vs.SetReadOnly(vol.ID, true); err != nil {
		t.Fatal(err)
	}

	// try to add another sector to the volume, should fail with
	// ErrNotEnoughStorage
	_, _, err = vs.StoreSector(frand.Entropy256())
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

	vs := NewVolumeStore(db)
	ss := NewSectorStore(db)

	vol, err := vs.AddVolume("test", 1000, false)
	if err != nil {
		t.Fatal(err)
	}

	root := frand.Entropy256()
	// try to store a sector in the empty volume, should return
	// ErrNotEnoughStorage
	_, _, err = vs.StoreSector(root)
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// grow the volume to support a sector
	if err := vs.GrowVolume(vol.ID, 1); err != nil {
		t.Fatal(err)
	}

	// store the sector
	{
		loc, release, err := vs.StoreSector(root)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := release(); err != nil {
				t.Fatal("failed to release sector1:", err)
			}
		}()

		// check that the sector was stored in the correct location
		if loc.Volume != vol.ID {
			t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
		} else if loc.Index != 0 {
			t.Fatalf("expected sector index 0, got %v", loc.Index)
		}

		// add the sector's metadata to store the sector
		err = ss.Update(func(tx storage.SectorUpdateTransaction) error {
			return tx.AddSectorMetadata(root, loc)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	{
		// check the location was added
		loc, release, err := ss.SectorLocation(root)
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

		vol, err := vs.Volumes()
		if err != nil {
			t.Fatal(err)
		} else if len(vol) != 1 {
			t.Fatalf("expected 1 volume, got %v", len(vol))
		} else if vol[0].UsedSectors != 1 {
			t.Fatalf("expected 1 used sector, got %v", vol[0].UsedSectors)
		}
	}

	{
		// store the sector again, should return ErrSectorExists
		loc, release, err := vs.StoreSector(root)
		if !errors.Is(err, storage.ErrSectorExists) {
			t.Fatalf("expected ErrSectorExists, got %v", err)
		}
		defer func() {
			if err := release(); err != nil {
				t.Fatal("failed to release sector2:", err)
			}
		}()
		if loc.Volume != vol.ID {
			t.Fatalf("expected volume ID %v, got %v", vol.ID, loc.Volume)
		} else if loc.Index != 0 {
			t.Fatalf("expected sector index 0, got %v", loc.Index)
		}
	}

	// try to store another sector in the volume, should return
	// ErrNotEnoughStorage
	_, _, err = vs.StoreSector(frand.Entropy256())
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestVolumeGrow(t *testing.T) {

}
