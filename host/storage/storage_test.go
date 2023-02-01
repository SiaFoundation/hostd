package storage_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"lukechampine.com/frand"
)

const sectorSize = 1 << 22

func checkFileSize(fp string, expectedSize int64) error {
	stat, err := os.Stat(fp)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	} else if stat.Size() != expectedSize {
		return fmt.Errorf("expected file size %v, got %v", expectedSize, stat.Size())
	}
	return nil
}

func TestAddVolume(t *testing.T) {
	const expectedSectors = 500
	dir := t.TempDir()

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		t.Fatal(err)
	}

	volume, err := vm.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors)
	if err != nil {
		t.Fatal(err)
	}

	volumes, err := vm.Volumes()
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case len(volumes) != 1:
		t.Fatalf("expected 1 volumes, got %v", len(volumes))
	case volumes[0].ID != volume.ID:
		t.Fatalf("expected volume %v, got %v", volume.ID, volumes[0].ID)
	case volumes[0].TotalSectors != expectedSectors:
		t.Fatalf("expected %v total sectors, got %v", expectedSectors, volumes[0].TotalSectors)
	case volumes[0].UsedSectors != 0:
		t.Fatalf("expected 0 used sectors, got %v", volumes[0].UsedSectors)
	case volumes[0].ReadOnly:
		t.Fatal("expected volume to be writable")
	}
}

func TestRemoveVolume(t *testing.T) {
	const expectedSectors = (1 << 40) / (1 << 22) // 1 TiB
	dir := t.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := vm.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors)
	if err != nil {
		t.Fatal(err)
	}

	sector := make([]byte, 1<<22)
	if _, err := frand.Read(sector[:256]); err != nil {
		t.Fatal(err)
	}
	root := storage.SectorRoot(merkle.SectorRoot(sector))

	// write the sector
	release, err := vm.Write(root, sector)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(volume.ID, false); !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// remove the sector
	if err := vm.RemoveSector(root); err != nil {
		t.Fatal(err)
	}

	// remove the volume
	if err := vm.RemoveVolume(volume.ID, false); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeGrow(t *testing.T) {
	const initialSectors = 32
	dir := t.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		t.Fatal(err)
	}
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumeFilePath, initialSectors)
	if err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(initialSectors*sectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	// grow the volume
	const newSectors = 64
	if err := vm.ResizeVolume(volume.ID, newSectors); err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(newSectors*sectorSize)); err != nil {
		t.Fatal(err)
	}
	volume, err = vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != newSectors {
		t.Fatalf("expected %v total sectors, got %v", newSectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}
}

func TestVolumeShrink(t *testing.T) {
	const sectors = 32
	dir := t.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		t.Fatal(err)
	}
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumeFilePath, sectors)
	if err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(sectors*sectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]storage.SectorRoot, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		sector := make([]byte, 1<<22)
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := storage.SectorRoot(merkle.SectorRoot(sector))
		release, err := vm.Write(root, sector)
		if err != nil {
			t.Fatal(i, err)
		}
		defer release()
		roots = append(roots, root)

		// validate the volume stats are correct
		volumes, err := vm.Volumes()
		if err != nil {
			t.Fatal(err)
		}
		if volumes[0].UsedSectors != uint64(i+1) {
			t.Fatalf("expected %v used sectors, got %v", i+1, volumes[0].UsedSectors)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// validate that each sector was stored in the expected location
	for i, root := range roots {
		loc, release, err := db.SectorLocation(root)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if loc.Volume != volume.ID {
			t.Fatal(err)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector %v to be at index %v, got %v", root, i, loc.Index)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// try to shrink the volume, should fail since no space is available
	toRemove := sectors / 4
	remainingSectors := uint64(sectors - toRemove)
	if err := vm.ResizeVolume(volume.ID, remainingSectors); !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected not enough storage error, got %v", err)
	}

	// remove some sectors from the beginning of the volume
	for _, root := range roots[:toRemove] {
		if err := vm.RemoveSector(root); err != nil {
			t.Fatal(err)
		}
	}
	// when shrinking, the roots after the target size should be moved to
	// the beginning of the volume
	roots = append(roots[remainingSectors:], roots[toRemove:remainingSectors]...)

	// shrink the volume by the number of sectors removed, should succeed
	if err := vm.ResizeVolume(volume.ID, remainingSectors); err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(remainingSectors*sectorSize)); err != nil {
		t.Fatal(err)
	}

	volume, err = vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != remainingSectors {
		t.Fatalf("expected %v total sectors, got %v", remainingSectors, volume.TotalSectors)
	} else if volume.UsedSectors != remainingSectors {
		t.Fatalf("expected %v used sectors, got %v", remainingSectors, volume.UsedSectors)
	}

	// validate that the sectors were moved to the beginning of the volume
	for i, root := range roots {
		loc, release, err := db.SectorLocation(root)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if loc.Volume != volume.ID {
			t.Fatal(err)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector %v to be at index %v, got %v", root, i, loc.Index)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVolumeManagerReadWrite(t *testing.T) {
	const sectors = 32
	dir := t.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		t.Fatal(err)
	}
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumeFilePath, sectors)
	if err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(sectors*sectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]storage.SectorRoot, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		sector := make([]byte, 1<<22)
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := storage.SectorRoot(merkle.SectorRoot(sector))
		release, err := vm.Write(root, sector)
		if err != nil {
			t.Fatal(i, err)
		}
		defer release()
		roots = append(roots, root)

		// validate the volume stats are correct
		volumes, err := vm.Volumes()
		if err != nil {
			t.Fatal(err)
		}
		if volumes[0].UsedSectors != uint64(i+1) {
			t.Fatalf("expected %v used sectors, got %v", i+1, volumes[0].UsedSectors)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// read the sectors back
	frand.Shuffle(len(roots), func(i, j int) { roots[i], roots[j] = roots[j], roots[i] })
	for _, root := range roots {
		sector, err := vm.Read(root)
		if err != nil {
			t.Fatal(err)
		}
		retrievedRoot := storage.SectorRoot(merkle.SectorRoot(sector))
		if retrievedRoot != root {
			t.Fatalf("expected root %v, got %v", root, retrievedRoot)
		}
	}
}

func BenchmarkVolumeManagerWrite(b *testing.B) {
	dir := b.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		b.Fatal(err)
	}
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(volumeFilePath, uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(sectorSize)

	// fill the volume
	for i := 0; i < b.N; i++ {
		sector := make([]byte, 1<<22)
		frand.Read(sector[:256])
		root := storage.SectorRoot(merkle.SectorRoot(sector))
		release, err := vm.Write(root, sector)
		if err != nil {
			b.Fatal(i, err)
		} else if err := release(); err != nil {
			b.Fatal(i, err)
		}
	}
}

func BenchmarkVolumeManagerRead(b *testing.B) {
	dir := b.TempDir()

	// create the database
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db)
	if err != nil {
		b.Fatal(err)
	}
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(volumeFilePath, uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	// fill the volume
	written := make([]storage.SectorRoot, 0, b.N)
	for i := 0; i < b.N; i++ {
		sector := make([]byte, 1<<22)
		frand.Read(sector[:256])
		root := storage.SectorRoot(merkle.SectorRoot(sector))
		release, err := vm.Write(root, sector)
		if err != nil {
			b.Fatal(i, err)
		} else if err := release(); err != nil {
			b.Fatal(i, err)
		}
		written = append(written, root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(sectorSize)
	// read the sectors back
	for _, root := range written {
		if _, err := vm.Read(root); err != nil {
			b.Fatal(err)
		}
	}
}
