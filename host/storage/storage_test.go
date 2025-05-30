package storage_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/persist/sqlite"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func checkFileSize(fp string, expectedSize int64) error {
	stat, err := os.Stat(fp)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	} else if stat.Size() != expectedSize {
		return fmt.Errorf("expected file size %v, got %v", expectedSize, stat.Size())
	}
	return nil
}

func TestVolumeLoad(t *testing.T) {
	const expectedSectors = 150
	dir := t.TempDir()

	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volume, err := vm.AddVolume(context.Background(), filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// write a sector
	var sector [rhp2.SectorSize]byte
	frand.Read(sector[:])
	root := rhp2.SectorRoot(&sector)
	if err = vm.Write(root, &sector); err != nil {
		t.Fatal(err)
	} else if err := vm.AddTemporarySectors([]storage.TempSector{{Root: root, Expiration: 1}}); err != nil { // must add a temp sector to prevent pruning
		t.Fatal(err)
	}

	// close the volume manager
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen the volume manager
	vm, err = storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	// check that the volume is still there
	loaded, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if loaded.ID != volume.ID {
		t.Fatal("loaded volume has wrong ID")
	} else if loaded.UsedSectors != 1 {
		t.Fatal("loaded volume has wrong size")
	} else if loaded.ReadOnly {
		t.Fatal("loaded volume should be writable")
	} else if loaded.TotalSectors != expectedSectors {
		t.Fatal("loaded volume has wrong size")
	}

	// check that the sector is still there
	sector2, err := vm.ReadSector(root)
	if err != nil {
		t.Fatal(err)
	} else if *sector2 != sector {
		t.Fatal("sector was corrupted")
	}

	// write a new sector
	frand.Read(sector[:])
	root = rhp2.SectorRoot(&sector)
	if err = vm.Write(root, &sector); err != nil {
		t.Fatal(err)
	}
}

func TestAddVolume(t *testing.T) {
	const expectedSectors = 500
	dir := t.TempDir()

	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volume, err := vm.AddVolume(context.Background(), filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
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
	case volumes[0].Status != storage.VolumeStatusReady:
		t.Fatalf("expected volume status %v, got %v", storage.VolumeStatusReady, volumes[0].Status)
	}
}

func TestRemoveVolume(t *testing.T) {
	const expectedSectors = 50
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(context.Background(), volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	roots := make([]types.Hash256, 10)
	for i := range roots {
		var sector [rhp2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		roots[i] = rhp2.SectorRoot(&sector)

		// write the sector

		if err := vm.Write(roots[i], &sector); err != nil {
			t.Fatal(err)
		} else if err := vm.AddTemporarySectors([]storage.TempSector{{Root: roots[i], Expiration: 1}}); err != nil { // must add a temp sector to prevent pruning
			t.Fatal(err)
		}
	}

	checkRoots := func(roots []types.Hash256) error {
		for _, root := range roots {
			sector, err := vm.ReadSector(root)
			if err != nil {
				return fmt.Errorf("failed to read sector: %w", err)
			} else if rhp2.SectorRoot(sector) != root {
				return errors.New("sector was corrupted")
			}
		}
		return nil
	}

	checkVolume := func(id int64, used, total uint64) error {
		vol, err := vm.Volume(id)
		if err != nil {
			return fmt.Errorf("failed to get volume: %w", err)
		} else if vol.UsedSectors != used {
			return fmt.Errorf("expected %v used sectors, got %v", used, vol.UsedSectors)
		} else if vol.TotalSectors != total {
			return fmt.Errorf("expected %v total sectors, got %v", total, vol.TotalSectors)
		}
		return nil
	}

	// attempt to remove the volume. Should return ErrMigrationFailed since
	// there is only one volume.
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		// blocking error should be nil
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// check that the volume metrics did not change
	if err := checkRoots(roots); err != nil {
		t.Fatal(err)
	} else if err := checkVolume(volume.ID, 10, expectedSectors); err != nil {
		t.Fatal(err)
	}

	// add a second volume with enough space to migrate half the sectors
	volume2, err := vm.AddVolume(context.Background(), filepath.Join(t.TempDir(), "hostdata2.dat"), uint64(len(roots)/2), result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	if err := checkVolume(volume2.ID, 0, uint64(len(roots)/2)); err != nil {
		t.Fatal(err)
	}

	// remove the volume first volume. Should still fail with ErrMigrationFailed,
	// but some sectors should be migrated to the second volume.
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatal(err)
	}

	if err := checkRoots(roots); err != nil {
		t.Fatal(err)
	} else if err := checkVolume(volume.ID, 5, expectedSectors); err != nil { // half the sectors should have been migrated
		t.Fatal(err)
	} else if err := checkVolume(volume2.ID, 5, uint64(len(roots)/2)); err != nil {
		t.Fatal(err)
	}

	// expand the second volume to accept the remaining sectors
	if err := vm.ResizeVolume(context.Background(), volume2.ID, expectedSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	if err := checkVolume(volume2.ID, 5, expectedSectors); err != nil {
		t.Fatal(err)
	} else if err := checkVolume(volume.ID, 5, expectedSectors); err != nil {
		t.Fatal(err)
	} else if err := checkRoots(roots); err != nil {
		t.Fatal(err)
	}

	// remove the first volume
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(volumePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("volume file still exists", err)
	}
}

func TestRemoveCorrupt(t *testing.T) {
	const expectedSectors = 50
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	assertMetrics := func(t *testing.T, physical, lost, total uint64) {
		t.Helper()

		if m, err := db.Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Storage.TotalSectors != total {
			t.Fatalf("expected %v total sectors, got %v", total, m.Storage.TotalSectors)
		} else if m.Storage.PhysicalSectors != physical {
			t.Fatalf("expected %v used sectors, got %v", physical, m.Storage.PhysicalSectors)
		} else if m.Storage.LostSectors != lost {
			t.Fatalf("expected %v lost sectors, got %v", lost, m.Storage.LostSectors)
		}
	}

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	assertMetrics(t, 0, 0, 0)

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(context.Background(), volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	assertVolume := func(t *testing.T, volumeID int64, used, total uint64) {
		t.Helper()

		if vol, err := vm.Volume(volumeID); err != nil {
			t.Fatal(err)
		} else if vol.Status != storage.VolumeStatusReady {
			t.Fatal("volume should be ready")
		} else if vol.UsedSectors != used {
			t.Fatalf("expected %v used sectors, got %v", used, vol.UsedSectors)
		} else if vol.TotalSectors != total {
			t.Fatalf("expected %v total sectors, got %v", total, vol.TotalSectors)
		}
	}

	assertVolume(t, volume.ID, 0, expectedSectors)
	assertMetrics(t, 0, 0, expectedSectors)

	for i := 0; i < 10; i++ {
		if _, err := storeRandomSector(vm, 1); err != nil {
			t.Fatal(err)
		}
	}

	assertMetrics(t, 10, 0, expectedSectors)
	assertVolume(t, volume.ID, 10, expectedSectors)

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		// blocking error should be nil
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// check that the metrics did not change
	assertMetrics(t, 10, 0, expectedSectors)
	assertVolume(t, volume.ID, 10, expectedSectors)

	f, err := os.OpenFile(volumePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// corrupt a sector in the volume
	n := rhp2.SectorSize * frand.Intn(10)
	f.WriteAt(frand.Bytes(512), int64(n))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// add a second volume to accept the data
	volume2, err := vm.AddVolume(context.Background(), filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// check that the total metrics doubled, but the volume metrics are unchanged
	assertMetrics(t, 10, 0, expectedSectors*2)
	assertVolume(t, volume.ID, 10, expectedSectors)
	assertVolume(t, volume2.ID, 0, expectedSectors)

	// remove the volume
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		t.Fatal(err) // blocking error should be nil
	} else if err := <-result; err == nil {
		t.Fatal("expected error when removing corrupt volume", err)
	} else if !errors.Is(err, storage.ErrMigrationFailed) {
		t.Fatalf("expected ErrMigrationFailed, got %v", err)
	}

	// check that only the one failed sector is left in the original volume
	assertMetrics(t, 10, 0, expectedSectors*2)
	assertVolume(t, volume.ID, 1, expectedSectors)
	assertVolume(t, volume2.ID, 9, expectedSectors)

	// force remove the volume
	if err := vm.RemoveVolume(context.Background(), volume.ID, true, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(volumePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("volume file still exists", err)
	}

	// check that the corrupt sector was removed from the volume metrics
	assertMetrics(t, 9, 1, expectedSectors)
	assertVolume(t, volume2.ID, 9, expectedSectors)

	if _, err := vm.Volume(volume.ID); !errors.Is(err, storage.ErrVolumeNotFound) {
		t.Fatalf("expected ErrVolumeNotFound, got %v", err)
	}
}

func TestRemoveMissing(t *testing.T) {
	const expectedSectors = 50
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	assertMetrics := func(t *testing.T, physical, lost, total uint64) {
		t.Helper()

		if m, err := db.Metrics(time.Now()); err != nil {
			t.Fatal(err)
		} else if m.Storage.TotalSectors != total {
			t.Fatalf("expected %v total sectors, got %v", total, m.Storage.TotalSectors)
		} else if m.Storage.PhysicalSectors != physical {
			t.Fatalf("expected %v used sectors, got %v", physical, m.Storage.PhysicalSectors)
		} else if m.Storage.LostSectors != lost {
			t.Fatalf("expected %v lost sectors, got %v", lost, m.Storage.LostSectors)
		}
	}

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	assertMetrics(t, 0, 0, 0)

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(context.Background(), volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	assertVolume := func(t *testing.T, volumeID int64, used, total uint64) {
		t.Helper()

		if vol, err := vm.Volume(volumeID); err != nil {
			t.Fatal(err)
		} else if vol.UsedSectors != used {
			t.Fatalf("expected %v used sectors, got %v", used, vol.UsedSectors)
		} else if vol.TotalSectors != total {
			t.Fatalf("expected %v total sectors, got %v", total, vol.TotalSectors)
		}
	}

	assertMetrics(t, 0, 0, expectedSectors)
	assertVolume(t, volume.ID, 0, expectedSectors)

	assertVolume(t, volume.ID, 0, expectedSectors)
	assertMetrics(t, 0, 0, expectedSectors)

	for i := 0; i < 10; i++ {
		if _, err := storeRandomSector(vm, 1); err != nil {
			t.Fatal(err)
		}
	}

	assertMetrics(t, 10, 0, expectedSectors)
	assertVolume(t, volume.ID, 10, expectedSectors)

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// check that the volume metrics did not change
	assertMetrics(t, 10, 0, expectedSectors)
	assertVolume(t, volume.ID, 10, expectedSectors)

	// close the volume manager
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}

	// remove the volume from disk
	if err := os.Remove(volumePath); err != nil {
		t.Fatal(err)
	}

	// reload the volume manager
	vm, err = storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	// check that the volume metrics did not change
	assertMetrics(t, 10, 0, expectedSectors)
	assertVolume(t, volume.ID, 10, expectedSectors)

	// add a volume to accept the data
	volume2, err := vm.AddVolume(context.Background(), filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// check that the total metrics doubled and the volume metrics did not change
	assertMetrics(t, 10, 0, expectedSectors*2)
	assertVolume(t, volume.ID, 10, expectedSectors)
	assertVolume(t, volume2.ID, 0, expectedSectors)

	// remove the volume
	if err := vm.RemoveVolume(context.Background(), volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrMigrationFailed) {
		t.Fatalf("expected ErrMigrationFailed, got %v", err)
	}

	// check that the volume metrics did not change
	assertMetrics(t, 10, 0, expectedSectors*2)
	assertVolume(t, volume.ID, 10, expectedSectors)
	assertVolume(t, volume2.ID, 0, expectedSectors)

	// force remve the volume
	if err := vm.RemoveVolume(context.Background(), volume.ID, true, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// check that the sectors were marked as lost
	assertMetrics(t, 0, 10, expectedSectors)
	assertVolume(t, volume2.ID, 0, expectedSectors)

	if _, err := vm.Volume(volume.ID); !errors.Is(err, storage.ErrVolumeNotFound) {
		t.Fatalf("expected ErrVolumeNotFound, got %v", err)
	}
}

func TestVolumeConcurrency(t *testing.T) {
	t.Skip("This test is flaky and needs to be fixed")

	const (
		sectors      = 256
		writeSectors = 10
	)
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	result := make(chan error, 1)
	volume, err := vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	}

	for {
		// reload the volume, since the initialization progress will have changed
		v, err := vm.Volume(volume.ID)
		if err != nil {
			t.Fatal(err)
		}
		volume = v.Volume
		// wait for enough sectors to be initialized
		if volume.TotalSectors >= writeSectors {
			break
		}
	}

	// add a few sectors while the volume is initializing
	roots := make([]types.Hash256, writeSectors)
	for i := range roots {
		roots[i], err = storeRandomSector(vm, 1)
		if err != nil {
			t.Fatal(i, err)
		}
	}

	// read the sectors back
	for _, root := range roots {
		sector, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(sector) != root {
			t.Fatal("sector was corrupted")
		}
	}

	// wait for the volume to finish initializing
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	// read the sectors back
	for _, root := range roots {
		sector, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(sector) != root {
			t.Fatal("sector was corrupted")
		}
	}

	// refresh the volume, since initialization should be complete
	v, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	volume = v.Volume

	// check the volume
	if err := checkFileSize(volumeFilePath, int64(sectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != writeSectors {
		t.Fatalf("expected %v used sectors, got %v", writeSectors, volume.UsedSectors)
	}

	// generate a random sector
	var sector [rhp2.SectorSize]byte
	_, _ = frand.Read(sector[:256])
	root := rhp2.SectorRoot(&sector)

	// shrink the volume so it is nearly full
	const newSectors = writeSectors + 5
	result = make(chan error, 1)
	if err := vm.ResizeVolume(context.Background(), volume.ID, newSectors, result); err != nil {
		t.Fatal(err)
	}

	// try to write a sector to the volume, which should fail
	if err := vm.Write(root, &sector); !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected %v, got %v", storage.ErrNotEnoughStorage, err)
	}

	// wait for the volume to finish shrinking
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	// reload the volume, since shrinking should be complete
	v, err = vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	volume = v.Volume

	// check the volume
	if err := checkFileSize(volumeFilePath, int64((newSectors)*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != newSectors {
		t.Fatalf("expected %v total sectors, got %v", newSectors, volume.TotalSectors)
	} else if volume.UsedSectors != writeSectors {
		t.Fatalf("expected %v used sectors, got %v", writeSectors, volume.UsedSectors)
	}

	// write the sector again, which should succeed
	if err := vm.Write(root, &sector); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeGrow(t *testing.T) {
	const initialSectors = 20
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(context.Background(), volumeFilePath, initialSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// reload the volume, since the initialization progress will have changed
	v, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	volume = v.Volume

	// check the volume
	if err := checkFileSize(volumeFilePath, int64(initialSectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	// fill the volume
	roots := make([]types.Hash256, 0, initialSectors)
	for i := 0; i < int(initialSectors); i++ {
		root, err := storeRandomSector(vm, 1)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}

	// grow the volume
	const newSectors = 100
	if err := vm.ResizeVolume(context.Background(), volume.ID, newSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// check the volume
	if err := checkFileSize(volumeFilePath, int64(newSectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	}

	meta, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if meta.TotalSectors != newSectors {
		t.Fatalf("expected %v total sectors, got %v", newSectors, meta.TotalSectors)
	} else if meta.UsedSectors != uint64(len(roots)) {
		t.Fatalf("expected %v used sectors, got %v", len(roots), meta.UsedSectors)
	}

	f, err := os.Open(volumeFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var sector [rhp2.SectorSize]byte
	for _, root := range roots {
		if _, err := io.ReadFull(f, sector[:]); err != nil {
			t.Fatal(err)
		} else if rhp2.SectorRoot(&sector) != root {
			t.Fatal("sector was corrupted")
		}
	}
}

func TestVolumeShrink(t *testing.T) {
	const sectors = 101
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	volume, err := vm.Volume(vol.ID)
	if err != nil {
		t.Fatal(err)
	}

	// check the volume
	if err := checkFileSize(volumeFilePath, int64(sectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		root, err := storeRandomSector(vm, uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)

		// validate the volume stats are correct
		volumes, err := vm.Volumes()
		if err != nil {
			t.Fatal(err)
		}
		if volumes[0].UsedSectors != uint64(i+1) {
			t.Fatalf("expected %v used sectors, got %v", i+1, volumes[0].UsedSectors)
		}
	}

	sectorLocation := func(root types.Hash256) (storage.SectorLocation, error) {
		loc, err := db.SectorLocation(root)
		if err != nil {
			return loc, err
		}
		return loc, nil
	}

	// validate that each sector was stored in the expected location
	for i, root := range roots {
		loc, err := sectorLocation(root)
		if err != nil {
			t.Fatal(err)
		}
		if loc.Volume != volume.ID {
			t.Fatal(err)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector %v to be at index %v, got %v", root, i, loc.Index)
		}
	}

	// try to shrink the volume, should fail since no space is available
	toRemove := sectors * 2 / 3
	remainingSectors := uint64(sectors - toRemove)
	if err := vm.ResizeVolume(context.Background(), volume.ID, remainingSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// remove some sectors from the beginning of the volume
	for _, root := range roots[:toRemove] {
		if err := vm.RemoveSector(root); err != nil {
			t.Fatal(err)
		}
	}

	// when shrinking, the roots after the target size should be moved to
	// the beginning of the volume
	roots = roots[toRemove:]

	// shrink the volume by the number of sectors removed, should succeed
	if err := vm.ResizeVolume(context.Background(), volume.ID, remainingSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(remainingSectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	}

	meta, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if meta.TotalSectors != remainingSectors {
		t.Fatalf("expected %v total sectors, got %v", remainingSectors, meta.TotalSectors)
	} else if meta.UsedSectors != remainingSectors {
		t.Fatalf("expected %v used sectors, got %v", remainingSectors, meta.UsedSectors)
	}

	// validate that the sectors were moved to the beginning of the volume
	for i, root := range roots {
		loc, err := sectorLocation(root)
		if err != nil {
			t.Fatal(err)
		}
		if loc.Volume != volume.ID {
			t.Fatal(err)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector %v to be at index %v, got %v", root, i, loc.Index)
		}
	}
}

func TestVolumeManagerReadWrite(t *testing.T) {
	const sectors = 10
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	volume, err := vm.Volume(vol.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := checkFileSize(volumeFilePath, int64(sectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		root, err := storeRandomSector(vm, 1)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)

		// validate the volume stats are correct
		volumes, err := vm.Volumes()
		if err != nil {
			t.Fatal(err)
		}
		if volumes[0].UsedSectors != uint64(i+1) {
			t.Fatalf("expected %v used sectors, got %v", i+1, volumes[0].UsedSectors)
		}
	}

	// read the sectors back
	frand.Shuffle(len(roots), func(i, j int) { roots[i], roots[j] = roots[j], roots[i] })
	for _, root := range roots {
		sector, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		}
		retrievedRoot := rhp2.SectorRoot(sector)
		if retrievedRoot != root {
			t.Fatalf("expected root %v, got %v", root, retrievedRoot)
		}
	}
}

func storeRandomSector(vm *storage.VolumeManager, expiration uint64) (types.Hash256, error) {
	var sector [rhp2.SectorSize]byte
	if _, err := frand.Read(sector[:256]); err != nil {
		return types.Hash256{}, fmt.Errorf("failed to generate random sector: %w", err)
	}
	root := rhp2.SectorRoot(&sector)
	err := vm.Write(root, &sector)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to write sector: %w", err)
	}

	err = vm.AddTemporarySectors([]storage.TempSector{{Root: root, Expiration: expiration}})
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to add temporary sector: %w", err)
	}
	return root, nil
}

func TestSectorCache(t *testing.T) {
	const sectors = 10
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")), storage.WithCacheSize(sectors/2)) // cache half the sectors
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	volume, err := vm.Volume(vol.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := checkFileSize(volumeFilePath, int64(sectors*rhp2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		root, err := storeRandomSector(vm, uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)

		// validate the volume stats are correct
		volumes, err := vm.Volumes()
		if err != nil {
			t.Fatal(err)
		} else if volumes[0].UsedSectors != uint64(i+1) {
			t.Fatalf("expected %v used sectors, got %v", i+1, volumes[0].UsedSectors)
		}
	}

	// read the last 5 sectors all sectors should be cached
	for i, root := range roots[5:] {
		_, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		}

		hits, misses := vm.CacheStats()
		if hits != uint64(i+1) {
			t.Fatalf("expected %v cache hits, got %v", i+1, hits)
		} else if misses != 0 {
			t.Fatalf("expected 0 cache misses, got %v", misses)
		}
	}

	// read the first 5 sectors all sectors should be missed
	for i, root := range roots[:5] {
		_, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		}

		hits, misses := vm.CacheStats()
		if hits != 5 {
			t.Fatalf("expected 5 cache hits, got %v", hits) // existing 5 cache hits
		} else if misses != uint64(i+1) {
			t.Fatalf("expected %v cache misses, got %v", i+1, misses)
		}
	}

	// read the first 5 sectors again all sectors should be cached
	for i, root := range roots[:5] {
		_, err := vm.ReadSector(root)
		if err != nil {
			t.Fatal(err)
		}

		expectedHits := 5 + (uint64(i) + 1) // 5 original hits, plus the new hit
		hits, misses := vm.CacheStats()
		if hits != expectedHits {
			t.Fatalf("expected %d cache hits, got %v", expectedHits, hits)
		} else if misses != 5 {
			t.Fatalf("expected %v cache misses, got %v", 5, misses) // existing 5 cache misses
		}
	}
}

func TestStoragePrune(t *testing.T) {
	const sectors = 10
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")), storage.WithCacheSize(0), storage.WithPruneInterval(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	assertUsedSectors := func(t *testing.T, used uint64) {
		t.Helper()

		time.Sleep(2 * time.Second) // note: longer than prune interval for timing issues
		volume, err := vm.Volume(vol.ID)
		if err != nil {
			t.Fatal(err)
		} else if volume.UsedSectors != used {
			t.Fatalf("expected %v used sectors, got %v", used, volume.UsedSectors)
		}

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		} else if m.Storage.PhysicalSectors != used {
			t.Fatalf("expected %v used sectors, got %v", used, m.Storage.PhysicalSectors)
		}
	}

	assertUsedSectors(t, 0)

	storeRandomSector := func(t *testing.T, expiration uint64) types.Hash256 {
		t.Helper()

		var sector [rhp2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal("failed to generate random sector:", err)
		}
		root := rhp2.SectorRoot(&sector)
		if err = vm.StoreSector(root, &sector, expiration); err != nil {
			t.Fatal("failed to store sector:", err)
		}
		return root
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		storeRandomSector(t, uint64(i+1))
	}

	// ensure the sectors are not pruned immediately
	assertUsedSectors(t, sectors)

	// expire half the sectors
	if err := db.ExpireTempSectors(5); err != nil {
		t.Fatal(err)
	}
	assertUsedSectors(t, 5)

	// expire the remaining sectors
	if err := db.ExpireTempSectors(10); err != nil {
		t.Fatal(err)
	}
	assertUsedSectors(t, 0)
}

func BenchmarkVolumeManagerWrite(b *testing.B) {
	dir := b.TempDir()

	// create the database
	log := zaptest.NewLogger(b)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(context.Background(), volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	sectors := make([][rhp2.SectorSize]byte, b.N)
	roots := make([]types.Hash256, b.N)
	for i := range sectors {
		frand.Read(sectors[i][:256])
		roots[i] = rhp2.SectorRoot(&sectors[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhp2.SectorSize)

	// fill the volume
	for i := 0; i < b.N; i++ {
		root, sector := roots[i], sectors[i]
		err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		}
	}
}

func BenchmarkNewVolume(b *testing.B) {
	const sectors = 100
	dir := b.TempDir()

	// create the database
	log := zaptest.NewLogger(b)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	b.ResetTimer()
	b.ReportMetric(float64(sectors), "sectors")
	b.SetBytes(sectors * rhp2.SectorSize)

	result := make(chan error, 1)
	for i := 0; i < b.N; i++ {
		volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
		_, err = vm.AddVolume(context.Background(), volumeFilePath, sectors, result)
		if err != nil {
			b.Fatal(err)
		} else if err := <-result; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVolumeManagerRead(b *testing.B) {
	dir := b.TempDir()

	// create the database
	log := zaptest.NewLogger(b)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(context.Background(), volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	// fill the volume
	written := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhp2.SectorRoot(&sector)
		err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		}
		written = append(written, root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhp2.SectorSize)
	// read the sectors back
	for _, root := range written {
		if _, err := vm.ReadSector(root); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVolumeRemove(b *testing.B) {
	dir := b.TempDir()

	// create the database
	log := zaptest.NewLogger(b)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// initialize the storage manager
	vm, err := storage.NewVolumeManager(db, storage.WithLogger(log.Named("volumes")))
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	volume1, err := vm.AddVolume(context.Background(), volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	// fill the volume
	for i := 0; i < b.N; i++ {
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhp2.SectorRoot(&sector)
		err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		}
	}

	// add a new volume
	volume2FilePath := filepath.Join(b.TempDir(), "hostdata2.dat")
	_, err = vm.AddVolume(context.Background(), volume2FilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhp2.SectorSize)

	// migrate the sectors
	if err := vm.RemoveVolume(context.Background(), volume1.ID, false, result); err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}
}
