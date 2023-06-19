package storage_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/chain"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

const sectorCacheSize = 64

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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volume, err := vm.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// write a sector
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	root := rhpv2.SectorRoot(&sector)
	release, err := vm.Write(root, &sector)
	if err != nil {
		t.Fatal(err)
	}
	release()

	// close the volume manager
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen the volume manager
	vm, err = storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
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
	sector2, err := vm.Read(root)
	if err != nil {
		t.Fatal(err)
	} else if *sector2 != sector {
		t.Fatal("sector was corrupted")
	}

	// write a new sector
	frand.Read(sector[:])
	root = rhpv2.SectorRoot(&sector)
	release, err = vm.Write(root, &sector)
	if err != nil {
		t.Fatal(err)
	} else if err := release(); err != nil {
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volume, err := vm.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors, result)
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	var sector [rhpv2.SectorSize]byte
	if _, err := frand.Read(sector[:256]); err != nil {
		t.Fatal(err)
	}
	root := rhpv2.SectorRoot(&sector)

	// write the sector
	release, err := vm.Write(root, &sector)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
		// blocking error should be nil
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		// async error should be ErrNotEnoughStorage
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// remove the sector
	if err := vm.RemoveSector(root); err != nil {
		t.Fatal(err)
	}

	// remove the volume
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		var sector [rhpv2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := rhpv2.SectorRoot(&sector)

		// write the sector
		release, err := vm.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
		// blocking error should be nil
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		// async error should be ErrNotEnoughStorage
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// add a second volume to the manager
	_, err = vm.AddVolume(filepath.Join(t.TempDir(), "vol2.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(volumePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// corrupt a sector in the volume
	n := rhpv2.SectorSize * frand.Intn(10)
	f.WriteAt(frand.Bytes(512), int64(n))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// remove the volume
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
		t.Fatal(err) // blocking error should be nil
	} else if err := <-result; err == nil {
		t.Fatal("expected error when removing corrupt volume", err)
	}
	// force remove the volume
	if err := vm.RemoveVolume(volume.ID, true, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(volumePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("volume file still exists", err)
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumePath, expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		var sector [rhpv2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := rhpv2.SectorRoot(&sector)

		// write the sector
		release, err := vm.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// add a second volume to the manager
	_, err = vm.AddVolume(filepath.Join(t.TempDir(), "vol2.dat"), expectedSectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// close the volume manager
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}

	// remove the volume from disk
	if err := os.Remove(volumePath); err != nil {
		t.Fatal(err)
	}

	// reload the volume manager
	vm, err = storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	vol, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if vol.Status != storage.VolumeStatusUnavailable {
		t.Fatal("volume should be unavailable")
	}

	// remove the volume
	if err := vm.RemoveVolume(volume.ID, false, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err == nil {
		t.Fatal("expected error when removing missing volume")
	}

	if err := vm.RemoveVolume(volume.ID, true, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestVolumeGrow(t *testing.T) {
	const initialSectors = 32
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	volume, err := vm.AddVolume(volumeFilePath, initialSectors, result)
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
	if err := checkFileSize(volumeFilePath, int64(initialSectors*rhpv2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	// grow the volume
	const newSectors = 64
	if err := vm.ResizeVolume(volume.ID, newSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	if err := checkFileSize(volumeFilePath, int64(newSectors*rhpv2.SectorSize)); err != nil {
		t.Fatal(err)
	}

	meta, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if meta.TotalSectors != newSectors {
		t.Fatalf("expected %v total sectors, got %v", newSectors, meta.TotalSectors)
	} else if meta.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", meta.UsedSectors)
	} else if meta.Status != storage.VolumeStatusReady {
		t.Fatalf("expected volume status to be ready, got %v", meta.Status)
	}
}

func TestVolumeShrink(t *testing.T) {
	const sectors = 32
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(volumeFilePath, sectors, result)
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
	if err := checkFileSize(volumeFilePath, int64(sectors*rhpv2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		var sector [rhpv2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := rhpv2.SectorRoot(&sector)
		release, err := vm.Write(root, &sector)
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
	if err := vm.ResizeVolume(volume.ID, remainingSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; !errors.Is(err, storage.ErrNotEnoughStorage) {
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
	if err := vm.ResizeVolume(volume.ID, remainingSectors, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	} else if err := checkFileSize(volumeFilePath, int64(remainingSectors*rhpv2.SectorSize)); err != nil {
		t.Fatal(err)
	}

	meta, err := vm.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if meta.TotalSectors != remainingSectors {
		t.Fatalf("expected %v total sectors, got %v", remainingSectors, meta.TotalSectors)
	} else if meta.UsedSectors != remainingSectors {
		t.Fatalf("expected %v used sectors, got %v", remainingSectors, meta.UsedSectors)
	} else if meta.Status != storage.VolumeStatusReady {
		t.Fatalf("expected volume status to be ready, got %v", meta.Status)
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
	const sectors = 10
	dir := t.TempDir()

	// create the database
	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(t.TempDir(), "hostdata.dat")
	vol, err := vm.AddVolume(volumeFilePath, sectors, result)
	if err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	volume, err := vm.Volume(vol.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := checkFileSize(volumeFilePath, int64(sectors*rhpv2.SectorSize)); err != nil {
		t.Fatal(err)
	} else if volume.TotalSectors != sectors {
		t.Fatalf("expected %v total sectors, got %v", sectors, volume.TotalSectors)
	} else if volume.UsedSectors != 0 {
		t.Fatalf("expected 0 used sectors, got %v", volume.UsedSectors)
	}

	roots := make([]types.Hash256, 0, sectors)
	// fill the volume
	for i := 0; i < cap(roots); i++ {
		var sector [rhpv2.SectorSize]byte
		if _, err := frand.Read(sector[:256]); err != nil {
			t.Fatal(err)
		}
		root := rhpv2.SectorRoot(&sector)
		release, err := vm.Write(root, &sector)
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
		retrievedRoot := rhpv2.SectorRoot(sector)
		if retrievedRoot != root {
			t.Fatalf("expected root %v, got %v", root, retrievedRoot)
		}
	}
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		b.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	sectors := make([][rhpv2.SectorSize]byte, b.N)
	roots := make([]types.Hash256, b.N)
	for i := range sectors {
		frand.Read(sectors[i][:256])
		roots[i] = rhpv2.SectorRoot(&sectors[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhpv2.SectorSize)

	// fill the volume
	for i := 0; i < b.N; i++ {
		root, sector := roots[i], sectors[i]
		release, err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		} else if err := release(); err != nil {
			b.Fatal(i, err)
		}
	}
}

func BenchmarkNewVolume(b *testing.B) {
	dir := b.TempDir()

	// create the database
	log := zaptest.NewLogger(b)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		b.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	b.ResetTimer()
	b.ReportMetric(float64(b.N), "sectors")
	b.SetBytes(rhpv2.SectorSize)

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		b.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	_, err = vm.AddVolume(volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	// fill the volume
	written := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhpv2.SectorRoot(&sector)
		release, err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		} else if err := release(); err != nil {
			b.Fatal(i, err)
		}
		written = append(written, root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhpv2.SectorSize)
	// read the sectors back
	for _, root := range written {
		if _, err := vm.Read(root); err != nil {
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

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	select {
	case err := <-errCh:
		b.Fatal(err)
	default:
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		b.Fatal(err)
	}
	defer cm.Close()

	// initialize the storage manager
	am := alerts.NewManager()
	vm, err := storage.NewVolumeManager(db, am, cm, log.Named("volumes"), sectorCacheSize)
	if err != nil {
		b.Fatal(err)
	}
	defer vm.Close()

	result := make(chan error, 1)
	volumeFilePath := filepath.Join(b.TempDir(), "hostdata.dat")
	volume1, err := vm.AddVolume(volumeFilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	// fill the volume
	for i := 0; i < b.N; i++ {
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhpv2.SectorRoot(&sector)
		release, err := vm.Write(root, &sector)
		if err != nil {
			b.Fatal(i, err)
		} else if err := release(); err != nil {
			b.Fatal(i, err)
		}
	}

	// add a new volume
	volume2FilePath := filepath.Join(b.TempDir(), "hostdata2.dat")
	_, err = vm.AddVolume(volume2FilePath, uint64(b.N), result)
	if err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhpv2.SectorSize)

	// migrate the sectors
	if err := vm.RemoveVolume(volume1.ID, false, result); err != nil {
		b.Fatal(err)
	} else if err := <-result; err != nil {
		b.Fatal(err)
	}
}
