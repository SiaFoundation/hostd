package storage_test

import (
	"errors"
	"path/filepath"
	"testing"

	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"lukechampine.com/frand"
)

func TestChunks(t *testing.T) {
	start := 50
	add := 100
	end := start + add
	n := 256
	for i := end; i > start; i -= n {
		if start > i-n {
			n = i - start
		}
		t.Log(start, end, i-n)
	}
	t.Fail()
}

func TestAddVolume(t *testing.T) {
	const expectedSectors = (1 << 40) / (1 << 22) // 1 TiB
	dir := t.TempDir()

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sectorStore := sqlite.NewSectorStore(db)
	volumeStore := sqlite.NewVolumeStore(db)

	manager := storage.NewManager(volumeStore, sectorStore)

	volume, err := manager.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors)
	if err != nil {
		t.Fatal(err)
	}

	volumes, err := manager.Volumes()
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
	case volumes[0].MaxSectors != expectedSectors:
		t.Fatalf("expected %v max sectors, got %v", expectedSectors, volumes[0].MaxSectors)
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
	sectorStore := sqlite.NewSectorStore(db)
	volumeStore := sqlite.NewVolumeStore(db)
	manager := storage.NewManager(volumeStore, sectorStore)

	volume, err := manager.AddVolume(filepath.Join(t.TempDir(), "hostdata.dat"), expectedSectors)
	if err != nil {
		t.Fatal(err)
	}

	sector := make([]byte, 1<<22)
	if _, err := frand.Read(sector[:256]); err != nil {
		t.Fatal(err)
	}
	root := storage.SectorRoot(merkle.SectorRoot(sector))

	// write the sector
	loc, release, err := manager.WriteSector(root, sector)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if loc.Volume != volume.ID {
		t.Fatalf("expected volume %v, got %v", volume.ID, loc.Volume)
	} else if loc.Index != 0 {
		t.Fatalf("expected index 0, got %v", loc.Index)
	}
	// add the sector's metadata
	err = sectorStore.Update(func(tx storage.SectorUpdateTransaction) error {
		return tx.AddSectorMetadata(root, loc)
	})
	if err != nil {
		t.Fatal(err)
	}

	// attempt to remove the volume. Should return ErrNotEnoughStorage since
	// there is only one volume.
	if err := manager.RemoveVolume(volume.ID, false); !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// remove the sector
	if err := manager.RemoveSector(root); err != nil {
		t.Fatal(err)
	}

	// remove the volume
	if err := manager.RemoveVolume(volume.ID, false); err != nil {
		t.Fatal(err)
	}
}
