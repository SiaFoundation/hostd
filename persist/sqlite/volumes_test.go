package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

// addVolume is a helper to add a new volume to the database
func addVolume(db *Store, name string, size uint64) (storage.Volume, error) {
	volumeID, err := db.AddVolume(name, false)
	if err != nil {
		return storage.Volume{}, fmt.Errorf("failed to add volume: %w", err)
	} else if err := db.GrowVolume(volumeID, size); err != nil {
		return storage.Volume{}, fmt.Errorf("failed to grow volume: %w", err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		return storage.Volume{}, fmt.Errorf("failed to set volume available: %w", err)
	}
	return db.Volume(volumeID)
}

func TestVolumeSetReadOnly(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// add a volume that is available and writable
	volume, err := addVolume(db, "test", 10)
	if err != nil {
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
	if err := db.SetReadOnly(volume.ID, true); err != nil {
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
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
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

	volumes, err = db.Volumes()
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %v", len(volumes))
	} else if volumes[0].UsedSectors != 1 {
		t.Fatalf("expected 1 used sector, got %v", volumes[0].UsedSectors)
	}

	// try to store another sector in the volume, should return
	// ErrNotEnoughStorage
	_, err = db.StoreSector(frand.Entropy256(), func(storage.SectorLocation, bool) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestVolumeAdd(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
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

func TestGrowVolume(t *testing.T) {
	const initialSectors = 64
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addVolume(db, "test", initialSectors)
	if err != nil {
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
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, m.Storage.TotalSectors)
	}

	// double the number of sectors in the volume
	if err := db.GrowVolume(volume.ID, initialSectors*2); err != nil {
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
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != initialSectors*2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors*2, m.Storage.TotalSectors)
	}

	// add a second volume
	volume2, err := addVolume(db, "test2", initialSectors/2)
	if err != nil {
		t.Fatal(err)
	}

	if volume, err := db.Volume(volume2.ID); err != nil {
		t.Fatal(err)
	} else if volume.ID != volume2.ID {
		t.Fatalf("expected volume ID %v, got %v", volume2.ID, volume.ID)
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
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if n := uint64((initialSectors * 2) + (initialSectors / 2)); m.Storage.TotalSectors != n {
		t.Fatalf("expected %v total sectors, got %v", n, m.Storage.TotalSectors)
	}
}

func TestShrinkVolume(t *testing.T) {
	const initialSectors = 64
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, m.Storage.TotalSectors)
	}

	// shrink the volume by half
	if err := db.ShrinkVolume(volume.ID, initialSectors/2); err != nil {
		t.Fatal(err)
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != initialSectors/2 {
		t.Fatalf("expected %v total sectors, got %v", initialSectors/2, m.Storage.TotalSectors)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
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
	if err := db.ShrinkVolume(volume.ID, 2); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be shrunk to the used sectors
	if err := db.ShrinkVolume(volume.ID, 5); err != nil {
		t.Fatal(err)
	} else if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != 5 {
		t.Fatalf("expected %v total sectors, got %v", 5, m.Storage.TotalSectors)
	}
}

func TestRemoveVolume(t *testing.T) {
	const initialSectors = 128
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// check that the empty volume can be removed
	if err := db.RemoveVolume(volume.ID, false); err != nil {
		t.Fatal(err)
	}

	// add another volume
	volume, err = addVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		release, err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
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

	// check that the metrics were updated correctly
	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != initialSectors {
		t.Fatalf("expected %v total sectors, got %v", initialSectors, m.Storage.TotalSectors)
	} else if m.Storage.PhysicalSectors != 5 {
		t.Fatalf("expected %v used sectors, got %v", 5, m.Storage.PhysicalSectors)
	}

	// check that the volume cannot be removed
	if err := db.RemoveVolume(volume.ID, false); !errors.Is(err, storage.ErrVolumeNotEmpty) {
		t.Fatalf("expected ErrVolumeNotEmpty, got %v", err)
	}

	// check that the volume can be force removed
	if err := db.RemoveVolume(volume.ID, true); err != nil {
		t.Fatal(err)
	}

	// check that the metrics were updated correctly
	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if m.Storage.TotalSectors != 0 {
		t.Fatalf("expected %v total sectors, got %v", 0, m.Storage.TotalSectors)
	} else if m.Storage.PhysicalSectors != 0 {
		t.Fatalf("expected %v used sectors, got %v", 0, m.Storage.PhysicalSectors)
	}
}

func TestMigrateSectors(t *testing.T) {
	const initialSectors = 64
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// store enough sectors to fill the volume
	roots := make([]types.Hash256, initialSectors)
	for i := range roots {
		root := frand.Entropy256()
		roots[i] = root
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
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
	err = db.MigrateSectors(volume.ID, initialSectors/2, func(locations []storage.SectorLocation) error {
		for _, loc := range locations {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
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
		} else if loc.Volume != volume.ID {
			t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector index %v, got %v", i, loc.Index)
		} else if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// add a second volume with only a quarter of the initial space
	volume2, err := addVolume(db, "test2", initialSectors/4)
	if err != nil {
		t.Fatal(err)
	}

	// migrate the remaining sectors from the first volume; should partially complete
	err = db.MigrateSectors(volume.ID, 0, func(locations []storage.SectorLocation) error {
		if len(locations) > initialSectors/4 {
			t.Fatalf("expected only %v migrations, got %v", initialSectors/4, len(locations))
		}
		return nil
	})
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// check that volume 2 is now full
	v2, err := db.Volume(volume2.ID)
	if err != nil {
		t.Fatal(err)
	} else if v2.ID != volume2.ID {
		t.Fatalf("expected volume ID %v, got %v", volume2.ID, v2.ID)
	} else if v2.UsedSectors != v2.TotalSectors {
		t.Fatalf("expected volume to be full, got %v/%v", v2.UsedSectors, v2.TotalSectors)
	}

	// check that volume 1 was reduced by the same amount
	v1, err := db.Volume(volume.ID)
	if err != nil {
		t.Fatal(err)
	} else if v1.ID != volume.ID {
		t.Fatalf("expected volume ID %v, got %v", volume.ID, v1.ID)
	} else if v1.UsedSectors != initialSectors/4 {
		t.Fatalf("expected volume to have %v sectors, got %v/%v", initialSectors/4, v1.UsedSectors, v1.TotalSectors)
	}
}

func TestPrune(t *testing.T) {
	const sectors = 256

	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addVolume(db, "test", sectors)
	if err != nil {
		t.Fatal(err)
	}

	// store enough sectors to fill the volume
	roots := make([]types.Hash256, sectors)
	for i := range roots {
		root := frand.Entropy256()
		roots[i] = root
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
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

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	// add a contract to store the sectors
	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	c := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			UnlockConditions: contractUnlockConditions,
			ParentID:         types.FileContractID(frand.Entropy256()),
			FileContract: types.FileContract{
				UnlockHash:  types.Hash256(contractUnlockConditions.UnlockHash()),
				WindowStart: 90,
				WindowEnd:   100,
			},
		},
	}
	if err := db.AddContract(c, []types.Transaction{}, types.MaxCurrency, contracts.Usage{}, 100); err != nil {
		t.Fatal(err)
	}
	contractSectors, tempSectors, lockedSectors, deletedSectors := roots[:20], roots[20:40], roots[40:60], roots[60:]
	err = db.UpdateContract(c.Revision.ParentID, func(tx contracts.UpdateContractTransaction) error {
		for _, root := range contractSectors {
			if err := tx.AppendSector(root); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// add the temporary sectors
	var tempRoots []storage.TempSector
	for _, root := range tempSectors {
		tempRoots = append(tempRoots, storage.TempSector{
			Root:       root,
			Expiration: 100,
		})
	}
	if err := db.AddTemporarySectors(tempRoots); err != nil {
		t.Fatal(err)
	}

	// check that there are no locked sectors
	var count int
	err = db.queryRow(`SELECT COUNT(id) FROM locked_volume_sectors;`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 locked sectors, got %v", count)
	}

	// lock the remaining sectors
	var locks []int64
	for _, root := range lockedSectors {
		loc, err := sectorLocation(&dbTxn{db}, root)
		if err != nil {
			t.Fatal(err)
		}
		lockID, err := lockLocation(&dbTxn{db}, loc.ID)
		if err != nil {
			t.Fatal(err)
		}
		locks = append(locks, lockID)
	}

	err = db.queryRow(`SELECT COUNT(id) FROM locked_volume_sectors;`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	} else if count != len(locks) {
		t.Fatalf("expected %v locked sectors, got %v", len(locks), count)
	}

	// prune the sectors
	count, err = db.PruneSectors()
	if err != nil {
		t.Fatal(err)
	} else if count != len(deletedSectors) {
		t.Fatalf("expected to prune %v sectors, got %v", len(deletedSectors), count)
	}

	// next prune should be a no-op
	count, err = db.PruneSectors()
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected to prune %v sectors, got %v", 0, count)
	}

	checkConsistency := func(stored, deleted []types.Hash256) error {
		for _, root := range stored {
			if _, release, err := db.SectorLocation(root); err != nil {
				return fmt.Errorf("sector %v not found: %w", root, err)
			} else if err := release(); err != nil {
				return fmt.Errorf("failed to release sector %v: %w", root, err)
			}
		}

		for _, root := range deleted {
			if _, _, err := db.SectorLocation(root); !errors.Is(err, storage.ErrSectorNotFound) {
				return fmt.Errorf("expected ErrSectorNotFound, got %v", err)
			}
		}

		used, _, err := db.StorageUsage()
		if err != nil {
			return fmt.Errorf("failed to get storage usage: %w", err)
		} else if used != uint64(len(stored)) {
			return fmt.Errorf("expected %v sectors, got %v", len(stored), used)
		}
		return nil
	}

	if err := checkConsistency(roots[:60], deletedSectors); err != nil {
		t.Fatal(err)
	}

	// unlock locked sectors
	if err := unlockLocationBatch(&dbTxn{db}, locks...); err != nil {
		t.Fatal(err)
	}

	// prune the unlocked sectors
	count, err = db.PruneSectors()
	if err != nil {
		t.Fatal(err)
	} else if count != len(lockedSectors) {
		t.Fatalf("expected to prune %v sectors, got %v", len(lockedSectors), count)
	}

	if err := checkConsistency(roots[:40], roots[40:]); err != nil {
		t.Fatal(err)
	}

	// expire the temp sectors
	if err := db.ExpireTempSectors(100); err != nil {
		t.Fatal(err)
	}

	// prune the temp sectors
	count, err = db.PruneSectors()
	if err != nil {
		t.Fatal(err)
	} else if count != len(tempSectors) {
		t.Fatalf("expected to prune %v sectors, got %v", len(tempSectors), count)
	}

	if err := checkConsistency(roots[:20], roots[20:]); err != nil {
		t.Fatal(err)
	}

	// expire the contract sectors
	if err := db.ExpireContractSectors(c.Revision.WindowEnd + 1); err != nil {
		t.Fatal(err)
	}

	// prune the contract sectors
	count, err = db.PruneSectors()
	if err != nil {
		t.Fatal(err)
	} else if count != len(contractSectors) {
		t.Fatalf("expected to prune %v sectors, got %v", len(contractSectors), count)
	}

	if err := checkConsistency(nil, roots); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkVolumeGrow(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
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
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
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

func BenchmarkVolumeMigrate(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volume1, err := addVolume(db, "test", uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, b.N)
	for i := range roots {
		roots[i] = frand.Entropy256()
		release, err := db.StoreSector(roots[i], func(loc storage.SectorLocation, exists bool) error { return nil })
		if err != nil {
			b.Fatalf("failed to store sector %v: %v", i, err)
		} else if err := release(); err != nil {
			b.Fatal(err)
		}
	}

	_, err = addVolume(db, "test2", uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// migrate all sectors from the first volume to the second
	if err := db.MigrateSectors(volume1.ID, 0, func(locations []storage.SectorLocation) error {
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEmptyLocation(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = addVolume(db, "test", 1e6)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.transaction(func(tx txn) error {
			_, err := emptyLocation(tx)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
