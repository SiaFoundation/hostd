package sqlite

import (
	"context"
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

// addTestVolume is a helper to add a new volume to the database
func addTestVolume(db *Store, name string, size uint64) (storage.Volume, error) {
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
	volume, err := addTestVolume(db, "test", 10)
	if err != nil {
		t.Fatal(err)
	}

	// try to add a sector to the volume
	err = db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	// set the volume to read-only
	if err := db.SetReadOnly(volume.ID, true); err != nil {
		t.Fatal(err)
	}

	// try to add another sector to the volume, should fail with
	// ErrNotEnoughStorage
	err = db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation) error { return nil })
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
	err = db.StoreSector(root, func(storage.SectorLocation) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}

	// grow the volume to support a sector
	if err := db.GrowVolume(volumeID, 1); err != nil {
		t.Fatal(err)
	}
	// store the sector
	err = db.StoreSector(root, func(loc storage.SectorLocation) error {
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

	// check the location was added
	loc, err := db.SectorLocation(root)
	if err != nil {
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

	// store the sector again, should be a no-op
	err = db.StoreSector(root, func(loc storage.SectorLocation) error {
		t.Fatal("store function called twice")
		return nil
	})
	if err != nil {
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
	err = db.StoreSector(frand.Entropy256(), func(storage.SectorLocation) error { return nil })
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected ErrNotEnoughStorage, got %v", err)
	}
}

func TestHasSector(t *testing.T) {
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
	} else if err := db.GrowVolume(volumeID, 1); err != nil {
		t.Fatal(err)
	}

	root := frand.Entropy256()
	// store the sector
	err = db.StoreSector(root, func(loc storage.SectorLocation) error {
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

	// check the location was added
	loc, err := db.SectorLocation(root)
	if err != nil {
		t.Fatal(err)
	}

	if loc.Volume != volumeID {
		t.Fatalf("expected volume ID %v, got %v", volumeID, loc.Volume)
	} else if loc.Index != 0 {
		t.Fatalf("expected sector index 0, got %v", loc.Index)
	}

	// the sector should not exist since it is not referenced
	exists, err := db.HasSector(root)
	if err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected sector to not exist")
	}

	// add a temporary sector
	if err = db.AddTempSector(root, 1); err != nil {
		t.Fatal(err)
	}

	// the sector should now exist since it is referenced by a temp sector
	exists, err = db.HasSector(root)
	if err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Fatal("expected sector to exist")
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
		} else if volume.ID != int64(i) {
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

	volume, err := addTestVolume(db, "test", initialSectors)
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
	volume2, err := addTestVolume(db, "test2", initialSectors/2)
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

	volume, err := addTestVolume(db, "test", initialSectors)
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
		err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index %v, got %v", i, loc.Index)
			}
			return nil
		})
		if err != nil {
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

	volume, err := addTestVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// check that the empty volume can be removed
	if err := db.RemoveVolume(volume.ID, false); err != nil {
		t.Fatal(err)
	}

	// add another volume
	volume, err = addTestVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// add a few sectors
	for i := 0; i < 5; i++ {
		sectorRoot := frand.Entropy256()
		err := db.StoreSector(sectorRoot, func(loc storage.SectorLocation) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index 0, got %v", loc.Index)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = db.AddTemporarySectors([]storage.TempSector{{Root: sectorRoot, Expiration: uint64(i)}})
		if err != nil {
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

	// expire all of the temporary sectors
	if err := db.ExpireTempSectors(5); err != nil {
		t.Fatal(err)
	} else if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// check that the volume can be removed
	if err := db.RemoveVolume(volume.ID, false); err != nil {
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

	volume, err := addTestVolume(db, "test", initialSectors)
	if err != nil {
		t.Fatal(err)
	}

	// store enough sectors to fill the volume
	roots := make([]types.Hash256, initialSectors)
	for i := range roots {
		root := frand.Entropy256()
		roots[i] = root
		err := db.StoreSector(root, func(loc storage.SectorLocation) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index %v, got %v", i, loc.Index)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		} else if err := db.AddTemporarySectors([]storage.TempSector{{Root: root, Expiration: uint64(i)}}); err != nil {
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
	// migrate the remaining sectors to the first half of the volume
	var i int
	migrated, failed, err := db.MigrateSectors(context.Background(), volume.ID, initialSectors/2, func(from, to storage.SectorLocation) error {
		if to.Volume != volume.ID {
			t.Fatalf("expected volume ID %v, got %v", volume.ID, to.Volume)
		} else if to.Index != uint64(i) {
			t.Fatalf("expected sector index %v, got %v", i, to.Index)
		} else if to.Root != roots[i] {
			t.Fatalf("expected sector root index %d %v, got %v", i, roots[i], to.Root)
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	} else if i != 32 {
		t.Fatalf("expected 32 sectors, got %v", i)
	} else if migrated != 32 {
		t.Fatalf("expected 32 migrated sectors, got %v", migrated)
	} else if failed != 0 {
		t.Fatalf("expected 0 failed sectors, got %v", failed)
	}

	// check that the sector metadata has been updated
	for i, root := range roots {
		if loc, err := db.SectorLocation(root); err != nil {
			t.Fatal(err)
		} else if loc.Volume != volume.ID {
			t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
		} else if loc.Index != uint64(i) {
			t.Fatalf("expected sector index %v, got %v", i, loc.Index)
		}
	}

	// add a second volume with only a quarter of the initial space
	volume2, err := addTestVolume(db, "test2", initialSectors/4)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetReadOnly(volume.ID, true); err != nil { // set the volume to read-onl
		t.Fatal(err)
	}

	// migrate the remaining sectors from the first volume; should partially complete
	migrated, failed, err = db.MigrateSectors(context.Background(), volume.ID, 0, func(from, to storage.SectorLocation) error {
		return nil
	})
	if !errors.Is(err, storage.ErrNotEnoughStorage) {
		t.Fatalf("expected not enough storage error, got %v", err)
	} else if migrated != initialSectors/4 {
		t.Fatalf("expected %v migrated sectors, got %v", initialSectors/4, migrated)
	} else if failed != 0 {
		t.Fatalf("expected %v failed sectors, got %v", initialSectors-(initialSectors/4), failed)
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
	const sectors = 75

	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	volume, err := addTestVolume(db, "test", sectors)
	if err != nil {
		t.Fatal(err)
	}

	// store enough sectors to fill the volume
	roots := make([]types.Hash256, 0, sectors)
	for i := 0; i < sectors; i++ {
		root := frand.Entropy256()
		err := db.StoreSector(root, func(loc storage.SectorLocation) error {
			if loc.Volume != volume.ID {
				t.Fatalf("expected volume ID %v, got %v", volume.ID, loc.Volume)
			} else if loc.Index != uint64(i) {
				t.Fatalf("expected sector index %v, got %v", i, loc.Index)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	addContract := func(t *testing.T, expiration uint64) contracts.SignedRevision {
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
				ParentID:         types.FileContractID(frand.Entropy256()),
				UnlockConditions: contractUnlockConditions,
				FileContract: types.FileContract{
					UnlockHash:  contractUnlockConditions.UnlockHash(),
					WindowStart: expiration,
					WindowEnd:   expiration,
				},
			},
		}
		if err := db.AddContract(c, []types.Transaction{}, types.MaxCurrency, contracts.Usage{}, 100); err != nil {
			t.Fatal(err)
		}
		return c
	}

	addSectorsToContract := func(t *testing.T, c *contracts.SignedRevision, sectors []types.Hash256) {
		// append the contract sectors to the contract
		var changes []contracts.SectorChange
		for _, root := range sectors {
			changes = append(changes, contracts.SectorChange{
				Root:   root,
				Action: contracts.SectorActionAppend,
			})
		}
		err = db.ReviseContract(*c, []types.Hash256{}, contracts.Usage{}, changes)
		if err != nil {
			t.Fatal(err)
		}
	}

	contract1Sectors, contract2Sectors, tempSectors, unreferencedSectors := roots[:10], roots[10:25], roots[25:50], roots[50:]

	c1 := addContract(t, 100)
	c2 := addContract(t, 110)

	// add the sectors to the contract
	addSectorsToContract(t, &c1, contract1Sectors)
	addSectorsToContract(t, &c2, contract2Sectors)

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

	assertSectors := func(t *testing.T, contract, temp uint64, available, deleted []types.Hash256) {
		t.Helper()

		for _, root := range available {
			if _, err := db.SectorLocation(root); err != nil {
				t.Fatalf("sector %v not found: %s", available, err)
			}
		}
		for _, root := range deleted {
			if _, err := db.SectorLocation(root); !errors.Is(err, storage.ErrSectorNotFound) {
				t.Fatalf("expected ErrSectorNotFound, got %v", err)
			}
		}

		used, _, err := db.StorageUsage()
		if err != nil {
			t.Fatalf("failed to get storage usage: %v", err)
		} else if used != uint64(len(available)) {
			t.Fatalf("expected %v sectors, got %v", used, len(available))
		}

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatalf("failed to get metrics: %v", err)
		} else if m.Storage.PhysicalSectors != uint64(len(available)) {
			t.Fatalf("expected %v physical sectors, got %v", len(available), m.Storage.PhysicalSectors)
		} else if m.Storage.ContractSectors != contract {
			t.Fatalf("expected %v contract sectors, got %v", contract, m.Storage.ContractSectors)
		} else if m.Storage.TempSectors != temp {
			t.Fatalf("expected %v temporary sectors, got %v", temp, m.Storage.TempSectors)
		}
	}
	assertSectors(t, 25, 25, roots, nil)

	// prune unreferenced sectors
	available, deleted := roots[:len(roots)-len(unreferencedSectors)], unreferencedSectors
	if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertSectors(t, 25, 25, available, deleted)

	// expire one of the contract's sectors
	if err := db.ExpireContractSectors(101); err != nil {
		t.Fatal(err)
	} else if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	available, deleted = append(contract2Sectors, tempSectors...), append(deleted, contract1Sectors...)
	assertSectors(t, 15, 25, available, deleted)

	// expire the temp sectors
	if err := db.ExpireTempSectors(101); err != nil {
		t.Fatal(err)
	} else if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	available, deleted = contract2Sectors, append(deleted, tempSectors...)
	assertSectors(t, 15, 0, available, deleted)

	if err := db.ExpireContractSectors(111); err != nil {
		t.Fatal(err)
	} else if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	available, deleted = nil, append(deleted, contract1Sectors...)
	assertSectors(t, 0, 0, available, deleted)
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

	volume1, err := addTestVolume(db, "test", uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, b.N)
	for i := range roots {
		roots[i] = frand.Entropy256()
		err := db.StoreSector(roots[i], func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			b.Fatalf("failed to store sector %v: %v", i, err)
		}
	}

	_, err = addTestVolume(db, "test2", uint64(b.N))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	// migrate all sectors from the first volume to the second
	migrated, failed, err := db.MigrateSectors(context.Background(), volume1.ID, 0, func(from, to storage.SectorLocation) error {
		return nil
	})
	if err != nil {
		b.Fatal(err)
	} else if migrated != b.N {
		b.Fatalf("expected %v migrated sectors, got %v", b.N, migrated)
	} else if failed != 0 {
		b.Fatalf("expected 0 failed sectors, got %v", failed)
	}
}

func BenchmarkStoreSector(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = addTestVolume(db, "test", uint64(b.N*2))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	for i := 0; i < b.N; i++ {
		err := db.StoreSector(frand.Entropy256(), func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSector(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = addTestVolume(db, "test", uint64(b.N*2))
	if err != nil {
		b.Fatal(err)
	}

	root := frand.Entropy256()
	err = db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	for i := 0; i < b.N; i++ {
		if _, err := db.SectorLocation(root); err != nil {
			b.Fatal(err)
		}
	}
}
