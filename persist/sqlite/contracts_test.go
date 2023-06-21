package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func rootsEqual(a, b []types.Hash256) error {
	if len(a) != len(b) {
		return errors.New("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf("root %v mismatch: expected %v, got %v", i, a[i], b[i])
		}
	}
	return nil
}

func TestReviseContract(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}

	// add a contract to the database
	contract := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
			FileContract: types.FileContract{
				UnlockHash:     types.Hash256(contractUnlockConditions.UnlockHash()),
				RevisionNumber: 1,
				WindowStart:    100,
				WindowEnd:      200,
			},
		},
	}

	if err := db.AddContract(contract, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}, 0); err != nil {
		t.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(volumeID, 100); err != nil {
		t.Fatal(err)
	}

	// add some sector roots
	var changes []contracts.SectorChange
	var roots []types.Hash256
	for i := 0; i < 10; i++ {
		root := frand.Entropy256()
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		changes = append(changes, contracts.SectorChange{
			Root:   root,
			Action: contracts.SectorActionAppend,
		})
		roots = append(roots, root)
	}

	err = db.ReviseContract(contract, contracts.Usage{}, 0, changes)
	if err != nil {
		t.Fatal(err)
	}

	// verify the roots were added in the correct order
	dbRoots, err := db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// swap two roots
	i, j := 5, 8
	changes = []contracts.SectorChange{
		{Action: contracts.SectorActionSwap, A: uint64(i), B: uint64(j)},
	}
	err = db.ReviseContract(contract, contracts.Usage{}, uint64(len(roots)), changes)
	if err != nil {
		t.Fatal(err)
	}
	roots[i], roots[j] = roots[j], roots[i]

	// verify the roots were swapped
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// trim the last 3 roots
	toRemove := 3
	changes = []contracts.SectorChange{
		{Action: contracts.SectorActionTrim, A: uint64(toRemove)},
	}
	err = db.ReviseContract(contract, contracts.Usage{}, uint64(len(roots)), changes)
	if err != nil {
		t.Fatal(err)
	}
	roots = roots[:len(roots)-toRemove]

	// verify the roots were removed
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// swap a root outside of the range, should fail
	changes = []contracts.SectorChange{
		{Action: contracts.SectorActionSwap, A: 0, B: 15},
	}
	err = db.ReviseContract(contract, contracts.Usage{}, uint64(len(roots)), changes)
	if err == nil {
		t.Fatal("expected error")
	}

	// verify the roots stayed the same
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// trim everything
	toTrim := len(roots)
	// swap a root outside of the range, should fail
	changes = []contracts.SectorChange{
		{Action: contracts.SectorActionTrim, A: uint64(toTrim)},
	}
	err = db.ReviseContract(contract, contracts.Usage{}, uint64(len(roots)), changes)
	if err != nil {
		t.Fatal(err)
	}
	roots = roots[:0]

	// verify the roots are gone
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// test multiple operations in the same transaction
	// add some sector roots
	changes = changes[:0]
	for i := 0; i < 10; i++ {
		root := frand.Entropy256()
		release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		changes = append(changes, contracts.SectorChange{
			Root:   root,
			Action: contracts.SectorActionAppend,
		})
		roots = append(roots, root)
	}

	// store a sector root to update the contract
	updateRoot := frand.Entropy256()
	release, err := db.StoreSector(updateRoot, func(loc storage.SectorLocation, exists bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	i, j = 3, 6
	toUpdate := 8
	toTrim = 3
	changes = append(changes, contracts.SectorChange{
		Action: contracts.SectorActionSwap,
		A:      uint64(i),
		B:      uint64(j),
	}, contracts.SectorChange{
		Action: contracts.SectorActionUpdate,
		A:      uint64(toUpdate),
		Root:   updateRoot,
	}, contracts.SectorChange{
		Action: contracts.SectorActionTrim,
		A:      uint64(toTrim),
	})

	err = db.ReviseContract(contract, contracts.Usage{}, 0, changes)
	if err != nil {
		t.Fatal(err)
	}

	// update the roots
	roots[i], roots[j] = roots[j], roots[i]
	roots[toUpdate] = updateRoot
	roots = roots[:len(roots)-toTrim]

	// verify the roots match
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}
}

func TestContracts(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}

	c, count, err := db.Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}

	// add a contract to the database
	contract := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
			FileContract: types.FileContract{
				UnlockHash:     types.Hash256(contractUnlockConditions.UnlockHash()),
				RevisionNumber: 1,
				WindowStart:    100,
				WindowEnd:      200,
			},
		},
	}

	if err := db.AddContract(contract, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}, 0); err != nil {
		t.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(volumeID, 100); err != nil {
		t.Fatal(err)
	}

	c, count, err = db.Contracts(contracts.ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 1 {
		t.Fatal("expected one contract")
	} else if count != 1 {
		t.Fatal("expected one contract")
	}

	filter := contracts.ContractFilter{
		Statuses: []contracts.ContractStatus{contracts.ContractStatusActive},
	}
	c, count, err = db.Contracts(filter)
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}
}
