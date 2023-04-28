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

func TestUpdateContractRoots(t *testing.T) {
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
	roots := make([]types.Hash256, 10)
	for i := range roots {
		roots[i] = frand.Entropy256()
		release, err := db.StoreSector(roots[i], func(loc storage.SectorLocation, exists bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}

	err = db.UpdateContract(contract.Revision.ParentID, func(tx contracts.UpdateContractTransaction) error {
		for _, root := range roots {
			if err := tx.AppendSector(root); err != nil {
				return err
			}
		}
		return nil
	})
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
	roots[i], roots[j] = roots[j], roots[i]
	err = db.UpdateContract(contract.Revision.ParentID, func(tx contracts.UpdateContractTransaction) error {
		return tx.SwapSectors(uint64(i), uint64(j))
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the roots were swapped
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// trim the last 3 roots
	toRemove := 3
	roots = roots[:len(roots)-toRemove]
	err = db.UpdateContract(contract.Revision.ParentID, func(tx contracts.UpdateContractTransaction) error {
		return tx.TrimSectors(toRemove)
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the roots were removed
	dbRoots, err = db.SectorRoots(contract.Revision.ParentID, 0, 100)
	if err != nil {
		t.Fatal(err)
	} else if err = rootsEqual(roots, dbRoots); err != nil {
		t.Fatal(err)
	}

	// swap a root outside of the range, should fail
	err = db.UpdateContract(contract.Revision.ParentID, func(tx contracts.UpdateContractTransaction) error {
		return tx.SwapSectors(0, 100)
	})
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
