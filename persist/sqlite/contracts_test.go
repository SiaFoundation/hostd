package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func (s *Store) rootAtIndex(contractID types.FileContractID, rootIndex int64) (root types.Hash256, err error) {
	err = s.transaction(func(tx txn) error {
		const query = `SELECT s.sector_root FROM contract_sector_roots csr
INNER JOIN stored_sectors s ON (csr.sector_id = s.id)
INNER JOIN contracts c ON (csr.contract_id = c.id)
WHERE c.contract_id=$1 AND csr.root_index=$2;`
		return tx.QueryRow(query, sqlHash256(contractID), rootIndex).Scan((*sqlHash256)(&root))
	})
	return
}

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

func runRevision(db *Store, revision contracts.SignedRevision, roots []types.Hash256, changes []contracts.SectorChange) error {
	for _, change := range changes {
		switch change.Action {
		// store a sector in the database for the append or update actions
		case contracts.SectorActionAppend, contracts.SectorActionUpdate:
			root := frand.Entropy256()
			release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
			if err != nil {
				return fmt.Errorf("failed to store sector: %w", err)
			}
			defer release()
			change.Root = root
		}
	}

	return db.ReviseContract(revision, roots, contracts.Usage{}, changes)
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

	// checkConsistency is a helper function that verifies the expected sector
	// roots are consistent with the database
	checkConsistency := func(roots []types.Hash256, expected int) error {
		dbRoot, err := db.SectorRoots(contract.Revision.ParentID)
		if err != nil {
			return fmt.Errorf("failed to get sector roots: %w", err)
		} else if len(dbRoot) != expected {
			return fmt.Errorf("expected %v sector roots, got %v", expected, len(dbRoot))
		} else if len(roots) != expected {
			return fmt.Errorf("expected %v sector roots, got %v", expected, len(roots))
		} else if err = rootsEqual(roots, dbRoot); err != nil {
			return fmt.Errorf("sector roots mismatch: %w", err)
		}

		// verify the roots were added in the correct order
		for i := range roots {
			root, err := db.rootAtIndex(contract.Revision.ParentID, int64(i))
			if err != nil {
				return fmt.Errorf("failed to get sector root %d: %w", i, err)
			} else if root != roots[i] {
				return fmt.Errorf("sector root mismatch: expected %v, got %v", roots[i], root)
			}
		}

		m, err := db.Metrics(time.Now())
		if err != nil {
			return fmt.Errorf("failed to get metrics: %w", err)
		} else if m.Storage.ContractSectors != uint64(len(roots)) {
			return fmt.Errorf("expected %v contract sectors, got %v", len(roots), m.Storage.ContractSectors)
		} else if m.Storage.PhysicalSectors != uint64(len(roots)) {
			return fmt.Errorf("expected %v physical sectors, got %v", len(roots), m.Storage.PhysicalSectors)
		}
		return nil
	}

	var roots []types.Hash256
	tests := []struct {
		name    string
		changes []contracts.SectorChange
		sectors int
		errors  bool
	}{
		{
			name: "append 10 roots",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
			},
			sectors: 10,
		},
		{
			name: "swap 4 roots",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionSwap, A: 0, B: 1},
				{Action: contracts.SectorActionSwap, A: 6, B: 4},
			},
			sectors: 10,
		},
		{
			name: "update root",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionUpdate, A: 3},
			},
			sectors: 10,
		},
		{
			name: "trim 5 roots",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionTrim, A: 5},
			},
			sectors: 5,
		},
		{
			name: "swap outside range",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionSwap, A: 0, B: 10},
			},
			errors:  true,
			sectors: 5,
		},
		{
			name: "append swap trim",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionSwap, A: 5, B: 2},
				{Action: contracts.SectorActionTrim, A: 1},
			},
			sectors: 5,
		},
		{
			name: "trim append swap",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionTrim, A: 1},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionSwap, A: 4, B: 1},
			},
			sectors: 5,
		},
		{
			name: "swap 2 roots",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionSwap, A: 3, B: 1},
			},
			sectors: 5,
		},
		{
			name: "trim append",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionTrim, A: 5},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
			},
			sectors: 3,
		},
		{
			name: "trim more",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionTrim, A: 5},
			},
			sectors: 3,
			errors:  true,
		},
		{
			name: "update outside range",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionUpdate, A: 6},
			},
			sectors: 3,
			errors:  true,
		},
		{
			name: "trim all",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionTrim, A: 3},
			},
			sectors: 0,
		},
		{
			name: "append 5",
			changes: []contracts.SectorChange{
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
				{Action: contracts.SectorActionAppend},
			},
			sectors: 5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldRoots := append([]types.Hash256(nil), roots...)
			// update the expected roots
			for i, change := range test.changes {
				switch change.Action {
				case contracts.SectorActionAppend:
					// add a random sector root
					root := frand.Entropy256()
					release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					defer release()
					test.changes[i].Root = root
					roots = append(roots, root)
				case contracts.SectorActionUpdate:
					// replace with a random sector root
					root := frand.Entropy256()
					release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					defer release()
					test.changes[i].Root = root

					if test.errors && change.A >= uint64(len(roots)) { // test failure
						continue
					}
					roots[change.A] = root
				case contracts.SectorActionSwap:
					if test.errors && (change.A >= uint64(len(roots)) || change.B >= uint64(len(roots))) { // test failure
						continue
					}
					roots[change.A], roots[change.B] = roots[change.B], roots[change.A]
				case contracts.SectorActionTrim:
					if test.errors && change.A >= uint64(len(roots)) { // test failure
						continue
					}
					roots = roots[:len(roots)-int(change.A)]
				}
			}

			if err := runRevision(db, contract, oldRoots, test.changes); err != nil {
				if test.errors {
					t.Log("received error:", err)
					return
				}
				t.Fatal(err)
			} else if err == nil && test.errors {
				t.Fatal("expected error")
			} else if err := checkConsistency(roots, test.sectors); err != nil {
				t.Fatal(err)
			}
		})
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
