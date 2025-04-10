package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func (s *Store) rootAtIndex(contractID types.FileContractID, rootIndex int64) (root types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT s.sector_root FROM contract_sector_roots csr
INNER JOIN stored_sectors s ON (csr.sector_id = s.id)
INNER JOIN contracts c ON (csr.contract_id = c.id)
WHERE c.contract_id=$1 AND csr.root_index=$2;`
		return tx.QueryRow(query, encode(contractID), rootIndex).Scan(decode(&root))
	})
	return
}

func (s *Store) dbRoots(contractID types.FileContractID) (roots []types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT s.sector_root FROM contract_sector_roots cr
INNER JOIN stored_sectors s ON (cr.sector_id = s.id)
INNER JOIN contracts c ON (cr.contract_id = c.id)
WHERE c.contract_id=$1 ORDER BY cr.root_index ASC;`

		rows, err := tx.Query(query, encode(contractID))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan(decode(&root)); err != nil {
				return err
			}
			roots = append(roots, root)
		}
		return rows.Err()
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
				UnlockHash:     contractUnlockConditions.UnlockHash(),
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
		// prune all possible sectors
		if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		}

		dbRoot, err := db.dbRoots(contract.Revision.ParentID)
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

			_, err = db.SectorLocation(root)
			if err != nil {
				return fmt.Errorf("failed to get sector location: %w", err)
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
		func() {
			t.Log("revising contract:", test.name)
			oldRoots := append([]types.Hash256(nil), roots...)

			for i, change := range test.changes {
				switch change.Action {
				case contracts.SectorActionAppend:
					// add a random sector root
					root := frand.Entropy256()
					err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					test.changes[i].Root = root
					roots = append(roots, root)
				case contracts.SectorActionUpdate:
					// replace with a random sector root
					root := frand.Entropy256()
					err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
					if err != nil {
						t.Fatal(err)
					}
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

			if err := db.ReviseContract(contract, oldRoots, contracts.Usage{}, test.changes); err != nil {
				if test.errors {
					t.Log("received error:", err)
					return
				}
				t.Fatal(err)
			} else if test.errors {
				t.Fatal("expected error")
			}

			if err := checkConsistency(roots, test.sectors); err != nil {
				t.Fatal(err)
			}
		}()
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
				UnlockHash:     contractUnlockConditions.UnlockHash(),
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

func TestV2Contracts(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	c, count, err := db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}

	// add a contract to the database
	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  renterKey.PublicKey(),
			HostPublicKey:    hostKey.PublicKey(),
			ProofHeight:      100,
			ExpirationHeight: 200,
		},
	}

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
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

	c, count, err = db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 1 {
		t.Fatal("expected one contract")
	} else if count != 1 {
		t.Fatal("expected one contract")
	}

	filter := contracts.V2ContractFilter{
		Statuses: []contracts.V2ContractStatus{contracts.V2ContractStatusActive},
	}
	c, count, err = db.V2Contracts(filter)
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}
}

func TestReviseV2ContractConsistency(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	c, count, err := db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}

	// add a contract to the database
	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  renterKey.PublicKey(),
			HostPublicKey:    hostKey.PublicKey(),
			ProofHeight:      100,
			ExpirationHeight: 200,
		},
	}

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(volumeID, 1000); err != nil {
		t.Fatal(err)
	}

	checkRootConsistency := func(t *testing.T, expected []types.Hash256) {
		t.Helper()

		err := db.transaction(func(tx *txn) error {
			stmt, err := tx.Prepare(`SELECT ss.sector_root FROM stored_sectors ss
INNER JOIN contract_v2_sector_roots csr ON (ss.id = csr.sector_id)
INNER JOIN contracts_v2 c ON (csr.contract_id = c.id)
WHERE c.contract_id=$1 AND csr.root_index= $2`)
			if err != nil {
				t.Fatal("failed to prepare statement:", err)
			}
			defer stmt.Close()

			for i, root := range expected {
				var dbRoot types.Hash256
				if err := stmt.QueryRow(encode(contract.ID), i).Scan(decode(&dbRoot)); err != nil {
					t.Fatalf("failed to scan root %d: %s", i, err)
				} else if dbRoot != root {
					t.Fatalf("expected root %q at index %d, got %q", root, i, dbRoot)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal("failed to get db roots:", err)
		}
	}

	checkMetricConsistency := func(t *testing.T, expected uint64) {
		t.Helper()

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal("failed to get metrics:", err)
		} else if m.Storage.ContractSectors != expected {
			t.Fatalf("expected %d contract sectors, got %d", expected, m.Storage.ContractSectors)
		}
	}

	var roots []types.Hash256
	appendSectors := func(t *testing.T, n int) {
		t.Helper()

		var appended []types.Hash256
		for i := 0; i < n; i++ {
			root := frand.Entropy256()
			err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
			if err != nil {
				t.Fatal("failed to store sector:", err)
			}
			appended = append(appended, root)
		}
		newRoots := append(append([]types.Hash256(nil), roots...), appended...)
		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
		checkMetricConsistency(t, uint64(len(newRoots)))
		roots = newRoots
	}

	swapSectors := func(t *testing.T) {
		t.Helper()

		a, b := frand.Intn(len(roots)), frand.Intn(len(roots))

		newRoots := append([]types.Hash256(nil), roots...)
		newRoots[a], newRoots[b] = newRoots[b], newRoots[a]

		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}
		checkRootConsistency(t, newRoots)
		checkMetricConsistency(t, uint64(len(newRoots)))
		roots = newRoots
	}

	deleteSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		for i := 0; i < n; i++ {
			j := frand.Intn(len(newRoots))
			newRoots = append(newRoots[:j], newRoots[j+1:]...)
		}

		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
		checkMetricConsistency(t, uint64(len(newRoots)))
		roots = newRoots
	}

	trimSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		newRoots = newRoots[:len(newRoots)-n]

		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
		checkMetricConsistency(t, uint64(len(newRoots)))
		roots = newRoots
	}

	appendSectors(t, 15)
	swapSectors(t)
	deleteSectors(t, 1)
	trimSectors(t, 5)

	appendSectors(t, frand.Intn(49)+1)
	for i := 0; i < 100; i++ {
		swapSectors(t)
	}
	for i := 0; i < 10; i++ {
		appendSectors(t, 1)
		deleteSectors(t, frand.Intn(len(roots)/4)+1)
	}
	trimSectors(t, frand.Intn(len(roots)/4)+1)
}

func BenchmarkTrimSectors(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
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
				UnlockHash:     contractUnlockConditions.UnlockHash(),
				RevisionNumber: 1,
				WindowStart:    100,
				WindowEnd:      200,
			},
		},
	}

	if err := db.AddContract(contract, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}, 0); err != nil {
		b.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	} else if err = db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, 0, b.N)
	appendActions := make([]contracts.SectorChange, 0, b.N)

	for i := 0; i < b.N; i++ {
		root := frand.Entropy256()
		roots = append(roots, root)
		appendActions = append(appendActions, contracts.SectorChange{Action: contracts.SectorActionAppend, Root: root})

		err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}

	if err := db.ReviseContract(contract, nil, contracts.Usage{}, appendActions); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	if err := db.ReviseContract(contract, roots, contracts.Usage{}, []contracts.SectorChange{{Action: contracts.SectorActionTrim, A: uint64(b.N)}}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkV2AppendSectors(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// add a contract to the database
	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RevisionNumber: 1,
		},
	}

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		b.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	} else if err = db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, 0, b.N)

	for i := 0; i < b.N; i++ {
		root := types.Hash256(frand.Entropy256())
		roots = append(roots, root)

		err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			b.Fatal(err)
		} else if err := db.AddTemporarySectors([]storage.TempSector{{Root: root, Expiration: 100}}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkV2TrimSectors(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// add a contract to the database
	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RevisionNumber: 1,
		},
	}

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		b.Fatal(err)
	}

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	} else if err = db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, 0, b.N)

	for i := 0; i < b.N; i++ {
		root := types.Hash256(frand.Entropy256())
		roots = append(roots, root)

		err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
		if err != nil {
			b.Fatal(err)
		} else if err := db.AddTemporarySectors([]storage.TempSector{{Root: root, Expiration: 100}}); err != nil {
			b.Fatal(err)
		}
	}

	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, nil, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}
}
