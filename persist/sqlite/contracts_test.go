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
	"go.uber.org/zap"
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

func TestFirstContractHeight(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	height, exists, err := db.FirstContractHeight()
	if err != nil {
		t.Fatal(err)
	} else if height != 0 {
		t.Fatalf("expected height 0, got %d", height)
	} else if exists {
		t.Fatal("expected no contracts")
	}

	err = db.AddV2Contract(contracts.V2Contract{
		ID:                frand.Entropy256(),
		NegotiationHeight: 100,
	}, rhp4.TransactionSet{})
	if err != nil {
		t.Fatal(err)
	}

	height, exists, err = db.FirstContractHeight()
	if err != nil {
		t.Fatal(err)
	} else if height != 100 {
		t.Fatalf("expected height 100, got %d", height)
	} else if !exists {
		t.Fatal("expected contract to exist")
	}

	_, err = db.db.Exec(`UPDATE contracts_v2 SET contract_status=$1`, contracts.V2ContractStatusActive)
	if err != nil {
		t.Fatal(err)
	}

	height, exists, err = db.FirstContractHeight()
	if err != nil {
		t.Fatal(err)
	} else if height != 100 {
		t.Fatalf("expected height 100, got %d", height)
	} else if !exists {
		t.Fatal("expected contract to exist")
	}

	_, err = db.db.Exec(`UPDATE contracts_v2 SET contract_status=$1`, contracts.V2ContractStatusSuccessful)
	if err != nil {
		t.Fatal(err)
	}

	// successful contracts should be ignored
	height, exists, err = db.FirstContractHeight()
	if err != nil {
		t.Fatal(err)
	} else if height != 0 {
		t.Fatalf("expected height 0, got %d", height)
	} else if exists {
		t.Fatal("expected no contracts")
	}

	// add multiple contracts
	err = db.AddV2Contract(contracts.V2Contract{
		ID:                frand.Entropy256(),
		NegotiationHeight: 50,
	}, rhp4.TransactionSet{})
	if err != nil {
		t.Fatal(err)
	}

	err = db.AddV2Contract(contracts.V2Contract{
		ID:                frand.Entropy256(),
		NegotiationHeight: 10,
	}, rhp4.TransactionSet{})
	if err != nil {
		t.Fatal(err)
	}

	// minimum height should be returned
	height, exists, err = db.FirstContractHeight()
	if err != nil {
		t.Fatal(err)
	} else if height != 10 {
		t.Fatalf("expected height 10, got %d", height)
	} else if !exists {
		t.Fatal("expected contract to exist")
	}
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
	checkConsistency := func(t *testing.T, roots []types.Hash256) error {
		t.Helper()

		// prune all possible sectors
		if err := db.PruneSectors(context.Background(), time.Now().Add(time.Hour)); err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		}

		expected := len(roots)
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
		if err := db.ReviseContract(contract, roots, newRoots, contracts.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkConsistency(t, newRoots)
		roots = newRoots
	}

	swapSectors := func(t *testing.T) {
		t.Helper()

		a, b := frand.Intn(len(roots)), frand.Intn(len(roots))

		newRoots := append([]types.Hash256(nil), roots...)
		newRoots[a], newRoots[b] = newRoots[b], newRoots[a]

		if err := db.ReviseContract(contract, roots, newRoots, contracts.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}
		checkConsistency(t, newRoots)
		roots = newRoots
	}

	deleteSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		for i := 0; i < n; i++ {
			j := frand.Intn(len(newRoots))
			newRoots = append(newRoots[:j], newRoots[j+1:]...)
		}

		if err := db.ReviseContract(contract, roots, newRoots, contracts.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkConsistency(t, newRoots)
		roots = newRoots
	}

	trimSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		newRoots = newRoots[:len(newRoots)-n]

		if err := db.ReviseContract(contract, roots, newRoots, contracts.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkConsistency(t, newRoots)
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

func TestV2DuplicateContracts(t *testing.T) {
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

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); !errors.Is(err, contracts.ErrContractExists) {
		t.Fatalf("expected ErrContractExists, got %v", err)
	}

	// change the contract status to rejected
	err = db.transaction(func(tx *txn) error {
		if err := incrementNumericStat(tx, metricRejectedContracts, 1, time.Now()); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE contracts_v2 SET contract_status=$1 WHERE contract_id=$2`, contracts.V2ContractStatusRejected, encode(contract.ID))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// add should succeed since the contract is rejected
	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
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
		contract.V2FileContract.RevisionNumber++
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

		contract.V2FileContract.RevisionNumber++
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

		contract.V2FileContract.RevisionNumber++
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

		contract.V2FileContract.RevisionNumber++
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

	contract.V2FileContract.RevisionNumber++
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

	contract.V2FileContract.RevisionNumber++
	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	contract.V2FileContract.RevisionNumber++
	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, nil, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRefreshContract(b *testing.B) {
	runBenchmark := func(b *testing.B, sectors uint64) {
		b.Helper()

		b.Run(fmt.Sprintf("sectors=%d", sectors), func(b *testing.B) {
			log := zap.NewNop()
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
			} else if err = db.GrowVolume(volumeID, sectors); err != nil {
				b.Fatal(err)
			}

			roots := make([]types.Hash256, 0, sectors)

			for range sectors {
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

			currentID := contract.ID
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				contract := contracts.V2Contract{
					ID: frand.Entropy256(),
					V2FileContract: types.V2FileContract{
						RevisionNumber: 1,
					},
				}
				if err := db.RenewV2Contract(contract, rhp4.TransactionSet{}, currentID, roots); err != nil {
					b.Fatal(err)
				}
				currentID = contract.ID
			}
		})
	}

	for _, n := range []uint64{
		10,
		100,
		1000,
		(1 << 40) / proto4.SectorSize,  // 1 TiB
		(10 << 40) / proto4.SectorSize, // 10 TiB
	} {
		runBenchmark(b, n)
	}
}
