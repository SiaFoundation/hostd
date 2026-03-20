package sqlite

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
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
		for range n {
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
		for range n {
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
	for range 100 {
		swapSectors(t)
	}
	for range 10 {
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

		var actual []types.Hash256
		err := db.transaction(func(tx *txn) error {
			var mapID, mapRevNum int64
			err := tx.QueryRow(`SELECT contract_v2_roots_map_id, contract_v2_roots_map_revision_number FROM contracts_v2 WHERE contract_id=$1`, encode(contract.ID)).Scan(&mapID, &mapRevNum)
			if err != nil {
				return err
			}
			// maxint64 is used here to catch orphaned deleted roots
			actual, err = v2ContractRoots(tx, mapID, mapRevNum, math.MaxInt64)
			return err
		})
		if err != nil {
			t.Fatal("failed to get sector roots:", err)
		} else if !slices.Equal(actual, expected) {
			t.Fatalf("expected roots %v, got %v", expected, actual)
		}
	}

	var roots []types.Hash256
	appendSectors := func(t *testing.T, n int) {
		t.Helper()

		var appended []types.Hash256
		for range n {
			root := frand.Entropy256()
			err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
			if err != nil {
				t.Fatal("failed to store sector:", err)
			}
			appended = append(appended, root)
		}
		newRoots := append(append([]types.Hash256(nil), roots...), appended...)
		contract.V2FileContract.RevisionNumber++
		contract.V2FileContract.Filesize = proto4.SectorSize * uint64(len(newRoots))
		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
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
		roots = newRoots
	}

	deleteSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		for range n {
			j := frand.Intn(len(newRoots))
			newRoots = append(newRoots[:j], newRoots[j+1:]...)
		}

		contract.V2FileContract.RevisionNumber++
		contract.V2FileContract.Filesize = proto4.SectorSize * uint64(len(newRoots))
		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
		roots = newRoots
	}

	trimSectors := func(t *testing.T, n int) {
		t.Helper()

		newRoots := append([]types.Hash256(nil), roots...)
		newRoots = newRoots[:len(newRoots)-n]

		contract.V2FileContract.RevisionNumber++
		contract.V2FileContract.Filesize = proto4.SectorSize * uint64(len(newRoots))
		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal("failed to revise contract:", err)
		}

		checkRootConsistency(t, newRoots)
		roots = newRoots
	}

	appendSectors(t, 15)
	swapSectors(t)
	deleteSectors(t, 1)
	trimSectors(t, 5)

	appendSectors(t, frand.Intn(49)+1)
	for range 100 {
		swapSectors(t)
	}
	for range 10 {
		appendSectors(t, 1)
		deleteSectors(t, frand.Intn(len(roots)/4)+1)
	}
	trimSectors(t, frand.Intn(len(roots)/4)+1)
}

func TestExpireV2ContractSectorsBatching(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(volumeID, 100); err != nil {
		t.Fatal(err)
	}

	resolveContract := func(t *testing.T, contractID types.FileContractID, height uint64) {
		t.Helper()
		err := db.transaction(func(tx *txn) error {
			_, err := tx.Exec(`UPDATE contracts_v2 SET resolution_height=$1, resolution_block_id=$2, contract_status=$3 WHERE contract_id=$4`,
				height, encode(types.BlockID{}), contracts.V2ContractStatusSuccessful, encode(contractID))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	countSectorRows := func(t *testing.T, contractID types.FileContractID) int {
		t.Helper()
		var count int
		err := db.transaction(func(tx *txn) error {
			return tx.QueryRow(`SELECT COUNT(*) FROM contract_v2_sector_roots csr
INNER JOIN contracts_v2 c ON (c.contract_v2_roots_map_id = csr.contract_v2_roots_map_id
AND c.contract_v2_roots_map_revision_number = csr.contract_v2_roots_map_revision_number)
WHERE c.contract_id = $1`, encode(contractID)).Scan(&count)
		})
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	countAllSectorRows := func(t *testing.T) int {
		t.Helper()
		var count int
		err := db.transaction(func(tx *txn) error {
			return tx.QueryRow(`SELECT COUNT(*) FROM contract_v2_sector_roots`).Scan(&count)
		})
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	storeSectors := func(t *testing.T, n int) []types.Hash256 {
		t.Helper()
		var roots []types.Hash256
		for range n {
			root := frand.Entropy256()
			if err := db.StoreSector(root, func(loc storage.SectorLocation) error { return nil }); err != nil {
				t.Fatal(err)
			}
			roots = append(roots, root)
		}
		return roots
	}

	t.Run("single contract", func(t *testing.T) {
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

		roots := storeSectors(t, 13)
		contract.V2FileContract.RevisionNumber++
		contract.V2FileContract.Filesize = proto4.SectorSize * uint64(len(roots))
		if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		resolveContract(t, contract.ID, 100)
		if err := db.ExpireV2ContractSectors(101); err != nil {
			t.Fatal(err)
		} else if n := countSectorRows(t, contract.ID); n != 0 {
			t.Fatalf("expected 0 remaining sector roots, got %d", n)
		}
	})

	t.Run("renewal chain with orphans", func(t *testing.T) {
		c1 := contracts.V2Contract{
			ID: frand.Entropy256(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				ProofHeight:      100,
				ExpirationHeight: 200,
			},
		}
		if err := db.AddV2Contract(c1, rhp4.TransactionSet{}); err != nil {
			t.Fatal(err)
		}

		roots := storeSectors(t, 8)
		c1.V2FileContract.RevisionNumber++
		c1.V2FileContract.Filesize = proto4.SectorSize * uint64(len(roots))
		if err := db.ReviseV2Contract(c1.ID, c1.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// renew the contract — c2 inherits roots from c1
		c2 := contracts.V2Contract{
			ID: frand.Entropy256(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				Filesize:         c1.V2FileContract.Filesize,
				ProofHeight:      200,
				ExpirationHeight: 300,
			},
		}
		if err := db.RenewV2Contract(c2, rhp4.TransactionSet{}, c1.ID); err != nil {
			t.Fatal(err)
		}

		// delete 3 sectors from the renewal via swap-and-trim, creating
		// orphaned inherited rows at the old revision
		newRoots := slices.Clone(roots)
		newRoots[1] = newRoots[len(newRoots)-1]
		newRoots[3] = newRoots[len(newRoots)-2]
		newRoots[5] = newRoots[len(newRoots)-3]
		newRoots = newRoots[:len(newRoots)-3]

		c2.V2FileContract.RevisionNumber++
		c2.V2FileContract.Filesize = proto4.SectorSize * uint64(len(newRoots))
		if err := db.ReviseV2Contract(c2.ID, c2.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// add more sectors to push total rows past the batch size
		extra := storeSectors(t, 6)
		finalRoots := append(slices.Clone(newRoots), extra...)
		c2.V2FileContract.RevisionNumber++
		c2.V2FileContract.Filesize = proto4.SectorSize * uint64(len(finalRoots))
		if err := db.ReviseV2Contract(c2.ID, c2.V2FileContract, newRoots, finalRoots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		totalBefore := countAllSectorRows(t)

		// resolve the original contract so its rows become eligible for expiry
		resolveContract(t, c1.ID, 100)
		if err := db.ExpireV2ContractSectors(101); err != nil {
			t.Fatal(err)
		}

		totalAfter := countAllSectorRows(t)
		// 5 of c1's 8 rows are superseded (indices 1,3,5,6,7 have
		// replacement rows at rev 1). The other 3 (indices 0,2,4) are
		// still inherited by c2 and must not be deleted.
		if totalBefore-totalAfter != 5 {
			t.Fatalf("expected 5 rows removed, got %d (before=%d, after=%d)", totalBefore-totalAfter, totalBefore, totalAfter)
		}

		// resolve c2 and expire its sectors
		resolveContract(t, c2.ID, 200)
		if err := db.ExpireV2ContractSectors(201); err != nil {
			t.Fatal(err)
		} else if n := countAllSectorRows(t); n != 0 {
			t.Fatalf("expected 0 remaining sector roots, got %d", n)
		}
	})

	// Verify that inherited rows at indices beyond the active contract's
	// sector_count are cleaned up immediately, rather than waiting for
	// the entire chain to expire.
	t.Run("out of range inherited rows", func(t *testing.T) {
		c1 := contracts.V2Contract{
			ID: frand.Entropy256(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				ProofHeight:      100,
				ExpirationHeight: 200,
			},
		}
		if err := db.AddV2Contract(c1, rhp4.TransactionSet{}); err != nil {
			t.Fatal(err)
		}

		// add 8 sectors to the original contract
		roots := storeSectors(t, 8)
		c1.V2FileContract.RevisionNumber++
		c1.V2FileContract.Filesize = proto4.SectorSize * uint64(len(roots))
		if err := db.ReviseV2Contract(c1.ID, c1.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// renew — c2 inherits 8 sectors from c1
		c2 := contracts.V2Contract{
			ID: frand.Entropy256(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				Filesize:         c1.V2FileContract.Filesize,
				ProofHeight:      200,
				ExpirationHeight: 300,
			},
		}
		if err := db.RenewV2Contract(c2, rhp4.TransactionSet{}, c1.ID); err != nil {
			t.Fatal(err)
		}

		// trim c2 down to 3 sectors (indices 0-2 kept, 3-7 out of range)
		trimmedRoots := roots[:3]
		c2.V2FileContract.RevisionNumber++
		c2.V2FileContract.Filesize = proto4.SectorSize * uint64(len(trimmedRoots))
		if err := db.ReviseV2Contract(c2.ID, c2.V2FileContract, roots, trimmedRoots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// 8 rows at rev 0 (c1), 0 new rows at rev 1 (trim only)
		totalBefore := countAllSectorRows(t)
		if totalBefore != 8 {
			t.Fatalf("expected 8 total rows, got %d", totalBefore)
		}

		// resolve c1 and expire — indices 3-7 are out of range for c2
		// (sector_count=3), so they should be cleaned up immediately
		resolveContract(t, c1.ID, 100)
		if err := db.ExpireV2ContractSectors(101); err != nil {
			t.Fatal(err)
		}

		totalAfter := countAllSectorRows(t)
		// indices 0,1,2 are still inherited by c2, indices 3-7 should be gone
		if totalAfter != 3 {
			t.Fatalf("expected 3 remaining rows, got %d", totalAfter)
		}

		// resolve c2 and clean up the rest
		resolveContract(t, c2.ID, 200)
		if err := db.ExpireV2ContractSectors(201); err != nil {
			t.Fatal(err)
		} else if n := countAllSectorRows(t); n != 0 {
			t.Fatalf("expected 0 remaining sector roots, got %d", n)
		}
	})

	t.Run("renewal rejected then renewed again", func(t *testing.T) {
		checkSectorRoots := func(t *testing.T, contractID types.FileContractID, expected []types.Hash256) {
			t.Helper()
			roots, err := db.V2SectorRoots()
			if err != nil {
				t.Fatal(err)
			}
			actual := roots[contractID]
			if !slices.Equal(actual, expected) {
				t.Fatalf("expected %d roots, got %d", len(expected), len(actual))
			}
		}

		rejectContract := func(t *testing.T, contractID types.FileContractID, parentID types.FileContractID) {
			t.Helper()
			err := db.transaction(func(tx *txn) error {
				// set the contract status to rejected
				if _, err := tx.Exec(`UPDATE contracts_v2 SET contract_status=$1 WHERE contract_id=$2`,
					contracts.V2ContractStatusRejected, encode(contractID)); err != nil {
					return err
				}
				// clear the renewed_to field on the parent so it can be renewed again
				if _, err := tx.Exec(`UPDATE contracts_v2 SET renewed_to=NULL WHERE contract_id=$1`, encode(parentID)); err != nil {
					return err
				}
				// increment rejected contracts metric (matches rejectV2Contracts behavior)
				return incrementNumericStat(tx, metricRejectedContracts, 1, time.Now())
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// create c1
		c1 := contracts.V2Contract{
			ID: frand.Entropy256(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				ProofHeight:      100,
				ExpirationHeight: 200,
			},
		}
		if err := db.AddV2Contract(c1, rhp4.TransactionSet{}); err != nil {
			t.Fatal(err)
		}

		// add sectors to c1
		c1Roots := storeSectors(t, 8)
		c1.V2FileContract.RevisionNumber++
		c1.V2FileContract.Filesize = proto4.SectorSize * uint64(len(c1Roots))
		if err := db.ReviseV2Contract(c1.ID, c1.V2FileContract, nil, c1Roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// renew c1 → c2
		c2 := contracts.V2Contract{
			ID: c1.ID.V2RenewalID(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				Filesize:         c1.V2FileContract.Filesize,
				ProofHeight:      200,
				ExpirationHeight: 300,
			},
		}
		if err := db.RenewV2Contract(c2, rhp4.TransactionSet{}, c1.ID); err != nil {
			t.Fatal(err)
		}

		// upload additional sectors to c2
		c2Extra := storeSectors(t, 4)
		c2Roots := append(slices.Clone(c1Roots), c2Extra...)
		c2.V2FileContract.RevisionNumber++
		c2.V2FileContract.Filesize = proto4.SectorSize * uint64(len(c2Roots))
		if err := db.ReviseV2Contract(c2.ID, c2.V2FileContract, c1Roots, c2Roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// reject c2 due to a reorg
		rejectContract(t, c2.ID, c1.ID)

		// renew c1 → c3
		c3 := contracts.V2Contract{
			ID: c1.ID.V2RenewalID(),
			V2FileContract: types.V2FileContract{
				RenterPublicKey:  renterKey.PublicKey(),
				HostPublicKey:    hostKey.PublicKey(),
				Filesize:         c1.V2FileContract.Filesize,
				ProofHeight:      300,
				ExpirationHeight: 400,
			},
		}
		if err := db.RenewV2Contract(c3, rhp4.TransactionSet{}, c1.ID); err != nil {
			t.Fatal(err)
		}

		// c3 should have c1's original sectors
		checkSectorRoots(t, c3.ID, c1Roots)

		// upload more sectors to c3
		c3Extra := storeSectors(t, 5)
		c3Roots := append(slices.Clone(c1Roots), c3Extra...)
		c3.V2FileContract.RevisionNumber++
		c3.V2FileContract.Filesize = proto4.SectorSize * uint64(len(c3Roots))
		if err := db.ReviseV2Contract(c3.ID, c3.V2FileContract, c1Roots, c3Roots, proto4.Usage{}); err != nil {
			t.Fatal(err)
		}

		// verify all sectors are present
		checkSectorRoots(t, c3.ID, c3Roots)
	})
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

			revision := contract.V2FileContract
			revision.RevisionNumber++
			revision.Filesize = proto4.SectorSize * uint64(len(roots))
			revision.FileMerkleRoot = proto4.MetaRoot(roots)

			if err := db.ReviseV2Contract(contract.ID, revision, nil, roots, proto4.Usage{}); err != nil {
				b.Fatal(err)
			}
			contract.V2FileContract = revision
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
				if err := db.RenewV2Contract(contract, rhp4.TransactionSet{}, currentID); err != nil {
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

func BenchmarkExpireV2ContractSectors(b *testing.B) {
	const (
		proofHeight      = 100
		expirationHeight = 200
	)
	contractSectors := b.N * sqlSectorBatchSize * 2

	log := zap.NewNop()
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log.Named("sqlite3"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	} else if err = db.GrowVolume(volumeID, uint64(contractSectors)); err != nil {
		b.Fatal(err)
	}

	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  frand.Entropy256(),
			HostPublicKey:    frand.Entropy256(),
			RevisionNumber:   1,
			ProofHeight:      proofHeight,
			ExpirationHeight: expirationHeight,
		},
	}
	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, contractSectors)
	for i := range roots {
		roots[i] = frand.Entropy256()
		if err := db.StoreSector(roots[i], func(loc storage.SectorLocation) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}

	contract.V2FileContract.RevisionNumber++
	contract.V2FileContract.Filesize = proto4.SectorSize * uint64(len(roots))
	contract.V2FileContract.FileMerkleRoot = proto4.MetaRoot(roots)
	if err := db.ReviseV2Contract(contract.ID, contract.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}

	err = db.transaction(func(tx *txn) error {
		_, err := tx.Exec(`UPDATE contracts_v2 SET resolution_height=$1, resolution_block_id=$2, contract_status=$3`,
			100, encode(types.BlockID{}), contracts.V2ContractStatusSuccessful)
		return err
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(contractSectors), "sectors")

	for range b.N {
		n, err := db.batchExpireV2ContractSectors(contract.ExpirationHeight + 1)
		if err != nil {
			b.Fatal(err)
		} else if n != int64(sqlSectorBatchSize) {
			b.Fatalf("expected to expire %d sectors, expired %d", sqlSectorBatchSize, n)
		}
	}
}

func BenchmarkExpireV2ContractSectorsSuperseded(b *testing.B) {
	const (
		proofHeight      = 100
		expirationHeight = 200
	)
	contractSectors := b.N * sqlSectorBatchSize * 2

	log := zap.NewNop()
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log.Named("sqlite3"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test.dat", false)
	if err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	} else if err = db.GrowVolume(volumeID, uint64(2*contractSectors)); err != nil {
		b.Fatal(err)
	}

	// create c1 with sectors
	c1 := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  frand.Entropy256(),
			HostPublicKey:    frand.Entropy256(),
			RevisionNumber:   1,
			ProofHeight:      proofHeight,
			ExpirationHeight: expirationHeight,
		},
	}
	if err := db.AddV2Contract(c1, rhp4.TransactionSet{}); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, contractSectors)
	for i := range roots {
		roots[i] = frand.Entropy256()
		if err := db.StoreSector(roots[i], func(loc storage.SectorLocation) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}

	c1.V2FileContract.RevisionNumber++
	c1.V2FileContract.Filesize = proto4.SectorSize * uint64(len(roots))
	c1.V2FileContract.FileMerkleRoot = proto4.MetaRoot(roots)
	if err := db.ReviseV2Contract(c1.ID, c1.V2FileContract, nil, roots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}

	// renew c1 -> c2, inheriting all sectors
	c2 := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  c1.RenterPublicKey,
			HostPublicKey:    c1.HostPublicKey,
			RevisionNumber:   1,
			Filesize:         c1.V2FileContract.Filesize,
			ProofHeight:      200,
			ExpirationHeight: 300,
		},
	}
	if err := db.RenewV2Contract(c2, rhp4.TransactionSet{}, c1.ID); err != nil {
		b.Fatal(err)
	}

	// replace every sector in c2 so all of c1's rows become superseded
	newRoots := make([]types.Hash256, contractSectors)
	for i := range newRoots {
		newRoots[i] = frand.Entropy256()
		if err := db.StoreSector(newRoots[i], func(loc storage.SectorLocation) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}

	c2.V2FileContract.RevisionNumber++
	c2.V2FileContract.Filesize = proto4.SectorSize * uint64(len(newRoots))
	c2.V2FileContract.FileMerkleRoot = proto4.MetaRoot(newRoots)
	if err := db.ReviseV2Contract(c2.ID, c2.V2FileContract, roots, newRoots, proto4.Usage{}); err != nil {
		b.Fatal(err)
	}

	// resolve c1 so its rows become eligible for expiry
	err = db.transaction(func(tx *txn) error {
		_, err := tx.Exec(`UPDATE contracts_v2 SET resolution_height=$1, resolution_block_id=$2, contract_status=$3 WHERE contract_id=$4`,
			proofHeight, encode(types.BlockID{}), contracts.V2ContractStatusRenewed, encode(c1.ID))
		return err
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(contractSectors), "sectors")

	for range b.N {
		n, err := db.batchExpireV2ContractSectors(c1.ExpirationHeight + 1)
		if err != nil {
			b.Fatal(err)
		} else if n != int64(sqlSectorBatchSize) {
			b.Fatalf("expected to expire %d sectors, expired %d", sqlSectorBatchSize, n)
		}
	}
}
