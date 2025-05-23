package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestWalletLockUnlock(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "hostd.sqlite3"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectedLocked := make(map[types.SiacoinOutputID]bool)
	lockedIDs := make([]types.SiacoinOutputID, 10)
	for i := range lockedIDs {
		lockedIDs[i] = frand.Entropy256()
		expectedLocked[lockedIDs[i]] = true
	}
	if err := db.LockUTXOs(lockedIDs, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err := db.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != len(lockedIDs) {
		t.Fatalf("expected %d locked UTXOs, got %d", len(lockedIDs), len(ids))
	}
	for _, id := range ids {
		if _, ok := expectedLocked[id]; !ok {
			t.Fatalf("unexpected locked UTXO %s", id)
		}
	}

	if err := db.ReleaseUTXOs(lockedIDs); err != nil {
		t.Fatal(err)
	}

	ids, err = db.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked UTXOs, got %d", len(ids))
	}

	// lock the ids, but set the unlock time to the past
	if err := db.LockUTXOs(lockedIDs, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	ids, err = db.LockedUTXOs(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if len(ids) != 0 {
		t.Fatalf("expected 0 locked UTXOs, got %d", len(ids))
	}

	// assert the utxos were cleaned up
	var count int
	err = db.db.QueryRow(`SELECT COUNT(*) FROM wallet_locked_utxos`).Scan(&count)
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 locked UTXOs, got %d", count)
	}
}
