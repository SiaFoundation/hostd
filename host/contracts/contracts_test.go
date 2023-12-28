package contracts_test

import (
	"math"
	"path/filepath"
	"reflect"
	"testing"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContractUpdater(t *testing.T) {
	const sectors = 256
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	dir := t.TempDir()

	log := zaptest.NewLogger(t)
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	node, err := test.NewWallet(hostKey, t.TempDir(), log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	webhookReporter, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		t.Fatal(err)
	}

	am := alerts.NewManager(webhookReporter, log.Named("alerts"))
	s, err := storage.NewVolumeManager(db, am, node.ChainManager(), log.Named("storage"), sectorCacheSize)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// create a fake volume so disk space is not used
	id, err := db.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := db.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err := db.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

	c, err := contracts.NewManager(db, am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	contractUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			renterKey.PublicKey().UnlockKey(),
			hostKey.PublicKey().UnlockKey(),
		},
		SignaturesRequired: 2,
	}
	rev := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			FileContract: types.FileContract{
				UnlockHash:  types.Hash256(contractUnlockConditions.UnlockHash()),
				WindowStart: 100,
				WindowEnd:   200,
			},
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
		},
	}

	if err := c.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	var roots []types.Hash256

	tests := []struct {
		name   string
		append int
		swap   [][2]uint64
		trim   uint64
	}{
		{
			name:   "single root",
			append: 1,
		},
		{
			name:   "multiple roots",
			append: 100,
		},
		{
			name: "swap roots",
			swap: [][2]uint64{{0, 1}, {2, 3}, {4, 5}, {0, 100}},
		},
		{
			name: "trim roots",
			trim: 3,
		},
		{
			name:   "append, swap, trim",
			append: 1,
			swap:   [][2]uint64{{50, 68}},
			trim:   10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			updater, err := c.ReviseContract(rev.Revision.ParentID)
			if err != nil {
				t.Fatal(err)
			}
			defer updater.Close()

			for i := 0; i < test.append; i++ {
				root := frand.Entropy256()
				release, err := db.StoreSector(root, func(loc storage.SectorLocation, exists bool) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				updater.AppendSector(root)
				roots = append(roots, root)
			}

			for _, swap := range test.swap {
				if err := updater.SwapSectors(swap[0], swap[1]); err != nil {
					t.Fatal(err)
				}
				roots[swap[0]], roots[swap[1]] = roots[swap[1]], roots[swap[0]]
			}

			if err := updater.TrimSectors(test.trim); err != nil {
				t.Fatal(err)
			}
			roots = roots[:len(roots)-int(test.trim)]

			if updater.MerkleRoot() != rhp2.MetaRoot(roots) {
				t.Fatal("wrong merkle root")
			} else if err := updater.Commit(rev, contracts.Usage{}); err != nil {
				t.Fatal(err)
			} else if err := updater.Close(); err != nil {
				t.Fatal(err)
			}

			// check that the sector roots are correct in the database
			dbRoots, err := db.SectorRoots(rev.Revision.ParentID)
			if err != nil {
				t.Fatal(err)
			} else if rhp2.MetaRoot(dbRoots) != rhp2.MetaRoot(roots) {
				t.Fatal("wrong merkle root in database")
			}
			// check that the cache sector roots are correct
			cachedRoots, err := c.SectorRoots(rev.Revision.ParentID, 0, 0)
			if err != nil {
				t.Fatal(err)
			} else if rhp2.MetaRoot(cachedRoots) != rhp2.MetaRoot(roots) {
				t.Fatal("wrong merkle root in cache")
			}
		})
	}
}

func TestUsageAdd(t *testing.T) {
	var ua, ub contracts.Usage
	var expected contracts.Usage
	uav := reflect.ValueOf(&ua).Elem()
	ubv := reflect.ValueOf(&ub).Elem()
	ev := reflect.ValueOf(&expected).Elem()

	for i := 0; i < uav.NumField(); i++ {
		va := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		vb := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		total := va.Add(vb)

		uav.Field(i).Set(reflect.ValueOf(va))
		ubv.Field(i).Set(reflect.ValueOf(vb))
		ev.Field(i).Set(reflect.ValueOf(total))
	}

	total := ua.Add(ub)
	tv := reflect.ValueOf(total)
	for i := 0; i < tv.NumField(); i++ {
		va := ev.Field(i).Interface().(types.Currency)
		vb := tv.Field(i).Interface().(types.Currency)
		if !va.Equals(vb) {
			t.Fatalf("field %v: expected %v, got %v", tv.Type().Field(i).Name, va, vb)
		}
	}
}
