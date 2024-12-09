package contracts_test

import (
	"math"
	"reflect"
	"testing"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/testutil"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestContractUpdater(t *testing.T) {
	const sectors = 256
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	log := zaptest.NewLogger(t)
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// create a fake volume so disk space is not used
	id, err := node.Store.AddVolume("test", false)
	if err != nil {
		t.Fatal(err)
	} else if err := node.Store.GrowVolume(id, sectors); err != nil {
		t.Fatal(err)
	} else if err := node.Store.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

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
				UnlockHash:  contractUnlockConditions.UnlockHash(),
				WindowStart: 100,
				WindowEnd:   200,
			},
			ParentID:         frand.Entropy256(),
			UnlockConditions: contractUnlockConditions,
		},
	}

	if err := node.Contracts.AddContract(rev, []types.Transaction{}, types.ZeroCurrency, contracts.Usage{}); err != nil {
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
			updater, err := node.Contracts.ReviseContract(rev.Revision.ParentID)
			if err != nil {
				t.Fatal(err)
			}
			defer updater.Close()

			for i := 0; i < test.append; i++ {
				root := frand.Entropy256()
				err := node.Store.StoreSector(root, func(loc storage.SectorLocation) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
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
			allRoots, err := node.Store.SectorRoots()
			if err != nil {
				t.Fatal(err)
			} else if rhp2.MetaRoot(allRoots[rev.Revision.ParentID]) != rhp2.MetaRoot(roots) {
				t.Fatal("wrong merkle root in database")
			}
			// check that the cache sector roots are correct
			cachedRoots := node.Contracts.SectorRoots(rev.Revision.ParentID)
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

func TestUsageSub(t *testing.T) {
	var ua, ub contracts.Usage
	var expected contracts.Usage
	uav := reflect.ValueOf(&ua).Elem()
	ubv := reflect.ValueOf(&ub).Elem()
	ev := reflect.ValueOf(&expected).Elem()

	for i := 0; i < uav.NumField(); i++ {
		va := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		vb := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		if va.Cmp(vb) < 0 {
			va, vb = vb, va
		}
		total := va.Sub(vb)

		uav.Field(i).Set(reflect.ValueOf(va))
		ubv.Field(i).Set(reflect.ValueOf(vb))
		ev.Field(i).Set(reflect.ValueOf(total))
	}

	total := ua.Sub(ub)
	tv := reflect.ValueOf(total)
	for i := 0; i < tv.NumField(); i++ {
		va := ev.Field(i).Interface().(types.Currency)
		vb := tv.Field(i).Interface().(types.Currency)
		if !va.Equals(vb) {
			t.Fatalf("field %v: expected %v, got %v", tv.Type().Field(i).Name, va, vb)
		}
	}
}
