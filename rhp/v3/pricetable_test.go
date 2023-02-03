package rhp

import (
	"sync"
	"testing"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"lukechampine.com/frand"
)

func TestPriceTableManager(t *testing.T) {
	pm := newPriceTableManager()
	t.Run("serial", func(t *testing.T) {
		pt := rhpv3.HostPriceTable{
			UID:      frand.Entropy128(),
			Validity: 100 * time.Millisecond,
		}
		if _, err := pm.Get(pt.UID); err == nil {
			t.Error("expected error")
		}
		pm.Register(pt)
		if _, err := pm.Get(pt.UID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
		if _, err := pm.Get(pt.UID); err == nil {
			t.Fatal("expected expiration error")
		}

		// register the price table again
		pm.Register(pt)
		if _, err := pm.Get(pt.UID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
		if _, err := pm.Get(pt.UID); err == nil {
			t.Fatal("expected expiration error")
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		// generate ids for each price table
		tables := make([][16]byte, 10)
		for i := range tables {
			tables[i] = frand.Entropy128()
		}

		// register all the tables
		var wg sync.WaitGroup
		wg.Add(len(tables))
		for _, id := range tables {
			go func(id [16]byte) {
				pm.Register(rhpv3.HostPriceTable{
					UID:      id,
					Validity: 250 * time.Millisecond,
				})
				wg.Done()
			}(id)
		}

		wg.Wait()
		// check that all the tables are valid
		for _, id := range tables {
			if _, err := pm.Get(id); err != nil {
				t.Fatal(err)
			}
		}

		// wait for all the tables to expire
		time.Sleep(500 * time.Millisecond)

		// check that all the tables are invalid
		for _, id := range tables {
			if _, err := pm.Get(id); err == nil {
				t.Fatal("expected expiration error")
			}
		}
	})
}
