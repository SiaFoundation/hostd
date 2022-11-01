package rhp

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

const (
	defaultPriceTableExpiration = 10 * time.Minute
)

type (
	// exiringPriceTable pairs a price table UID with an expiration timestamp.
	exiringPriceTable struct {
		uid    [16]byte
		expiry time.Time
	}

	// A priceTableManager handles registered price tables and their expiration.
	priceTableManager struct {
		mu sync.RWMutex // protects the fields below

		// expirationList is a doubly linked list of price table UIDs. The list
		// will naturally be sorted by expiration time since validity is
		// constant and new price tables are added to the back of the
		// list.
		expirationList *list.List
		// expirationTimer is a timer that fires when the next price table
		// expires. It is created using time.AfterFunc. It is set by the first
		// call to RegisterPriceTable and reset by pruneExpired.
		expirationTimer *time.Timer
		// priceTables is a map of valid price tables. The key is the UID of the
		// price table. Keys are deleted by the loop in pruneExpired.
		priceTables map[[16]byte]PriceTable
	}
)

// expirePriceTables removes expired price tables from the list of valid price
// tables. It is called by expirationTimer every time a price table expires.
func (pm *priceTableManager) pruneExpired() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	// loop through each price table and remove any that have expired
	for {
		ele := pm.expirationList.Front()
		if ele == nil {
			// no price tables remain, wait for a new one to be registered
			return
		}
		pt := ele.Value.(exiringPriceTable)
		// if the price table has not expired, reset the timer and return
		if rem := time.Until(pt.expiry); rem > 0 {
			// reset will cause pruneExpired to be called after the
			// remaining time.
			pm.expirationTimer.Reset(rem)
			return
		}
		// remove the uid from the list and the price table from the map
		pm.expirationList.Remove(ele)
		delete(pm.priceTables, pt.uid)
	}
}

// Get returns the price table with the given UID if it exists and
// has not expired.
func (pm *priceTableManager) Get(id [16]byte) (PriceTable, error) {
	pm.mu.RLock()
	pt, ok := pm.priceTables[id]
	pm.mu.RUnlock()
	if !ok {
		return PriceTable{}, fmt.Errorf("unrecognized price table ID: %x", id)
	}
	return pt, nil
}

// Register adds a price table to the list of valid price tables.
func (pm *priceTableManager) Register(pt PriceTable) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	expiration := time.Now().Add(defaultPriceTableExpiration)
	// add the price table the the map
	pm.priceTables[pt.UID] = pt
	// push the ID and expiration to the back of the list
	pm.expirationList.PushBack(exiringPriceTable{
		uid:    pt.UID,
		expiry: expiration,
	})
	if pm.expirationTimer == nil {
		// the expiration timer has not been set, set it now
		pm.expirationTimer = time.AfterFunc(time.Until(expiration), pm.pruneExpired)
	} else if len(pm.priceTables) == 1 {
		// if this is the only price table, reset the expiration timer. Reset()
		// will cause pruneExpired to be called after the remaining time. If
		// there are other price tables in the list, the timer should already be
		// set.
		pm.expirationTimer.Reset(time.Until(expiration))
	}
}

// newPriceTableManager creates a new price table manager. It is safe for
// concurrent use.
func newPriceTableManager() *priceTableManager {
	pm := &priceTableManager{
		expirationList: list.New(),
		priceTables:    make(map[[16]byte]PriceTable),
	}
	return pm
}
