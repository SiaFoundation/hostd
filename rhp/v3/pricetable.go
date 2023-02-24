package rhp

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

const (
	defaultPriceTableExpiration = 10 * time.Minute
)

type (
	// expiringPriceTable pairs a price table UID with an expiration timestamp.
	expiringPriceTable struct {
		uid    rhpv3.SettingsID
		expiry time.Time
	}

	// A priceTableManager handles registered price tables and their expiration.
	priceTableManager struct {
		mu sync.RWMutex // protects the fields below

		// expirationList is a doubly linked list of price table UIDs. The list
		// will naturally be sorted by expiration time since validity is
		// constant and new price tables are appended to the list.
		expirationList *list.List
		// expirationTimer is a timer that fires when the next price table
		// expires. It is created using time.AfterFunc. It is set by the first
		// call to RegisterPriceTable and reset by pruneExpired.
		expirationTimer *time.Timer
		// priceTables is a map of valid price tables. The key is the UID of the
		// price table. Keys are removed by the loop in pruneExpired.
		priceTables map[rhpv3.SettingsID]rhpv3.HostPriceTable
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
			return
		}

		pt := ele.Value.(expiringPriceTable)
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
func (pm *priceTableManager) Get(id [16]byte) (rhpv3.HostPriceTable, error) {
	pm.mu.RLock()
	pt, ok := pm.priceTables[id]
	pm.mu.RUnlock()
	if !ok {
		return rhpv3.HostPriceTable{}, fmt.Errorf("unrecognized price table ID: %x", id)
	}
	return pt, nil
}

// Register adds a price table to the list of valid price tables.
func (pm *priceTableManager) Register(pt rhpv3.HostPriceTable) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	expiration := time.Now().Add(pt.Validity)
	pm.priceTables[pt.UID] = pt
	pm.expirationList.PushBack(expiringPriceTable{
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

// PriceTable returns the session handler's current price table.
func (sh *SessionHandler) PriceTable() (rhpv3.HostPriceTable, error) {
	settings := sh.settings.Settings()
	count, max, err := sh.registry.Entries()
	if err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to get registry entries: %w", err)
	}

	fee := sh.tpool.RecommendedFee()
	currentHeight := sh.chain.TipState().Index.Height
	oneHasting := types.NewCurrency64(1)
	return rhpv3.HostPriceTable{
		UID:             frand.Entropy128(),
		HostBlockHeight: currentHeight,
		Validity:        defaultPriceTableExpiration,

		// ephemeral account costs
		AccountBalanceCost:   oneHasting,
		FundAccountCost:      oneHasting,
		UpdatePriceTableCost: oneHasting,

		// MDM costs
		HasSectorBaseCost:   oneHasting,
		MemoryTimeCost:      oneHasting,
		DropSectorsBaseCost: oneHasting,
		DropSectorsUnitCost: oneHasting,
		SwapSectorBaseCost:  oneHasting,

		ReadBaseCost:    settings.SectorAccessPrice,
		ReadLengthCost:  oneHasting,
		WriteBaseCost:   settings.SectorAccessPrice,
		WriteLengthCost: oneHasting,
		WriteStoreCost:  settings.MinStoragePrice,
		InitBaseCost:    settings.BaseRPCPrice,

		// bandwidth costs
		DownloadBandwidthCost: settings.MinEgressPrice,
		UploadBandwidthCost:   settings.MinIngressPrice,

		// LatestRevisionCost is set to a reasonable base + the estimated
		// bandwidth cost of downloading a filecontract. This isn't perfect but
		// at least scales a bit as the host updates their download bandwidth
		// prices.
		LatestRevisionCost: settings.BaseRPCPrice.Add(settings.MinEgressPrice.Mul64(2048)),

		// Contract Formation/Renewal related fields
		ContractPrice:     settings.ContractPrice,
		CollateralCost:    settings.Collateral,
		MaxCollateral:     settings.MaxCollateral,
		MaxDuration:       settings.MaxContractDuration,
		WindowSize:        144,
		RenewContractCost: types.Siacoins(100).Div64(1e9),

		// Registry related fields.
		RegistryEntriesLeft:  max - count,
		RegistryEntriesTotal: max,

		// Subscription related fields.
		SubscriptionMemoryCost:       oneHasting,
		SubscriptionNotificationCost: oneHasting,

		// TxnFee related fields.
		TxnFeeMinRecommended: fee.Div64(3),
		TxnFeeMaxRecommended: fee,
	}, nil
}

// readPriceTable reads the price table ID from the stream and returns an error
// if the price table is invalid or expired.
func (sh *SessionHandler) readPriceTable(s *rhpv3.Stream) (rhpv3.HostPriceTable, error) {
	// read the price table ID from the stream
	var uid rhpv3.SettingsID
	if err := readRequest(s, &uid, 16, 30*time.Second); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to read price table ID: %w", err)
	}
	return sh.priceTables.Get(uid)
}

// newPriceTableManager creates a new price table manager. It is safe for
// concurrent use.
func newPriceTableManager() *priceTableManager {
	pm := &priceTableManager{
		expirationList: list.New(),
		priceTables:    make(map[rhpv3.SettingsID]rhpv3.HostPriceTable),
	}
	return pm
}
