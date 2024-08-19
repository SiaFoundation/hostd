package metrics

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

// intervals at which metrics should be aggregated.
const (
	Interval5Minutes Interval = iota
	Interval15Minutes
	IntervalHourly
	IntervalDaily
	IntervalWeekly
	IntervalMonthly
	IntervalYearly
)

type (
	// Revenue is a collection of metrics related to revenue.
	Revenue struct {
		RPC           types.Currency `json:"rpc"`
		Storage       types.Currency `json:"storage"`
		Ingress       types.Currency `json:"ingress"`
		Egress        types.Currency `json:"egress"`
		RegistryRead  types.Currency `json:"registryRead"`
		RegistryWrite types.Currency `json:"registryWrite"`
	}

	// DataMetrics is a collection of metrics related to data usage.
	DataMetrics struct {
		RHP RHPData `json:"rhp"`
	}

	// RHPData is a collection of data metrics related to the RHP.
	RHPData struct {
		// Ingress returns the number of bytes received by the host.
		Ingress uint64 `json:"ingress"`
		// Egress returns the number of bytes sent by the host.
		Egress uint64 `json:"egress"`
	}

	// Contracts is a collection of metrics related to contracts.
	Contracts struct {
		Active     uint64 `json:"active"`
		Rejected   uint64 `json:"rejected"`
		Failed     uint64 `json:"failed"`
		Renewed    uint64 `json:"renewed"`
		Finalized  uint64 `json:"finalized"`
		Successful uint64 `json:"successful"`

		LockedCollateral types.Currency `json:"lockedCollateral"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
	}

	// Accounts is a collection of metrics related to ephemeral accounts.
	Accounts struct {
		Active  uint64         `json:"active"`
		Balance types.Currency `json:"balance"`
	}

	// Pricing is a collection of metrics related to the host's pricing settings.
	Pricing struct {
		ContractPrice        types.Currency `json:"contractPrice"`
		IngressPrice         types.Currency `json:"ingressPrice"`
		EgressPrice          types.Currency `json:"egressPrice"`
		BaseRPCPrice         types.Currency `json:"baseRPCPrice"`
		SectorAccessPrice    types.Currency `json:"sectorAccessPrice"`
		StoragePrice         types.Currency `json:"storagePrice"`
		CollateralMultiplier float64        `json:"collateralMultiplier"`
	}

	// Registry is a collection of metrics related to the host's registry.
	Registry struct {
		Entries    uint64 `json:"entries"`
		MaxEntries uint64 `json:"maxEntries"`

		Reads  uint64 `json:"reads"`
		Writes uint64 `json:"writes"`
	}

	// Storage is a collection of metrics related to storage.
	Storage struct {
		TotalSectors    uint64 `json:"totalSectors"`
		PhysicalSectors uint64 `json:"physicalSectors"`
		LostSectors     uint64 `json:"lostSectors"`
		ContractSectors uint64 `json:"contractSectors"`
		TempSectors     uint64 `json:"tempSectors"`

		Reads  uint64 `json:"reads"`
		Writes uint64 `json:"writes"`

		SectorCacheHits   uint64 `json:"sectorCacheHits"`
		SectorCacheMisses uint64 `json:"sectorCacheMisses"`
	}

	// RevenueMetrics is a collection of metrics related to revenue.
	RevenueMetrics struct {
		Potential Revenue `json:"potential"`
		Earned    Revenue `json:"earned"`
	}

	// WalletMetrics is a collection of metrics related to the wallet
	WalletMetrics struct {
		Balance         types.Currency `json:"balance"`
		ImmatureBalance types.Currency `json:"immatureBalance"`
	}

	// Metrics is a collection of metrics for the host.
	Metrics struct {
		Accounts  Accounts       `json:"accounts"`
		Revenue   RevenueMetrics `json:"revenue"`
		Pricing   Pricing        `json:"pricing"`
		Contracts Contracts      `json:"contracts"`
		Storage   Storage        `json:"storage"`
		Registry  Registry       `json:"registry"`
		Data      DataMetrics    `json:"data"`
		Wallet    WalletMetrics  `json:"wallet"`
		Timestamp time.Time      `json:"timestamp"`
	}

	// Interval is the interval at which metrics should be aggregated.
	Interval uint8
)

// String returns the interval as a string
func (i Interval) String() string {
	switch i {
	case Interval5Minutes:
		return "5m"
	case Interval15Minutes:
		return "15m"
	case IntervalHourly:
		return "hourly"
	case IntervalDaily:
		return "daily"
	case IntervalWeekly:
		return "weekly"
	case IntervalMonthly:
		return "monthly"
	case IntervalYearly:
		return "yearly"
	}
	return "unknown"
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (i *Interval) UnmarshalText(buf []byte) error {
	values := map[string]Interval{
		Interval5Minutes.String():  Interval5Minutes,
		Interval15Minutes.String(): Interval15Minutes,
		IntervalHourly.String():    IntervalHourly,
		IntervalDaily.String():     IntervalDaily,
		IntervalWeekly.String():    IntervalWeekly,
		IntervalMonthly.String():   IntervalMonthly,
		IntervalYearly.String():    IntervalYearly,
	}
	val, ok := values[string(buf)]
	if !ok {
		return fmt.Errorf("invalid interval: %v", string(buf))
	}
	*i = val
	return nil
}
