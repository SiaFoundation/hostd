package metrics

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

// intervals at which metrics should be aggregated.
const (
	Interval15Minutes Interval = iota
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

	// Data is a collection of metrics related to data usage.
	Data struct {
		// Ingress returns the number of bytes received by the host.
		Ingress uint64 `json:"ingress"`
		// Egress returns the number of bytes sent by the host.
		Egress uint64 `json:"egress"`
	}

	// Contracts is a collection of metrics related to contracts.
	Contracts struct {
		Pending    uint64 `json:"pending"`
		Active     uint64 `json:"active"`
		Rejected   uint64 `json:"rejected"`
		Failed     uint64 `json:"failed"`
		Successful uint64 `json:"successful"`

		LockedCollateral types.Currency `json:"lockedCollateral"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
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

	// DataMetrics is a collection of metrics related to data usage.
	DataMetrics struct {
		RHP2 Data `json:"rhp2"`
		RHP3 Data `json:"rhp3"`
	}

	// Metrics is a collection of metrics for the host.
	Metrics struct {
		Revenue   RevenueMetrics `json:"revenue"`
		Pricing   Pricing        `json:"pricing"`
		Contracts Contracts      `json:"contracts"`
		Storage   Storage        `json:"storage"`
		Registry  Registry       `json:"registry"`
		Data      DataMetrics    `json:"data"`
		Balance   types.Currency `json:"balance"`
		Timestamp time.Time      `json:"timestamp"`
	}

	// Interval is the interval at which metrics should be aggregated.
	Interval uint8
)

// String returns the interval as a string
func (i Interval) String() string {
	switch i {
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
