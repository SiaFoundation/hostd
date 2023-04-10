package metrics

import (
	"errors"
	"strings"
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
	// RevenueMetrics is a collection of metrics related to revenue.
	RevenueMetrics struct {
		RPC           types.Currency `json:"rpc"`
		Storage       types.Currency `json:"storage"`
		Ingress       types.Currency `json:"ingress"`
		Egress        types.Currency `json:"egress"`
		RegistryRead  types.Currency `json:"registryRead"`
		RegistryWrite types.Currency `json:"registryWrite"`
	}

	// ContractMetrics is a collection of metrics related to contracts.
	ContractMetrics struct {
		Pending    uint64 `json:"pending"`
		Active     uint64 `json:"active"`
		Rejected   uint64 `json:"rejected"`
		Failed     uint64 `json:"failed"`
		Successful uint64 `json:"successful"`
	}

	// StorageMetrics is a collection of metrics related to storage.
	StorageMetrics struct {
		PhysicalSectors uint64 `json:"physicalSectors"`
		ContractSectors uint64 `json:"contractSectors"`
		TempSectors     uint64 `json:"tempSectors"`
		RegistryEntries uint64 `json:"registryEntries"`
	}

	// Metrics is a collection of metrics for the host.
	Metrics struct {
		PotentialRevenue RevenueMetrics  `json:"potentialRevenue"`
		EarnedRevenue    RevenueMetrics  `json:"earnedRevenue"`
		Contracts        ContractMetrics `json:"contracts"`
		Storage          StorageMetrics  `json:"storage"`
		Balance          types.Currency  `json:"balance"`
		Timestamp        time.Time       `json:"timestamp"`
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
		return "1h"
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
	switch strings.ToLower(string(buf)) {
	case "15m":
		*i = Interval15Minutes
		return nil
	case "1h":
		*i = IntervalHourly
		return nil
	case "daily":
		*i = IntervalDaily
		return nil
	case "weekly":
		*i = IntervalWeekly
		return nil
	case "monthly":
		*i = IntervalMonthly
		return nil
	case "yearly":
		*i = IntervalYearly
		return nil
	}
	return errors.New("invalid interval")
}
