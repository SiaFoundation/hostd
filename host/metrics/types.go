package metrics

import "time"

type (
	ContractMetrics struct {
		Pending    uint64 `json:"pending"`
		Active     uint64 `json:"active"`
		Rejected   uint64 `json:"rejected"`
		Failed     uint64 `json:"failed"`
		Successful uint64 `json:"successful"`
	}

	StorageMetrics struct {
		PhysicalSectors uint64 `json:"physicalSectors"`
		ContractSectors uint64 `json:"contractSectors"`
		TempSectors     uint64 `json:"tempSectors"`
	}

	Metrics struct {
		Contracts ContractMetrics `json:"contracts"`
		Storage   StorageMetrics  `json:"storage"`
		Timestamp time.Time       `json:"timestamp"`
	}
)
