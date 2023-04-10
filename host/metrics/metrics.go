package metrics

import (
	"time"
)

type (
	// A Store retrieves metrics
	Store interface {
		// PeriodMetrics returns aggregated metrics for the period between start and end.
		PeriodMetrics(start, end time.Time, interval Interval) (period []Metrics, err error)
		// Metrics returns aggregated metrics for the host as of the timestamp.
		Metrics(time.Time) (m Metrics, err error)
	}

	// A MetricManager retrieves metrics from a store
	MetricManager struct {
		store Store // note: this is currently a thin wrapper around the store, but may be expanded in the future
	}
)

// PeriodMetrics returns aggregated PeriodMetrics for the period between start and end.
func (mm *MetricManager) PeriodMetrics(start, end time.Time, interval Interval) (period []Metrics, err error) {
	return mm.store.PeriodMetrics(start, end, interval)
}

// Metrics returns the current metrics for the host.
func (mm *MetricManager) Metrics(timestamp time.Time) (m Metrics, err error) {
	return mm.store.Metrics(timestamp)
}

// NewManager returns a new MetricManager
func NewManager(store Store) *MetricManager {
	return &MetricManager{
		store: store,
	}
}
