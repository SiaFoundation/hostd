package metrics

import (
	"time"
)

type (
	// A Store retrieves metrics
	Store interface {
		// PeriodMetrics returns metrics for n periods starting at start.
		PeriodMetrics(start time.Time, n int, interval Interval) (period []Metrics, err error)
		// Metrics returns aggregated metrics for the host as of the timestamp.
		Metrics(time.Time) (m Metrics, err error)
	}

	// A MetricManager retrieves metrics from a store
	MetricManager struct {
		store Store // note: this is currently a thin wrapper around the store, but may be expanded in the future
	}
)

// PeriodMetrics returns metrics for n periods starting at start.
func (mm *MetricManager) PeriodMetrics(start time.Time, periods int, interval Interval) ([]Metrics, error) {
	return mm.store.PeriodMetrics(start, periods, interval)
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
