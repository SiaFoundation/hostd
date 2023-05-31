package metrics

import (
	"fmt"
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
	start, err := Normalize(start, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize start time: %w", err)
	}
	return mm.store.PeriodMetrics(start, periods, interval)
}

// Metrics returns the current metrics for the host.
func (mm *MetricManager) Metrics(timestamp time.Time) (m Metrics, err error) {
	return mm.store.Metrics(timestamp)
}

// Normalize returns the normalized timestamp for the given interval.
func Normalize(timestamp time.Time, interval Interval) (time.Time, error) {
	switch interval {
	case Interval15Minutes:
		return timestamp.Truncate(15 * time.Minute), nil
	case IntervalHourly:
		return timestamp.Truncate(time.Hour), nil
	case IntervalDaily:
		y, m, d := timestamp.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, timestamp.Location()), nil
	case IntervalWeekly:
		y, m, d := timestamp.Date()
		return time.Date(y, m, d-int(timestamp.Weekday()), 0, 0, 0, 0, timestamp.Location()), nil
	case IntervalMonthly:
		y, m, _ := timestamp.Date()
		return time.Date(y, m, 1, 0, 0, 0, 0, timestamp.Location()), nil
	case IntervalYearly:
		return time.Date(timestamp.Year(), 1, 1, 0, 0, 0, 0, timestamp.Location()), nil
	default:
		return time.Time{}, fmt.Errorf("invalid interval: %q", interval)
	}
}

// NewManager returns a new MetricManager
func NewManager(store Store) *MetricManager {
	return &MetricManager{
		store: store,
	}
}
