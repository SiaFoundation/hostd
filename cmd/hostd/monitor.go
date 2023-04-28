package main

import "go.sia.tech/hostd/persist/sqlite"

type (
	// rhp2MonitorStore wraps the sqlite store and implements the
	// DataRecorderStore interface
	rhp2MonitorStore struct {
		store *sqlite.Store
	}

	// rhp3MonitorStore wraps the sqlite store and implements the
	// DataRecorderStore interface
	rhp3MonitorStore struct {
		store *sqlite.Store
	}
)

// IncrementDataUsage implements the DataRecorderStore interface
func (r2 *rhp2MonitorStore) IncrementDataUsage(ingress, egress uint64) error {
	return r2.store.IncrementRHP2DataUsage(ingress, egress)
}

// IncrementDataUsage implements the DataRecorderStore interface
func (r3 *rhp3MonitorStore) IncrementDataUsage(ingress, egress uint64) error {
	return r3.store.IncrementRHP3DataUsage(ingress, egress)
}
