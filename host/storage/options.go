package storage

import (
	"time"

	"go.uber.org/zap"
)

// A VolumeManagerOption configures a VolumeManager.
type VolumeManagerOption func(*VolumeManager)

// WithLogger sets the logger for the manager.
func WithLogger(l *zap.Logger) VolumeManagerOption {
	return func(s *VolumeManager) {
		s.log = l
	}
}

// WithAlerter sets the alerter for the manager.
func WithAlerter(a Alerts) VolumeManagerOption {
	return func(s *VolumeManager) {
		s.alerts = a
	}
}

// WithCacheSize sets the sector cache size for the manager.
func WithCacheSize(cacheSize int) VolumeManagerOption {
	return func(s *VolumeManager) {
		s.cacheSize = cacheSize
	}
}

// WithPruneInterval sets the time between cleaning up dereferenced
// sectors.
func WithPruneInterval(d time.Duration) VolumeManagerOption {
	return func(vm *VolumeManager) {
		vm.pruneInterval = d
	}
}

// WithMerkleCacheEnabled sets whether the merkle root cache is enabled (default true).
// When enabled, the merkle root cache will store the Merkle subtree roots
// for all sectors in the database to reduce disk IO for partial sector reads.
//
// This reduces the amount of disk IO required to serve partial sector reads,
// at the cost of database size.
//
// The overhead is ~8GiB per TiB of stored data (~0.8% overhead), but reduces
// disk IO by 1000x for random partial sector reads.
func WithMerkleCacheEnabled(enabled bool) VolumeManagerOption {
	return func(vm *VolumeManager) {
		vm.merkleCacheEnabled = enabled
	}
}
