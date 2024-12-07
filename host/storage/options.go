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
