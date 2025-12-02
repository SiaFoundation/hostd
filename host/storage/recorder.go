package storage

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const flushInterval = time.Minute

type (
	// SectorMetrics tracks the metrics related to sector access.
	SectorMetrics struct {
		ReadCount  uint64
		WriteCount uint64

		ReadBytes  uint64
		WriteBytes uint64

		CacheHit  uint64
		CacheMiss uint64
	}

	sectorAccessRecorder struct {
		store VolumeStore
		log   *zap.Logger

		mu      sync.Mutex
		metrics SectorMetrics
	}
)

// Flush persists the number of sectors read and written.
func (sr *sectorAccessRecorder) Flush() {
	sr.mu.Lock()
	metrics := sr.metrics
	sr.metrics = SectorMetrics{}
	sr.mu.Unlock()

	if err := sr.store.IncrementSectorMetrics(metrics); err != nil {
		sr.log.Error("failed to persist sector access", zap.Error(err))
	}
}

// AddRead increments the number of sectors read by 1
// and the number of bytes read by n.
func (sr *sectorAccessRecorder) AddRead(n uint64) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.metrics.ReadCount++
	sr.metrics.ReadBytes += n
}

// AddWrite increments the number of sectors written by 1.
// and the number of bytes written by n.
func (sr *sectorAccessRecorder) AddWrite(n uint64) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.metrics.WriteCount++
	sr.metrics.WriteBytes += n
}

func (sr *sectorAccessRecorder) AddCacheHit() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.metrics.CacheHit++
}

func (sr *sectorAccessRecorder) AddCacheMiss() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.metrics.CacheMiss++
}

// Run starts the recorder, flushing data at regular intervals.
func (sr *sectorAccessRecorder) Run(stop <-chan struct{}) {
	t := time.NewTicker(flushInterval)
	for {
		select {
		case <-stop:
			t.Stop()
			return
		case <-t.C:
			sr.Flush()
		}
	}
}
