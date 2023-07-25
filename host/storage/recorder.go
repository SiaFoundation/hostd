package storage

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const flushInterval = 10 * time.Second

type (
	sectorAccessRecorder struct {
		store VolumeStore
		log   *zap.Logger

		mu sync.Mutex
		r  uint64
		w  uint64

		cacheHit  uint64
		cacheMiss uint64
	}
)

// Flush persists the number of sectors read and written.
func (sr *sectorAccessRecorder) Flush() {
	sr.mu.Lock()
	r, w := sr.r, sr.w
	cacheHit, cacheMiss := sr.cacheHit, sr.cacheMiss
	sr.r, sr.w = 0, 0
	sr.cacheHit, sr.cacheMiss = 0, 0
	sr.mu.Unlock()

	// no need to persist if there is no change
	if r == 0 && w == 0 {
		return
	}

	if err := sr.store.IncrementSectorStats(r, w, cacheHit, cacheMiss); err != nil {
		sr.log.Error("failed to persist sector access", zap.Error(err))
		return
	}
}

// AddRead increments the number of sectors read by 1.
func (sr *sectorAccessRecorder) AddRead() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.r++
}

// AddWrite increments the number of sectors written by 1.
func (sr *sectorAccessRecorder) AddWrite() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.w++
}

func (sr *sectorAccessRecorder) AddCacheHit() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.cacheHit++
}

func (sr *sectorAccessRecorder) AddCacheMiss() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.cacheMiss++
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
