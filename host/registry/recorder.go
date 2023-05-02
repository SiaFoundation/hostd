package registry

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const flushInterval = 10 * time.Second

type (
	registryAccessRecorder struct {
		store Store
		log   *zap.Logger

		mu sync.Mutex
		r  uint64
		w  uint64
	}
)

// Flush persists the number of sectors read and written.
func (rr *registryAccessRecorder) Flush() {
	rr.mu.Lock()
	r, w := rr.r, rr.w
	rr.r, rr.w = 0, 0
	rr.mu.Unlock()

	// no need to persist if there is no change
	if r == 0 && w == 0 {
		return
	}

	if err := rr.store.IncrementRegistryAccess(r, w); err != nil {
		rr.log.Error("failed to persist sector access", zap.Error(err))
		return
	}
}

// AddRead increments the number of sectors read by 1.
func (rr *registryAccessRecorder) AddRead() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.r++
}

// AddWrite increments the number of sectors written by 1.
func (rr *registryAccessRecorder) AddWrite() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.w++
}

// Run starts the recorder, flushing data at regular intervals.
func (rr *registryAccessRecorder) Run(stop <-chan struct{}) {
	t := time.NewTicker(flushInterval)
	for {
		select {
		case <-stop:
			t.Stop()
			return
		case <-t.C:
		}
		rr.Flush()
	}
}
