package rhp

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const persistInterval = 30 * time.Second

type (
	// A DataRecorderStore persists data usage
	DataRecorderStore interface {
		IncrementDataUsage(ingress, egress uint64) error
	}

	// A DataRecorder records the amount of data read and written across
	// connections.
	DataRecorder struct {
		store DataRecorderStore
		log   *zap.Logger
		t     *time.Timer

		mu     sync.Mutex // guards the following fields
		lr, lw uint64
		r, w   uint64
	}
)

// ReadBytes increments the number of bytes read by n.
func (dr *DataRecorder) ReadBytes(n int) {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	dr.r += uint64(n)
}

// WriteBytes increments the number of bytes written by n.
func (dr *DataRecorder) WriteBytes(n int) {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	dr.w += uint64(n)
}

// Usage returns the number of bytes read and written
func (dr *DataRecorder) Usage() (read, written uint64) {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	return dr.r, dr.w
}

func (dr *DataRecorder) persistUsage() {
	dr.mu.Lock()
	deltaRead, deltaWrite := dr.r-dr.lr, dr.w-dr.lw
	dr.lr = dr.r
	dr.lw = dr.w
	dr.mu.Unlock()

	// no need to persist if there is no change
	if deltaRead == 0 && deltaWrite == 0 {
		return
	}

	if err := dr.store.IncrementDataUsage(deltaRead, deltaWrite); err != nil {
		dr.log.Error("failed to persist data usage", zap.Error(err))
		return
	}
}

// Close persists any remaining usage and returns nil
func (dr *DataRecorder) Close() error {
	if !dr.t.Stop() {
		<-dr.t.C
	}
	dr.persistUsage()
	return nil
}

// NewDataRecorder initializes a new DataRecorder
func NewDataRecorder(store DataRecorderStore, log *zap.Logger) *DataRecorder {
	recorder := &DataRecorder{
		store: store,
		log:   log,
	}
	recorder.t = time.AfterFunc(persistInterval, func() {
		recorder.persistUsage()
		recorder.t.Reset(persistInterval)
	})
	return recorder
}
