package rhp

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

const persistInterval = 30 * time.Second

type (
	rhp2Recorder struct {
		recorder *DataRecorder
	}

	rhp3Recorder struct {
		recorder *DataRecorder
	}

	// DataUsage represents the amount of data read and written across a
	// connection.
	DataUsage struct {
		Ingress uint64
		Egress  uint64
	}

	// A DataRecorderStore persists data usage
	DataRecorderStore interface {
		IncrementDataUsage(rhp2, rhp3 DataUsage) error
	}

	// A DataRecorder records the amount of data read and written across
	// connections.
	DataRecorder struct {
		store DataRecorderStore
		log   *zap.Logger
		close chan struct{}

		mu   sync.Mutex // guards the following fields
		rhp2 DataUsage
		rhp3 DataUsage
	}
)

// ReadBytes increments the number of bytes read by n.
func (r *rhp2Recorder) ReadBytes(n int) {
	r.recorder.mu.Lock()
	defer r.recorder.mu.Unlock()
	r.recorder.rhp2.Ingress += uint64(n)
}

// WriteBytes increments the number of bytes written by n.
func (r *rhp2Recorder) WriteBytes(n int) {
	r.recorder.mu.Lock()
	defer r.recorder.mu.Unlock()
	r.recorder.rhp2.Egress += uint64(n)
}

// ReadBytes increments the number of bytes read by n.
func (r *rhp3Recorder) ReadBytes(n int) {
	r.recorder.mu.Lock()
	defer r.recorder.mu.Unlock()
	r.recorder.rhp3.Ingress += uint64(n)
}

// WriteBytes increments the number of bytes written by n.
func (r *rhp3Recorder) WriteBytes(n int) {
	r.recorder.mu.Lock()
	defer r.recorder.mu.Unlock()
	r.recorder.rhp3.Egress += uint64(n)
}

// RHP2 returns a DataMonitor for RHP2 connections.
func (dr *DataRecorder) RHP2() DataMonitor {
	return &rhp2Recorder{dr}
}

// RHP3 returns a DataMonitor for RHP3 connections.
func (dr *DataRecorder) RHP3() DataMonitor {
	return &rhp3Recorder{dr}
}

// persistUsage flushes the current usage to the database.
func (dr *DataRecorder) persistUsage() {
	// copy the current usage
	dr.mu.Lock()
	rhp2, rhp3 := dr.rhp2, dr.rhp3
	dr.mu.Unlock()

	// persist the usage
	if err := dr.store.IncrementDataUsage(rhp2, rhp3); err != nil {
		dr.log.Error("failed to persist data usage", zap.Error(err))
		return
	}

	// decrement the flushed usage
	dr.mu.Lock()
	dr.rhp2.Ingress -= rhp2.Ingress
	dr.rhp2.Egress -= rhp2.Egress
	dr.rhp3.Ingress -= rhp3.Ingress
	dr.rhp3.Egress -= rhp3.Egress
	dr.mu.Unlock()
}

// Close persists any remaining usage and returns nil
func (dr *DataRecorder) Close() error {
	select {
	case <-dr.close:
		return nil
	default:
		close(dr.close)
	}
	dr.persistUsage()
	return nil
}

// NewDataRecorder initializes a new DataRecorder
func NewDataRecorder(store DataRecorderStore, log *zap.Logger) *DataRecorder {
	recorder := &DataRecorder{
		store: store,
		log:   log,
		close: make(chan struct{}),
	}
	go func() {
		t := time.NewTicker(persistInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				recorder.persistUsage()
			case <-recorder.close:
				return
			}
		}
	}()
	return recorder
}
