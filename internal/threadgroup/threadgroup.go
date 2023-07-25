package threadgroup

import (
	"context"
	"errors"
	"sync"
)

type (
	// A ThreadGroup provides synchronization between a module and its
	// goroutines to enable clean shutdowns
	ThreadGroup struct {
		mu     sync.Mutex
		wg     sync.WaitGroup
		closed chan struct{}
	}
)

// ErrClosed is returned when the threadgroup has already been stopped
var ErrClosed = errors.New("threadgroup closed")

// Done returns a channel that will be closed when the threadgroup is stopped
func (tg *ThreadGroup) Done() <-chan struct{} {
	return tg.closed
}

// Add adds a new thread to the group, done must be called to signal that the
// thread is done. Returns ErrClosed if the threadgroup is already closed.
func (tg *ThreadGroup) Add() (func(), error) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	select {
	case <-tg.closed:
		return nil, ErrClosed
	default:
	}
	tg.wg.Add(1)
	return func() { tg.wg.Done() }, nil
}

// WithContext returns a copy of the parent context. The returned context will
// be cancelled if the parent context is cancelled or if the threadgroup is
// stopped.
func (tg *ThreadGroup) WithContext(parent context.Context) (context.Context, context.CancelFunc) {
	// wrap the parent context in a cancellable context
	ctx, cancel := context.WithCancel(parent)
	// start a goroutine to wait for either the parent context being cancelled
	// or the threagroup being stopped
	go func() {
		select {
		case <-ctx.Done():
			break
		case <-tg.closed:
			break
		}
		// threadgroup or parent context cancelled, cancel the child context
		cancel()
	}()
	return ctx, cancel
}

// AddContext adds a new thread to the group and returns a copy of the parent
// context. It is a convenience function combining Add and WithContext.
func (tg *ThreadGroup) AddContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	// try to add to the group
	done, err := tg.Add()
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := tg.WithContext(parent)
	var once sync.Once
	return ctx, func() {
		cancel()
		once.Do(done)
	}, nil
}

// Stop stops accepting new threads and waits for all existing threads to close
func (tg *ThreadGroup) Stop() {
	tg.mu.Lock()
	select {
	case <-tg.closed:
	default:
		close(tg.closed)
	}
	tg.mu.Unlock()
	tg.wg.Wait()
}

// New creates a new threadgroup
func New() *ThreadGroup {
	return &ThreadGroup{
		closed: make(chan struct{}),
	}
}
