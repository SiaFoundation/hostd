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
		wg     sync.WaitGroup
		closed chan struct{}
	}
)

// ErrClosed is returned when the threadgroup has already been stopped
var ErrClosed = errors.New("threadgroup closed")

// Add adds a new thread to the group, done must be called to signal that the
// thread is done. Returns ErrClosed if the threadgroup is already closed.
func (tg *ThreadGroup) Add() (func(), error) {
	select {
	case <-tg.closed:
		return nil, ErrClosed
	default:
	}

	tg.wg.Add(1)
	return func() { tg.wg.Done() }, nil
}

// AddContext adds a new thread to the group and wraps the parent context. The
// returned context will be cancelled if the parent context is cancelled or if
// the threadgroup is stopped. The returned cancel func must be called to signal
// the thread is done. The cancel func is safe to call multiple times.
func (tg *ThreadGroup) AddContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	// try to add to the group
	done, err := tg.Add()
	if err != nil {
		return nil, nil, err
	}

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
	var once sync.Once
	return ctx, func() {
		cancel()
		once.Do(done)
	}, nil
}

// Stop stops accepting new threads and waits for all existing threads to close
func (tg *ThreadGroup) Stop() {
	select {
	case <-tg.closed:
	default:
		close(tg.closed)
	}
	tg.wg.Wait()
}

// New creates a new threadgroup
func New() *ThreadGroup {
	return &ThreadGroup{
		closed: make(chan struct{}),
	}
}
