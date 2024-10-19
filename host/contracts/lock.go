package contracts

import (
	"context"
	"sync"

	"go.sia.tech/core/types"
)

type (
	lock struct {
		ch chan struct{}
		n  int
	}

	locker struct {
		mu    sync.Mutex
		locks map[types.FileContractID]*lock
	}
)

func newLocker() *locker {
	l := &locker{
		locks: make(map[types.FileContractID]*lock),
	}
	return l
}

// Unlock releases a lock on the given contract ID. If the lock is not held, the
// function will panic.
func (lr *locker) Unlock(id types.FileContractID) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	l, ok := lr.locks[id]
	if !ok {
		panic("unlocking unheld lock") // developer error
	}
	l.n--
	if l.n == 0 {
		delete(lr.locks, id)
	} else {
		l.ch <- struct{}{}
	}
}

// Lock acquires a lock on the given contract ID. If the lock is already held, the
// function will block until the lock is released or the context is canceled.
// If the context is canceled, the function will return an error.
func (lr *locker) Lock(ctx context.Context, id types.FileContractID) error {
	lr.mu.Lock()
	l, ok := lr.locks[id]
	if !ok {
		// immediately acquire the lock
		defer lr.mu.Unlock()
		l = &lock{
			ch: make(chan struct{}, 1),
			n:  1,
		}
		lr.locks[id] = l
		return nil
	}
	l.n++
	lr.mu.Unlock() // unlock before waiting to avoid deadlock
	select {
	case <-ctx.Done():
		lr.mu.Lock()
		l.n--
		if l.n == 0 {
			delete(lr.locks, id)
		}
		lr.mu.Unlock()
		return ctx.Err()
	case <-l.ch:
		return nil
	}
}
