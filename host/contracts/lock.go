package contracts

import (
	"context"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
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

// Lock locks a contract for modification.
//
// Deprecated: Use LockV2Contract instead.
func (cm *Manager) Lock(ctx context.Context, id types.FileContractID) (SignedRevision, error) {
	ctx, cancel, err := cm.tg.AddContext(ctx)
	if err != nil {
		return SignedRevision{}, err
	}
	defer cancel()

	if err := cm.locks.Lock(ctx, id); err != nil {
		return SignedRevision{}, err
	}

	contract, err := cm.store.Contract(id)
	if err != nil {
		cm.locks.Unlock(id)
		return SignedRevision{}, fmt.Errorf("failed to get contract: %w", err)
	} else if err := cm.isGoodForModification(contract); err != nil {
		cm.locks.Unlock(id)
		return SignedRevision{}, fmt.Errorf("contract is not good for modification: %w", err)
	}
	return contract.SignedRevision, nil
}

// Unlock unlocks a locked contract.
//
// Deprecated: Use LockV2Contract instead.
func (cm *Manager) Unlock(id types.FileContractID) {
	cm.locks.Unlock(id)
}

// LockV2Contract locks a contract for modification. The returned unlock function
// must be called to release the lock.
func (cm *Manager) LockV2Contract(id types.FileContractID) (rev rhp4.RevisionState, unlock func(), _ error) {
	done, err := cm.tg.Add()
	if err != nil {
		return rhp4.RevisionState{}, nil, err
	}
	defer done()

	// blocking is fine because locks are held for a short time
	if err := cm.locks.Lock(context.Background(), id); err != nil {
		return rhp4.RevisionState{}, nil, err
	}

	contract, err := cm.store.V2Contract(id)
	if err != nil {
		cm.locks.Unlock(id)
		return rhp4.RevisionState{}, nil, fmt.Errorf("failed to get contract: %w", err)
	}

	var once sync.Once
	return rhp4.RevisionState{
			Revision: contract.V2FileContract,
			Roots:    cm.getSectorRoots(id),
		}, func() {
			once.Do(func() {
				cm.locks.Unlock(id)
			})
		}, nil
}
