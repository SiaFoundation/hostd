package accounts

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	"go.sia.tech/siad/types"
)

type (
	// An AccountID is a unique identifier for an account. It must be an ed25519
	// public key to sign withdrawals from the account.
	AccountID [32]byte

	// An AccountStore stores and updates account balances.
	AccountStore interface {
		// Balance returns the balance of the account with the given ID.
		Balance(accountID AccountID) (types.Currency, error)
		// Credit adds the specified amount to the account with the given ID.
		Credit(accountID AccountID, amount types.Currency) (types.Currency, error)
		// Debit subtracts the specified amount from the account with the given
		// ID. Returns the remaining balance of the account.
		Debit(accountID AccountID, amount types.Currency) (types.Currency, error)

		Close() error
	}

	accountState struct {
		balance  types.Currency
		openTxns int
	}

	// An AccountManager manages deposits and withdrawals for accounts. It is
	// primarily a synchronization wrapper around a store.
	AccountManager struct {
		store AccountStore

		mu sync.Mutex // guards the fields below

		// balances is a map of account IDs to their current balance. It
		// is used for consistency before a budget is synced to the underlying
		// store.
		balances map[AccountID]accountState

		// ch is used to wake all blocked withdrawals.
		//
		// Implementing a queue may be more performant with lots of blocked
		// goroutines, but in practice blocks are short and infrequent. Order of
		// withdrawals is not guaranteed. However, with current implementations
		// a single deposit will unblock all or most of an accounts pending
		// withdrawals.
		ch chan struct{}
	}
)

func (a *AccountID) String() string {
	return "ed25519:" + hex.EncodeToString(a[:])
}

// Close closes the underlying account store.
func (am *AccountManager) Close() error {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.store.Close()
}

// Balance returns the balance of the account with the given ID.
func (am *AccountManager) Balance(accountID AccountID) (types.Currency, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if state, ok := am.balances[accountID]; ok {
		return state.balance, nil
	}
	return am.store.Balance(accountID)
}

// Credit adds the specified amount to the account with the given ID. Credits
// are synced to the underlying store immediately.
func (am *AccountManager) Credit(accountID AccountID, amount types.Currency) (balance types.Currency, err error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	// credit the account
	if _, err = am.store.Credit(accountID, amount); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to credit account: %w", err)
	}

	// increment the balance in memory, if it exists
	if state, ok := am.balances[accountID]; ok {
		state.balance = balance.Add(amount)
		am.balances[accountID] = state
	}

	// wake all waiting withdrawals
	close(am.ch)
	am.ch = make(chan struct{})
	return balance.Add(amount), nil
}

// Budget creates a new budget for an account limited by amount. The spent
// amount will not be synced to the underlying store until Commit is called.
// This function will block until the account has enough funds to cover the full
// budget or until the context is cancelled.
func (am *AccountManager) Budget(ctx context.Context, accountID AccountID, amount types.Currency) (Budget, error) {
	// instead of monitoring each account, one global deposit channel wakes all
	// waiting withdrawals. Since the account's balance may not have changed,
	// the balance is checked in a loop.
	for {
		am.mu.Lock()

		var balance types.Currency
		// if there are currently outstanding debits, use the in-memory balance
		if state, ok := am.balances[accountID]; ok {
			balance = state.balance
		} else {
			var err error
			// otherwise, get the balance from the store
			balance, err = am.store.Balance(accountID)
			if err != nil {
				am.mu.Unlock()
				return nil, fmt.Errorf("failed to get account balance: %w", err)
			}

			// add the account to the map of balances
			state := accountState{
				balance:  balance,
				openTxns: 1,
			}
			am.balances[accountID] = state
		}

		// if the account has enough balance, deduct the amount from memory and
		// return a budget
		if balance.Cmp(amount) >= 0 {
			// update the balance in memory
			state := am.balances[accountID]
			state.balance = balance.Sub(amount)
			am.balances[accountID] = state

			am.mu.Unlock()
			return &budget{
				accountID: accountID,
				max:       amount,

				am: am,
			}, nil
		}

		// grab the channel under lock then unlock the mutex so other debits
		// can fire.
		ch := am.ch
		am.mu.Unlock()

		// wait for a deposit or for the context to be cancelled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
			// a deposit has occurred -- recheck the balance.
		}
	}
}

// NewManager creates a new account manager
func NewManager(store AccountStore) *AccountManager {
	return &AccountManager{
		ch:    make(chan struct{}),
		store: store,

		balances: make(map[AccountID]accountState),
	}
}
