package accounts

import (
	"context"
	"fmt"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type (
	// An AccountStore stores and updates account balances.
	AccountStore interface {
		// AccountBalance returns the balance of the account with the given ID.
		AccountBalance(accountID rhpv3.Account) (types.Currency, error)
		// CreditAccount adds the specified amount to the account with the given ID.
		CreditAccount(accountID rhpv3.Account, amount types.Currency, expiration time.Time) (types.Currency, error)
		// DebitAccount subtracts the specified amount from the account with the given
		// ID. Returns the remaining balance of the account.
		DebitAccount(accountID rhpv3.Account, amount types.Currency) (types.Currency, error)
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
		balances map[rhpv3.Account]accountState

		// ch is used to wake all blocked withdrawals.
		//
		// Implementing a queue may be more performant with lots of blocked
		// goroutines, but in practice blocks are short and infrequent. Order of
		// withdrawals is not guaranteed. However, with current implementations
		// a single deposit should unblock all or most of an accounts pending
		// withdrawals.
		ch chan struct{}
	}
)

// Balance returns the balance of the account with the given ID.
func (am *AccountManager) Balance(accountID rhpv3.Account) (types.Currency, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if state, ok := am.balances[accountID]; ok {
		return state.balance, nil
	}
	return am.store.AccountBalance(accountID)
}

// Credit adds the specified amount to the account with the given ID. Credits
// are synced to the underlying store immediately.
func (am *AccountManager) Credit(accountID rhpv3.Account, amount types.Currency, expiration time.Time) (balance types.Currency, err error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	// credit the account
	if _, err = am.store.CreditAccount(accountID, amount, expiration); err != nil {
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
// This function will block until the account has enough funds to cover the
// budget or until the context is cancelled.
func (am *AccountManager) Budget(ctx context.Context, accountID rhpv3.Account, amount types.Currency) (*Budget, error) {
	// instead of monitoring each account, one global deposit channel wakes all
	// waiting withdrawals. Since the account's balance may not have changed,
	// the balance is checked in a loop.
	for {
		am.mu.Lock()

		// if there are currently outstanding debits, use the in-memory balance
		state, ok := am.balances[accountID]
		if !ok {
			var err error
			// otherwise, get the balance from the store
			balance, err := am.store.AccountBalance(accountID)
			if err != nil {
				am.mu.Unlock()
				return nil, fmt.Errorf("failed to get account balance: %w", err)
			}

			// add the account to the map of balances
			state = accountState{
				balance: balance,
			}
		}

		// if the account has enough balance, deduct the amount from memory and
		// return a budget
		var underflow bool
		state.balance, underflow = state.balance.SubWithUnderflow(amount)
		if !underflow {
			// update the balance in memory
			state.openTxns++
			am.balances[accountID] = state
			am.mu.Unlock()
			return &Budget{
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

		balances: make(map[rhpv3.Account]accountState),
	}
}
