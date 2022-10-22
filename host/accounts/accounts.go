package accounts

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/siad/types"
)

type (
	// An AccountID is a unique identifier for an account. It is also an ed25519
	// public key used for verifying signed withdrawals from the account.
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

	// A SettingsReporter reports the host's current settings.
	SettingsReporter interface {
		Settings() (settings.Settings, error)
	}

	// An AccountManager manages deposits and withdrawals for accounts. It is
	// primarily a synchronization wrapper around a store.
	AccountManager struct {
		mu       sync.Mutex // guards all fields in the manager
		settings SettingsReporter
		store    AccountStore

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
	return am.store.Balance(accountID)
}

// Credit adds the specified amount to the account with the given ID.
func (am *AccountManager) Credit(accountID AccountID, amount types.Currency) (types.Currency, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	// get the current balance
	balance, err := am.store.Balance(accountID)
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to get current balance: %w", err)
	}
	settings, err := am.settings.Settings()
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to get settings: %w", err)
	}
	// check that the deposit does not exceed the maximum account balance.
	if balance.Add(amount).Cmp(settings.MaxAccountBalance) > 0 {
		return types.ZeroCurrency, errors.New("deposit exceeds max account balance")
	}

	// credit the account
	balance, err = am.store.Credit(accountID, amount)
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to credit account: %w", err)
	}
	// wake all waiting withdrawals
	close(am.ch)
	am.ch = make(chan struct{})
	return balance, nil
}

// Debit subtracts the specified amount from the account with the given ID and
// returns the remaining balance of the account. This function will block until
// the account has enough funds to cover the debit or until the context is
// cancelled.
func (am *AccountManager) Debit(ctx context.Context, accountID AccountID, amount types.Currency) (types.Currency, error) {
	// instead of monitoring each account, one global deposit channel wakes all
	// waiting withdrawals. Since the account's balance may not have changed,
	// the balance is checked in a loop.
	for {
		am.mu.Lock()
		// check if the current account balance is enough to cover the debit.
		balance, err := am.store.Balance(accountID)
		if err != nil {
			am.mu.Unlock()
			return types.ZeroCurrency, fmt.Errorf("failed to get current balance: %w", err)
		} else if balance.Cmp(amount) >= 0 {
			defer am.mu.Unlock()
			// the current balance is greater than the amount, debit the account
			// and return
			return am.store.Debit(accountID, amount)
		}

		// grab the channel under lock then unlock the mutex so other debits
		// can fire.
		ch := am.ch
		am.mu.Unlock()

		// wait for a deposit or for the context to be cancelled.
		select {
		case <-ctx.Done():
			return types.ZeroCurrency, ctx.Err()
		case <-ch:
			// a deposit has occurred -- recheck the balance.
		}
	}
}

// Refund credits the account even if the deposit would go over the host's
// maximum account balance.
func (am *AccountManager) Refund(accountID AccountID, amount types.Currency) (types.Currency, error) {
	return am.store.Credit(accountID, amount)
}

// NewManager creates a new account manager
func NewManager(store AccountStore, sr SettingsReporter) *AccountManager {
	return &AccountManager{
		ch:       make(chan struct{}),
		store:    store,
		settings: sr,
	}
}
