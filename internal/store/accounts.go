package store

import (
	"errors"
	"sync"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/siad/types"
)

// An EphemeralAccountStore is an in-memory implementation of the account
// store; implements host.AccountStore.
type EphemeralAccountStore struct {
	mu       sync.Mutex
	balances map[accounts.AccountID]types.Currency
}

// Balance returns the balance of the ephemeral account.
func (eas *EphemeralAccountStore) Balance(accountID accounts.AccountID) (types.Currency, error) {
	eas.mu.Lock()
	defer eas.mu.Unlock()
	return eas.balances[accountID], nil
}

// Credit adds the specified amount to the account, returning the current
// balance.
func (eas *EphemeralAccountStore) Credit(accountID accounts.AccountID, amount types.Currency) (types.Currency, error) {
	eas.mu.Lock()
	defer eas.mu.Unlock()
	eas.balances[accountID] = eas.balances[accountID].Add(amount)
	return eas.balances[accountID], nil
}

// Refund returns the amount to the ephemeral account.
func (eas *EphemeralAccountStore) Refund(accountID accounts.AccountID, amount types.Currency) error {
	eas.mu.Lock()
	defer eas.mu.Unlock()

	eas.balances[accountID] = eas.balances[accountID].Add(amount)
	return nil
}

// Debit subtracts the specified amount from the account, returning the current
// balance.
func (eas *EphemeralAccountStore) Debit(accountID accounts.AccountID, amount types.Currency) (types.Currency, error) {
	eas.mu.Lock()
	defer eas.mu.Unlock()

	bal, exists := eas.balances[accountID]
	if !exists || bal.Cmp(amount) < 0 {
		return bal, errors.New("insufficient funds")
	}

	eas.balances[accountID] = eas.balances[accountID].Sub(amount)
	return eas.balances[accountID], nil
}

func (eas *EphemeralAccountStore) Close() error {
	return nil
}

// NewEphemeralAccountStore intializes a new AccountStore.
func NewEphemeralAccountStore() *EphemeralAccountStore {
	return &EphemeralAccountStore{
		balances: make(map[accounts.AccountID]types.Currency),
	}
}
