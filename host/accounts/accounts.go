package accounts

import (
	"errors"
	"fmt"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
)

var (
	// ErrInsufficientFunds is returned when an account does not have enough
	// funds to cover a debit.
	ErrInsufficientFunds = errors.New("ephemeral account balance was insufficient") // note: text is required for compatibility with siad
	// ErrBalanceExceeded is returned when an account's balance exceeds the
	// maximum balance.
	ErrBalanceExceeded = errors.New("ephemeral account maximum balance exceeded") // note: text is required for compatibility with siad
)

type (
	// An AccountStore stores and updates account balances.
	AccountStore interface {
		// AccountBalance returns the balance of the account with the given ID.
		AccountBalance(accountID rhpv3.Account) (types.Currency, error)
		// CreditAccount adds the specified amount to the account with the given ID.
		CreditAccount(accountID rhpv3.Account, amount types.Currency, fundingSource types.FileContractID, expiration time.Time) (types.Currency, error)
		// DebitAccount subtracts the specified amount from the account with the given
		// ID. Returns the remaining balance of the account.
		DebitAccount(accountID rhpv3.Account, usage Usage) (types.Currency, error)
	}

	// Settings returns the host's current settings.
	Settings interface {
		Settings() settings.Settings
	}

	accountState struct {
		balance  types.Currency
		openTxns int
	}

	// An AccountManager manages deposits and withdrawals for accounts. It is
	// primarily a synchronization wrapper around a store.
	AccountManager struct {
		store    AccountStore
		settings Settings

		mu sync.Mutex // guards the fields below
		// balances is a map of account IDs to their current balance. It
		// is used for consistency before a budget is synced to the underlying
		// store.
		balances map[rhpv3.Account]accountState
	}
)

func (am *AccountManager) getBalance(accountID rhpv3.Account) (types.Currency, error) {
	if state, ok := am.balances[accountID]; ok {
		return state.balance, nil
	}
	return am.store.AccountBalance(accountID)
}

// Balance returns the balance of the account with the given ID.
func (am *AccountManager) Balance(accountID rhpv3.Account) (types.Currency, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.getBalance(accountID)
}

// Credit adds the specified amount to the account with the given ID. Credits
// are synced to the underlying store immediately.
func (am *AccountManager) Credit(accountID rhpv3.Account, amount types.Currency, fundingSource types.FileContractID, expiration time.Time, refund bool) (types.Currency, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	balance, err := am.getBalance(accountID)
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to get account balance: %w", err)
	}

	creditBalance := balance.Add(amount)
	if !refund && creditBalance.Cmp(am.settings.Settings().MaxAccountBalance) > 0 {
		return types.ZeroCurrency, ErrBalanceExceeded
	}

	// credit the account
	if _, err = am.store.CreditAccount(accountID, amount, fundingSource, expiration); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to credit account: %w", err)
	}
	// increment the balance in memory, if it exists
	if state, ok := am.balances[accountID]; ok {
		state.balance = creditBalance
		am.balances[accountID] = state
	}
	return creditBalance, nil
}

// Budget creates a new budget for an account limited by amount. The spent
// amount will not be synced to the underlying store until Commit is called.
func (am *AccountManager) Budget(accountID rhpv3.Account, amount types.Currency) (*Budget, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	// if there are currently outstanding debits, use the in-memory balance
	state, ok := am.balances[accountID]
	if !ok {
		var err error
		// otherwise, get the balance from the store
		balance, err := am.store.AccountBalance(accountID)
		if err != nil {
			return nil, fmt.Errorf("failed to get account balance: %w", err)
		}

		// add the account to the map of balances
		state = accountState{
			balance: balance,
		}
	}

	// if the account has enough balance, deduct the amount from memory and
	// return a budget
	updated, underflow := state.balance.SubWithUnderflow(amount)
	if underflow {
		return nil, ErrInsufficientFunds
	}

	// deduct the amount from the in-memory state
	state.openTxns++
	state.balance = updated
	am.balances[accountID] = state
	return &Budget{
		accountID: accountID,
		max:       amount,

		am: am,
	}, nil
}

// NewManager creates a new account manager
func NewManager(store AccountStore, settings Settings) *AccountManager {
	return &AccountManager{
		store:    store,
		settings: settings,

		balances: make(map[rhpv3.Account]accountState),
	}
}
