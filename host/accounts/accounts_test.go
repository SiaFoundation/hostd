package accounts

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// An EphemeralAccountStore is an in-memory implementation of the account store;
// implements host.AccountStore.
type EphemeralAccountStore struct {
	mu       sync.Mutex
	balances map[AccountID]types.Currency
}

// Balance returns the balance of the ephemeral account.
func (eas *EphemeralAccountStore) Balance(accountID AccountID) (types.Currency, error) {
	eas.mu.Lock()
	defer eas.mu.Unlock()
	return eas.balances[accountID], nil
}

// Credit adds the specified amount to the account, returning the current
// balance.
func (eas *EphemeralAccountStore) Credit(accountID AccountID, amount types.Currency) (types.Currency, error) {
	eas.mu.Lock()
	defer eas.mu.Unlock()
	eas.balances[accountID] = eas.balances[accountID].Add(amount)
	return eas.balances[accountID], nil
}

// Refund returns the amount to the ephemeral account.
func (eas *EphemeralAccountStore) Refund(accountID AccountID, amount types.Currency) error {
	eas.mu.Lock()
	defer eas.mu.Unlock()

	eas.balances[accountID] = eas.balances[accountID].Add(amount)
	return nil
}

// Debit subtracts the specified amount from the account, returning the current
// balance.
func (eas *EphemeralAccountStore) Debit(accountID AccountID, amount types.Currency) (types.Currency, error) {
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
		balances: make(map[AccountID]types.Currency),
	}
}

func TestCredit(t *testing.T) {
	am := NewManager(NewEphemeralAccountStore())
	accountID := frand.Entropy256()

	// attempt to credit the account
	amount := types.NewCurrency64(50)
	if _, err := am.Credit(accountID, amount); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := am.store.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(amount) != 0 {
		t.Fatal("expected balance to be equal to amount", balance, amount)
	}
}

func TestBudget(t *testing.T) {
	am := NewManager(NewEphemeralAccountStore())
	accountID := frand.Entropy256()

	// credit the account
	amount := types.NewCurrency64(50)
	if _, err := am.Credit(accountID, amount); err != nil {
		t.Fatal("expected successful credit", err)
	}

	expectedBalance := amount

	// initialize a new budget for half the account balance
	budget, err := am.Budget(context.Background(), accountID, amount.Div64(2))
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

	// check that the in-memory state is consistent
	budgetAmount := amount.Div64(2)
	expectedBalance = expectedBalance.Sub(budgetAmount)
	if am.balances[accountID].balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected in-memory balance to be %v, got %v", expectedBalance, am.balances[accountID].balance)
	}

	// spend half of the budget
	spendAmount := amount.Div64(4)
	if err := budget.Spend(spendAmount); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state did not change
	if am.balances[accountID].balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected in-memory balance to be %v, got %v", expectedBalance, am.balances[accountID].balance)
	}

	// commit the budget
	if err := budget.Commit(); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state has been cleared
	if _, exists := am.balances[accountID]; exists {
		t.Fatal("expected in-memory balance to be cleared")
	}

	// check that the account balance has been updated and only the spent
	// amount has been deducted
	expectedBalance = expectedBalance.Add(budgetAmount.Sub(spendAmount))
	if balance, err := am.store.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected balance to be equal to %v, got %v", expectedBalance, balance)
	}
}
