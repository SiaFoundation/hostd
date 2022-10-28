package accounts

import (
	"errors"
	"fmt"

	"go.sia.tech/siad/types"
)

type (
	// A Budget transactionally manages an account's balance. It is not safe for
	// concurrent use.
	Budget interface {
		Remaining() types.Currency
		Spend(amount types.Currency) error
		Rollback() error
		Commit() error
	}

	budget struct {
		accountID AccountID
		max       types.Currency
		spent     types.Currency
		committed bool
		am        *AccountManager
	}
)

var (
	// ErrInsufficientBudget is returned when the renter's budget is not
	// sufficient to cover the payment.
	ErrInsufficientBudget = errors.New("insufficient budget")
)

// Remaining returns the amount remaining in the budget
func (b *budget) Remaining() types.Currency {
	return b.max.Sub(b.spent)
}

// Spend subtracts amount from the remaining budget. An error is returned if
// their are insufficient funds.
func (b *budget) Spend(amount types.Currency) error {
	spent := b.spent.Add(amount)
	if b.max.Cmp(spent) < 0 {
		return fmt.Errorf("unable to spend %v, %v remaining: %w", amount, b.max.Sub(b.spent), ErrInsufficientBudget)
	}
	b.spent = spent
	return nil
}

// Rollback returns the amount spent back to the account. If the budget has
// already been committed, Rollback is a no-op.
func (b *budget) Rollback() error {
	if b.committed {
		return nil
	}
	b.am.mu.Lock()
	defer b.am.mu.Unlock()

	state, ok := b.am.balances[b.accountID]
	if !ok {
		panic("account missing from memory")
	}
	state.openTxns--
	if state.openTxns == 0 {
		// if there are no more open transactions, we can remove the account
		// from memory without doing anything else
		delete(b.am.balances, b.accountID)
		return nil
	}
	// add the maximum value back to the spendable balance
	state.balance = state.balance.Add(b.max)
	b.am.balances[b.accountID] = state
	return nil
}

// Commit commits the budget's spending to the account. If the budget has
// already been committed, Commit will panic.
func (b *budget) Commit() error {
	if b.committed {
		panic("budget already committed")
	}
	// debit the account
	_, err := b.am.store.Debit(b.accountID, b.spent)
	if err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	// calculate the remainder and zero out the budget
	rem := b.max.Sub(b.spent)
	b.max, b.spent = types.ZeroCurrency, types.ZeroCurrency
	// set the committed flag
	b.committed = true

	b.am.mu.Lock()
	defer b.am.mu.Unlock()
	// update the balance in memory
	state, ok := b.am.balances[b.accountID]
	if !ok {
		panic("account missing from memory")
	}
	state.openTxns--
	if state.openTxns == 0 {
		// if there are no more open transactions, we can remove the account
		// from memory without doing anything else
		delete(b.am.balances, b.accountID)
		return nil
	}
	// add the remaining balance back to the spendable balance
	state.balance = state.balance.Add(b.max.Sub(rem))
	b.am.balances[b.accountID] = state
	return nil
}
