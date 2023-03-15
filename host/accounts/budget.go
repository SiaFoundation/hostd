package accounts

import (
	"fmt"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type (
	// A Budget transactionally manages an account's balance. It is not safe for
	// concurrent use.
	Budget struct {
		accountID rhpv3.Account
		max       types.Currency
		spent     types.Currency
		committed bool
		am        *AccountManager
	}
)

// Remaining returns the amount remaining in the budget
func (b *Budget) Remaining() types.Currency {
	return b.max.Sub(b.spent)
}

// Empty spends all of the remaining budget and returns the amount spent
func (b *Budget) Empty() (spent types.Currency) {
	if b.committed {
		panic("budget already committed")
	}
	spent, b.spent = b.spent, b.max
	return
}

// Refund returns amount back to the budget. Refund will panic if the budget has
// already been committed or the refund is greater than the amount spent.
func (b *Budget) Refund(amount types.Currency) {
	if b.committed {
		panic("budget already committed")
	} else if amount.Cmp(b.spent) > 0 {
		panic("cannot refund more than spent")
	}
	b.spent = b.spent.Sub(amount)
}

// Spend subtracts amount from the remaining budget. An error is returned if
// their are insufficient funds.
func (b *Budget) Spend(amount types.Currency) error {
	spent := b.spent.Add(amount)
	if b.max.Cmp(spent) < 0 {
		return fmt.Errorf("unable to spend %v, %v remaining: %w", amount, b.max.Sub(b.spent), ErrInsufficientFunds)
	}
	b.spent = spent
	return nil
}

// Rollback returns the amount spent back to the account. If the budget has
// already been committed, Rollback is a no-op.
func (b *Budget) Rollback() error {
	b.am.mu.Lock()
	defer b.am.mu.Unlock()

	if b.committed {
		return nil
	}

	state, ok := b.am.balances[b.accountID]
	if !ok {
		panic("account missing from memory")
	}
	b.committed = true
	state.openTxns--
	if state.openTxns <= 0 {
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
func (b *Budget) Commit() error {
	b.am.mu.Lock()
	defer b.am.mu.Unlock()

	if b.committed {
		return nil
	}
	// debit the account
	_, err := b.am.store.DebitAccount(b.accountID, b.spent)
	if err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}
	// calculate the remainder and zero out the budget
	rem := b.max.Sub(b.spent)
	// zero the budget
	b.max, b.spent = types.ZeroCurrency, types.ZeroCurrency
	b.committed = true

	// update the balance in memory
	state, ok := b.am.balances[b.accountID]
	if !ok {
		panic("account missing from memory")
	}
	state.openTxns--
	if state.openTxns <= 0 {
		// if there are no more open transactions, remove the account
		// from memory.
		delete(b.am.balances, b.accountID)
		return nil
	}
	// add the remaining balance back to the spendable balance
	state.balance = state.balance.Add(rem)
	b.am.balances[b.accountID] = state
	return nil
}
