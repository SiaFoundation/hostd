package accounts

import (
	"fmt"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type (
	// Usage tracks the usage of an account's funds.
	Usage struct {
		RPCRevenue     types.Currency `json:"rpc"`
		StorageRevenue types.Currency `json:"storage"`
		EgressRevenue  types.Currency `json:"egress"`
		IngressRevenue types.Currency `json:"ingress"`
		RegistryRead   types.Currency `json:"registryRead"`
		RegistryWrite  types.Currency `json:"registryWrite"`
	}

	// A Budget transactionally manages an account's balance. It is not safe for
	// concurrent use.
	Budget struct {
		accountID rhpv3.Account
		max       types.Currency
		usage     Usage
		committed bool
		am        *AccountManager
	}
)

// Total returns the total of all revenue types.
func (u Usage) Total() types.Currency {
	return u.RPCRevenue.
		Add(u.StorageRevenue).
		Add(u.EgressRevenue).
		Add(u.IngressRevenue).
		Add(u.RegistryRead).
		Add(u.RegistryWrite)
}

// Add returns the sum of two Usages.
func (a Usage) Add(b Usage) Usage {
	return Usage{
		RPCRevenue:     a.RPCRevenue.Add(b.RPCRevenue),
		StorageRevenue: a.StorageRevenue.Add(b.StorageRevenue),
		EgressRevenue:  a.EgressRevenue.Add(b.EgressRevenue),
		IngressRevenue: a.IngressRevenue.Add(b.IngressRevenue),
		RegistryRead:   a.RegistryRead.Add(b.RegistryRead),
		RegistryWrite:  a.RegistryWrite.Add(b.RegistryWrite),
	}
}

// Sub returns the difference of two Usages.
func (a Usage) Sub(b Usage) Usage {
	return Usage{
		RPCRevenue:     a.RPCRevenue.Sub(b.RPCRevenue),
		StorageRevenue: a.StorageRevenue.Sub(b.StorageRevenue),
		EgressRevenue:  a.EgressRevenue.Sub(b.EgressRevenue),
		IngressRevenue: a.IngressRevenue.Sub(b.IngressRevenue),
		RegistryRead:   a.RegistryRead.Sub(b.RegistryRead),
		RegistryWrite:  a.RegistryWrite.Sub(b.RegistryWrite),
	}
}

// Remaining returns the amount remaining in the budget
func (b *Budget) Remaining() types.Currency {
	return b.max.Sub(b.usage.Total())
}

// Refund returns amount back to the budget. Refund will panic if the budget has
// already been committed or the refund is greater than the amount spent.
func (b *Budget) Refund(usage Usage) {
	if b.committed {
		panic("budget already committed")
	}
	b.usage = b.usage.Sub(usage)
}

// Spend subtracts amount from the remaining budget. An error is returned if
// their are insufficient funds.
func (b *Budget) Spend(usage Usage) error {
	newUsage := b.usage.Add(usage)
	spent := newUsage.Total()
	if b.max.Cmp(spent) < 0 {
		return fmt.Errorf("unable to spend %v, %v remaining: %w", usage.Total(), b.max.Sub(spent), ErrInsufficientFunds)
	}
	b.usage = newUsage
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
	_, err := b.am.store.DebitAccount(b.accountID, b.usage)
	if err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}
	// calculate the remainder and zero out the budget
	spent := b.usage.Total()
	rem := b.max.Sub(spent)
	// zero the budget
	b.max = types.ZeroCurrency
	b.usage = Usage{}
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
