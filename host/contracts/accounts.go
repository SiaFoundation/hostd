package contracts

import (
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// AccountBalance returns the balance of an account.
func (cm *Manager) AccountBalance(account proto4.Account) (types.Currency, error) {
	return cm.store.RHP4AccountBalance(account)
}

// CreditAccountsWithContract atomically revises a contract and credits the accounts
// returning the new balance of each account.
func (cm *Manager) CreditAccountsWithContract(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract, usage proto4.Usage) ([]types.Currency, error) {
	return cm.store.RHP4CreditAccounts(deposits, contractID, revision, usage)
}

// DebitAccount debits an account.
func (cm *Manager) DebitAccount(account proto4.Account, usage proto4.Usage) error {
	return cm.store.RHP4DebitAccount(account, usage)
}

// AccountBalances returns the balances of multiple accounts. Balances is returned
// in the same order as the input accounts. If an account does not exist, the balance
// at that index will be types.ZeroCurrency.
func (cm *Manager) AccountBalances(accounts []proto4.Account) ([]types.Currency, error) {
	return cm.store.RHP4AccountBalances(accounts)
}
