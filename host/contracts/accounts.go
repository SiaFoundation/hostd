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
	return cm.store.RHP4CreditAccounts(deposits, contractID, revision, V2Usage{
		RPCRevenue:       usage.RPC,
		StorageRevenue:   usage.Storage,
		IngressRevenue:   usage.Ingress,
		EgressRevenue:    usage.Egress,
		AccountFunding:   usage.AccountFunding,
		RiskedCollateral: usage.RiskedCollateral,
	})
}

// DebitAccount debits an account.
func (cm *Manager) DebitAccount(account proto4.Account, usage proto4.Usage) error {
	return cm.store.RHP4DebitAccount(account, V2Usage{
		RPCRevenue:       usage.RPC,
		StorageRevenue:   usage.Storage,
		IngressRevenue:   usage.Ingress,
		EgressRevenue:    usage.Egress,
		AccountFunding:   usage.AccountFunding,
		RiskedCollateral: usage.RiskedCollateral,
	})
}
