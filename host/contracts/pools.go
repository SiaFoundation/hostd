package contracts

import (
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// PoolBalances returns the balances of multiple pools.
func (cm *Manager) PoolBalances(pools []proto4.Account) ([]types.Currency, error) {
	return cm.store.RHP4PoolBalances(pools)
}

// CreditPoolsWithContract atomically revises a contract and credits the pools,
// returning the new balance of each pool.
func (cm *Manager) CreditPoolsWithContract(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract, usage proto4.Usage) ([]types.Currency, error) {
	return cm.store.RHP4CreditPools(deposits, contractID, revision, usage)
}

// AttachPools attaches accounts to pools.
func (cm *Manager) AttachPools(attachments []proto4.PoolAttachment) error {
	return cm.store.RHP4AttachPools(attachments)
}

// DetachPools detaches accounts from pools.
func (cm *Manager) DetachPools(detachments []proto4.PoolDetachment) error {
	return cm.store.RHP4DetachPools(detachments)
}
