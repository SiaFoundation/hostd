package contracts

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// Consensus defines the interface required by the contract manager to
	// interact with the consensus set.
	Consensus interface {
		BlockByID(types.BlockID) (types.Block, types.BlockHeight, bool)
		BlockAtHeight(types.BlockHeight) (types.Block, bool)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.UnlockHash
		FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.SiacoinOutputID, func(), error)
		SignTransaction(*types.Transaction, []types.SiacoinOutputID) error
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		FeeEstimate() (min types.Currency, max types.Currency)
	}

	// A StorageManager stores and retrieves sectors.
	StorageManager interface {
		HasSector(crypto.Hash) (bool, error)
		// AddSector adds a sector to the storage manager.
		AddSector(root crypto.Hash, sector []byte, refs int) error
		// DeleteSector deletes the sector with the given root.
		DeleteSector(root crypto.Hash, refs int) error
		// Sector reads a sector from the store
		Sector(root crypto.Hash) ([]byte, error)
	}

	// A ContractStore stores contracts for the host. It also updates stored
	// contracts from the blockchain and determines which contracts need
	// lifecycle actions.
	ContractStore interface {
		// Exists returns true if the contract is in the store.
		Exists(types.FileContractID) bool
		// Get returns the contract with the given ID.
		Get(types.FileContractID) (Contract, error)
		// Add stores the provided contract, should error if the contract
		// already exists in the store.
		Add(SignedRevision, []types.Transaction) error
		// Delete removes the contract with the given ID from the store.
		Delete(types.FileContractID) error
		// ReviseContract updates the current revision associated with a contract.
		Revise(revision types.FileContractRevision, hostSig []byte, renterSig []byte) error

		// Roots returns the roots of all sectors stored by the contract.
		Roots(types.FileContractID) ([]crypto.Hash, error)
		// SetRoots sets the stored roots of the contract.
		SetRoots(types.FileContractID, []crypto.Hash) error

		// ContractAction calls contractFn on every contract in the store that
		// needs a lifecycle action performed.
		ContractAction(cc *modules.ConsensusChange, contractFn func(types.FileContractID)) error

		Close() error
	}

	locker struct {
		c       chan struct{}
		waiters int
	}

	// A ContractManager manages contracts' lifecycle
	ContractManager struct {
		store     ContractStore
		storage   StorageManager
		consensus Consensus
		tpool     TransactionPool
		wallet    Wallet

		blockHeight uint64

		// contracts must be locked while they are being modified
		mu    sync.Mutex
		locks map[types.FileContractID]*locker
	}
)

// Close closes the contract manager.
func (cm *ContractManager) Close() error {
	return cm.store.Close()
}

// Lock locks a contract for modification.
func (cm *ContractManager) Lock(id types.FileContractID, timeout time.Duration) (SignedRevision, error) {
	cm.mu.Lock()
	contract, err := cm.store.Get(id)
	if err != nil {
		return SignedRevision{}, err
	}

	// if the contract isn't already locked, create a new lock
	if _, exists := cm.locks[id]; !exists {
		cm.locks[id] = &locker{
			c:       make(chan struct{}, 1),
			waiters: 0,
		}
		cm.mu.Unlock()
		return contract.SignedRevision, nil
	}
	cm.locks[id].waiters++
	c := cm.locks[id].c
	// mutex must be unlocked before waiting on the channel to prevent deadlock.
	cm.mu.Unlock()
	select {
	case <-c:
		contract, err := cm.store.Get(id)
		if err != nil {
			return SignedRevision{}, fmt.Errorf("failed to get contract: %w", err)
		}
		return contract.SignedRevision, nil
	case <-time.After(timeout):
		return SignedRevision{}, errors.New("contract lock timeout")
	}
}

// Unlock unlocks a locked contract.
func (cm *ContractManager) Unlock(id types.FileContractID) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	lock, exists := cm.locks[id]
	if !exists {
		return
	} else if lock.waiters <= 0 {
		delete(cm.locks, id)
		return
	}
	lock.waiters--
	lock.c <- struct{}{}
}

// AddContract stores the provided contract, overwriting any previous contract
// with the same ID.
func (cm *ContractManager) AddContract(contract SignedRevision, formationTxnSet []types.Transaction) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.store.Add(contract, formationTxnSet)
}

// ReviseContract updates the current revision associated with a contract.
func (cm *ContractManager) ReviseContract(revision types.FileContractRevision, hostSig []byte, renterSig []byte) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.store.Revise(revision, hostSig, renterSig)
}

// SectorRoots returns the roots of all sectors stored by the contract.
func (cm *ContractManager) SectorRoots(id types.FileContractID) ([]crypto.Hash, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.store.Roots(id)
}

// SetRoots updates the roots of the contract.
func (cm *ContractManager) SetRoots(id types.FileContractID, roots []crypto.Hash) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.store.SetRoots(id, roots)
}

// ProcessConsensusChange applies a block update to the contract manager.
func (cm *ContractManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.store.ContractAction(&cc, func(id types.FileContractID) {
		_ = cm.handleContractAction(uint64(cc.BlockHeight), id)
		// TODO: log error
	})
	atomic.StoreUint64(&cm.blockHeight, uint64(cc.BlockHeight))
}

// NewManager creates a new contract manager.
func NewManager(store ContractStore, storage StorageManager, consensus Consensus, tpool TransactionPool, wallet Wallet) *ContractManager {
	cm := &ContractManager{
		store:     store,
		storage:   storage,
		consensus: consensus,
		tpool:     tpool,
		wallet:    wallet,
		locks:     make(map[types.FileContractID]*locker),
	}
	return cm
}
