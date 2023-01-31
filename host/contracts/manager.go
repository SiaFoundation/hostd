package contracts

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// ChainManager defines the interface required by the contract manager to
	// interact with the consensus set.
	ChainManager interface {
		IndexAtHeight(height uint64) (consensus.ChainIndex, error)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.UnlockHash
		FundTransaction(txn *types.Transaction, amount types.Currency) (toSign []crypto.Hash, release func(), err error)
		SignTransaction(*types.Transaction, []crypto.Hash, types.CoveredFields) error
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		FeeEstimation() (min types.Currency, max types.Currency)
	}

	// A StorageManager stores and retrieves sectors.
	StorageManager interface {
		// ReadSector reads a sector from the store
		ReadSector(root storage.SectorRoot) ([]byte, error)
	}

	locker struct {
		c       chan struct{}
		waiters int
	}

	// A ContractManager manages contracts' lifecycle
	ContractManager struct {
		store   ContractStore
		storage StorageManager
		chain   ChainManager
		tpool   TransactionPool
		wallet  Wallet

		blockHeight uint64

		// contracts must be locked while they are being modified
		mu    sync.Mutex
		locks map[types.FileContractID]*locker
	}
)

// Lock locks a contract for modification.
func (cm *ContractManager) Lock(id types.FileContractID, timeout time.Duration) (SignedRevision, error) {
	cm.mu.Lock()
	contract, err := cm.store.Contract(id)
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
		contract, err := cm.store.Contract(id)
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

// AddContract stores the provided contract, should error if the contract
// already exists.
func (cm *ContractManager) AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	return cm.store.AddContract(revision, formationSet, lockedCollateral, negotationHeight)
}

// RenewContract renews a contract. It is expected that the existing
// contract will be cleared.
func (cm *ContractManager) RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	if existing.Revision.NewFileMerkleRoot != (crypto.Hash{}) {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.NewFileSize != 0 {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.NewRevisionNumber != math.MaxUint64 {
		return errors.New("existing contract must be cleared")
	}
	return cm.store.RenewContract(renewal, existing, formationSet, lockedCollateral, negotationHeight)
}

// SectorRoots returns the roots of all sectors stored by the contract.
func (cm *ContractManager) SectorRoots(id types.FileContractID, limit, offset uint64) ([]crypto.Hash, error) {
	return cm.store.SectorRoots(id, limit, offset)
}

// ProcessConsensusChange applies a block update to the contract manager.
func (cm *ContractManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	err := cm.store.ContractAction(&cc, cm.handleContractAction)
	if err != nil {
		log.Println("CONTRACTOR ERROR:", err)
	}
	atomic.StoreUint64(&cm.blockHeight, uint64(cc.BlockHeight))
}

func (cm *ContractManager) ReviseContract(contractID types.FileContractID) (*ContractUpdater, error) {
	roots, err := cm.store.SectorRoots(contractID, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get sector roots: %w", err)
	}
	return &ContractUpdater{
		store:       cm.store,
		sectorRoots: roots,
	}, nil
}

// NewManager creates a new contract manager.
func NewManager(store ContractStore, storage StorageManager, chain ChainManager, tpool TransactionPool, wallet Wallet) *ContractManager {
	cm := &ContractManager{
		store:   store,
		storage: storage,
		chain:   chain,
		tpool:   tpool,
		wallet:  wallet,
		locks:   make(map[types.FileContractID]*locker),
	}
	return cm
}
