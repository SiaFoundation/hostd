package contracts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

type (
	// ChainManager defines the interface required by the contract manager to
	// interact with the consensus set.
	ChainManager interface {
		IndexAtHeight(height uint64) (types.ChainIndex, error)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		FundTransaction(txn *types.Transaction, amount types.Currency) (toSign []types.Hash256, release func(), err error)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		RecommendedFee() types.Currency
	}

	// A StorageManager stores and retrieves sectors.
	StorageManager interface {
		// Read reads a sector from the store
		Read(root types.Hash256) (*[rhpv2.SectorSize]byte, error)
	}

	locker struct {
		c       chan struct{}
		waiters int
	}

	// A ContractManager manages contracts' lifecycle
	ContractManager struct {
		store ContractStore
		tg    *threadgroup.ThreadGroup
		log   *zap.Logger

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
func (cm *ContractManager) Lock(ctx context.Context, id types.FileContractID) (SignedRevision, error) {
	ctx, cancel, err := cm.tg.AddContext(ctx)
	if err != nil {
		return SignedRevision{}, err
	}
	defer cancel()

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
	case <-ctx.Done():
		return SignedRevision{}, ctx.Err()
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
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()
	return cm.store.AddContract(revision, formationSet, lockedCollateral, negotationHeight)
}

// RenewContract renews a contract. It is expected that the existing
// contract will be cleared.
func (cm *ContractManager) RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	if existing.Revision.FileMerkleRoot != (types.Hash256{}) {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.Filesize != 0 {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.RevisionNumber != types.MaxRevisionNumber {
		return errors.New("existing contract must be cleared")
	}
	return cm.store.RenewContract(renewal, existing, formationSet, lockedCollateral, negotationHeight)
}

// SectorRoots returns the roots of all sectors stored by the contract.
func (cm *ContractManager) SectorRoots(id types.FileContractID, limit, offset uint64) ([]types.Hash256, error) {
	done, err := cm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	return cm.store.SectorRoots(id, limit, offset)
}

// ProcessConsensusChange applies a block update to the contract manager.
func (cm *ContractManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	done, err := cm.tg.Add()
	if err != nil {
		return
	}
	defer done()

	cm.mu.Lock()
	defer cm.mu.Unlock()
	err = cm.store.ContractAction(&cc, cm.handleContractAction)
	if err != nil {
		cm.log.Error("failed to process consensus change", zap.Error(err))
		return
	}
	atomic.StoreUint64(&cm.blockHeight, uint64(cc.BlockHeight))
}

// ReviseContract initializes a new contract updater for the given contract.
func (cm *ContractManager) ReviseContract(contractID types.FileContractID) (*ContractUpdater, error) {
	done, err := cm.tg.Add()
	if err != nil {
		return nil, err
	}

	roots, err := cm.store.SectorRoots(contractID, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get sector roots: %w", err)
	}
	return &ContractUpdater{
		store:       cm.store,
		sectorRoots: roots,

		done: done, // decrements the threadgroup counter after the updater is closed
	}, nil
}

// Close closes the contract manager.
func (cm *ContractManager) Close() error {
	cm.tg.Stop()
	return nil
}

// NewManager creates a new contract manager.
func NewManager(store ContractStore, storage StorageManager, chain ChainManager, tpool TransactionPool, wallet Wallet, log *zap.Logger) *ContractManager {
	cm := &ContractManager{
		store: store,
		tg:    threadgroup.New(),
		log:   log,

		storage: storage,
		chain:   chain,
		tpool:   tpool,
		wallet:  wallet,

		locks: make(map[types.FileContractID]*locker),
	}
	return cm
}
