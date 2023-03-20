package contracts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"gitlab.com/NebulousLabs/encoding"
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
		TipState() consensus.State
		IndexAtHeight(height uint64) (types.ChainIndex, error)
		Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		UnlockConditions() types.UnlockConditions
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

		processQueue chan uint64 // signals that the contract manager should process actions for a given block height

		mu    sync.Mutex                       // guards the following fields
		locks map[types.FileContractID]*locker // contracts must be locked while they are being modified
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
		cm.mu.Unlock()
		return SignedRevision{}, fmt.Errorf("failed to get contract: %w", err)
	} else if err := isGoodForModification(contract, cm.chain.TipState().Index.Height); err != nil {
		cm.mu.Unlock()
		return SignedRevision{}, fmt.Errorf("contract is not good for modification: %w", err)
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
		} else if err := isGoodForModification(contract, cm.chain.TipState().Index.Height); err != nil {
			return SignedRevision{}, fmt.Errorf("contract is not good for modification: %w", err)
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

// Contracts returns a paginated list of contracts sorted by expiration
// ascending.
func (cm *ContractManager) Contracts(filter ContractFilter) ([]Contract, error) {
	return cm.store.Contracts(filter)
}

// Contract returns the contract with the given id.
func (cm *ContractManager) Contract(id types.FileContractID) (Contract, error) {
	return cm.store.Contract(id)
}

// AddContract stores the provided contract, should error if the contract
// already exists.
func (cm *ContractManager) AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage, negotationHeight uint64) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()
	return cm.store.AddContract(revision, formationSet, lockedCollateral, initialUsage, negotationHeight)
}

// RenewContract renews a contract. It is expected that the existing
// contract will be cleared.
func (cm *ContractManager) RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage, negotationHeight uint64) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// sanity checks
	if existing.Revision.FileMerkleRoot != (types.Hash256{}) {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.Filesize != 0 {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.RevisionNumber != types.MaxRevisionNumber {
		return errors.New("existing contract must be cleared")
	}
	return cm.store.RenewContract(renewal, existing, formationSet, lockedCollateral, initialUsage, negotationHeight)
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
	log := cm.log.Named("consensusChange")

	var revertedFormations, revertedResolutions []types.FileContractID
	revertedRevisions := make(map[types.FileContractID]struct{})
	for _, reverted := range cc.RevertedBlocks {
		for _, transaction := range reverted.Transactions {
			for i := range transaction.FileContracts {
				contractID := types.FileContractID(transaction.FileContractID(uint64(i)))
				revertedFormations = append(revertedFormations, contractID)
			}

			for _, rev := range transaction.FileContractRevisions {
				contractID := types.FileContractID(rev.ParentID)
				revertedRevisions[contractID] = struct{}{} // TODO: revert to the previous revision number, instead of setting to 0
			}

			for _, proof := range transaction.StorageProofs {
				contractID := types.FileContractID(proof.ParentID)
				revertedResolutions = append(revertedResolutions, contractID)
			}
		}
	}

	var appliedFormations, appliedResolutions []types.FileContractID
	appliedRevisions := make(map[types.FileContractID]types.FileContractRevision)
	for _, applied := range cc.AppliedBlocks {
		for _, transaction := range applied.Transactions {
			for i := range transaction.FileContracts {
				contractID := types.FileContractID(transaction.FileContractID(uint64(i)))
				appliedFormations = append(appliedFormations, contractID)
			}

			for _, rev := range transaction.FileContractRevisions {
				contractID := types.FileContractID(rev.ParentID)
				var revision types.FileContractRevision
				convertToCore(rev, &revision)
				appliedRevisions[contractID] = revision
			}

			for _, proof := range transaction.StorageProofs {
				contractID := types.FileContractID(proof.ParentID)
				appliedResolutions = append(appliedResolutions, contractID)
			}
		}
	}

	err = cm.store.UpdateContractState(cc.ID, uint64(cc.BlockHeight), func(tx UpdateStateTransaction) error {
		for _, reverted := range revertedFormations {
			if relevant, err := tx.ContractRelevant(reverted); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertFormation(reverted); err != nil {
				return fmt.Errorf("failed to revert formation: %w", err)
			} else if err := tx.SetStatus(reverted, ContractStatusPending); err != nil {
				return fmt.Errorf("failed to set status: %w", err)
			}
			log.Debug("contract formation reverted", zap.String("contract", reverted.String()))
		}

		for reverted := range revertedRevisions {
			if relevant, err := tx.ContractRelevant(reverted); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertRevision(reverted); err != nil {
				return fmt.Errorf("failed to revert revision: %w", err)
			}
			log.Debug("contract revision reverted", zap.String("contract", reverted.String()))
		}

		for _, reverted := range revertedResolutions {
			if relevant, err := tx.ContractRelevant(reverted); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertResolution(reverted); err != nil {
				return fmt.Errorf("failed to revert proof: %w", err)
			} else if err := tx.SetStatus(reverted, ContractStatusActive); err != nil {
				return fmt.Errorf("failed to set status: %w", err)
			}
			log.Debug("contract resolution reverted", zap.String("contract", reverted.String()))
		}

		for _, applied := range appliedFormations {
			if relevant, err := tx.ContractRelevant(applied); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmFormation(applied); err != nil {
				return fmt.Errorf("failed to apply formation: %w", err)
			} else if err := tx.SetStatus(applied, ContractStatusActive); err != nil {
				return fmt.Errorf("failed to set status: %w", err)
			}
			log.Debug("contract formation applied", zap.String("contract", applied.String()))
		}

		for _, applied := range appliedRevisions {
			if relevant, err := tx.ContractRelevant(applied.ParentID); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied.ParentID, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmRevision(applied); err != nil {
				return fmt.Errorf("failed to apply revision: %w", err)
			}
			log.Debug("contract revision applied", zap.String("contract", applied.ParentID.String()), zap.Uint64("revisionNumber", applied.RevisionNumber))
		}

		for _, applied := range appliedResolutions {
			if relevant, err := tx.ContractRelevant(applied); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmResolution(applied); err != nil {
				return fmt.Errorf("failed to apply proof: %w", err)
			} else if err := tx.SetStatus(applied, ContractStatusSuccessful); err != nil {
				return fmt.Errorf("failed to set status: %w", err)
			}
			log.Debug("contract resolution applied", zap.String("contract", applied.String()))
		}

		return nil
	})
	if err != nil {
		log.Error("failed to process consensus change", zap.Error(err))
		return
	}

	scanHeight := uint64(cc.BlockHeight)
	atomic.StoreUint64(&cm.blockHeight, scanHeight)
	log.Debug("consensus change applied", zap.Uint64("height", scanHeight), zap.String("changeID", cc.ID.String()))

	// perform actions in a separate goroutine to avoid deadlock in tpool
	// triggers the processActions goroutine to process the block
	cm.processQueue <- scanHeight
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
		store: cm.store,
		log:   cm.log.Named("contractUpdater"),

		contractID:  contractID,
		sectorRoots: roots,

		done: done, // decrements the threadgroup counter after the updater is closed
	}, nil
}

// Close closes the contract manager.
func (cm *ContractManager) Close() error {
	cm.tg.Stop()
	return nil
}

// isGoodForModification validates if a contract can be modified
func isGoodForModification(contract Contract, height uint64) error {
	switch {
	case contract.Status != ContractStatusActive && contract.Status != ContractStatusPending:
		return fmt.Errorf("contract status is %v, contract cannot be used", contract.Status)
	case (height + RevisionSubmissionBuffer) > contract.Revision.WindowStart:
		return fmt.Errorf("contract is too close to the proof window to be revised (%v > %v)", height+RevisionSubmissionBuffer, contract.Revision.WindowStart)
	case contract.Revision.RevisionNumber == math.MaxUint64:
		return fmt.Errorf("contract has reached the maximum number of revisions")
	}
	return nil
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// NewManager creates a new contract manager.
func NewManager(store ContractStore, storage StorageManager, chain ChainManager, tpool TransactionPool, wallet Wallet, log *zap.Logger) (*ContractManager, error) {
	cm := &ContractManager{
		store: store,
		tg:    threadgroup.New(),
		log:   log,

		storage: storage,
		chain:   chain,
		tpool:   tpool,
		wallet:  wallet,

		processQueue: make(chan uint64, 100),
		locks:        make(map[types.FileContractID]*locker),
	}

	changeID, err := store.LastContractChange()
	if err != nil {
		return nil, fmt.Errorf("failed to get last contract change: %w", err)
	}

	// start the actions queue. Required to avoid a deadlock in the tpool, but
	// still process consensus changes serially.
	go cm.processActions()

	// subscribe to the consensus set in a separate goroutine to prevent
	// blocking startup
	go func() {
		err := cm.chain.Subscribe(cm, changeID, cm.tg.Done())
		if err != nil {
			cm.log.Error("failed to subscribe to consensus set", zap.Error(err))
		}
	}()

	return cm, nil
}
