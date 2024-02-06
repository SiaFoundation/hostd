package contracts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

const (
	// sectorRootCacheSize is the number of contracts' sector roots to cache.
	// Caching prevents frequently updated contracts from continuously hitting the
	// DB. This is left as a hard-coded small value to limit memory usage since
	// contracts can contain any number of sector roots
	sectorRootCacheSize = 30
)

type (
	contractChange struct {
		id    types.FileContractID
		index types.ChainIndex
	}

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
		Read(root types.Hash256) (*[rhp2.SectorSize]byte, error)
	}

	// Alerts registers and dismisses global alerts.
	Alerts interface {
		Register(alerts.Alert)
		Dismiss(...types.Hash256)
	}

	locker struct {
		c       chan struct{}
		waiters int
	}

	// A ContractManager manages contracts' lifecycle
	ContractManager struct {
		blockHeight uint64 // ensure 64-bit alignment on 32-bit systems

		store ContractStore
		tg    *threadgroup.ThreadGroup
		log   *zap.Logger

		alerts  Alerts
		storage StorageManager
		chain   ChainManager
		tpool   TransactionPool
		wallet  Wallet

		processQueue chan uint64 // signals that the contract manager should process actions for a given block height

		// caches the sector roots of contracts to avoid hitting the DB
		// for frequently accessed contracts. The cache is limited to a
		// small number of contracts to limit memory usage.
		rootsCache *lru.TwoQueueCache[types.FileContractID, []types.Hash256]

		mu    sync.Mutex                       // guards the following fields
		locks map[types.FileContractID]*locker // contracts must be locked while they are being modified
	}
)

func (cm *ContractManager) getSectorRoots(id types.FileContractID) ([]types.Hash256, error) {
	// check the cache first
	roots, ok := cm.rootsCache.Get(id)
	if !ok {
		var err error
		// if the cache doesn't have the roots, read them from the store
		roots, err = cm.store.SectorRoots(id)
		if err != nil {
			return nil, fmt.Errorf("failed to get sector roots: %w", err)
		}
		// add the roots to the cache
		cm.rootsCache.Add(id, roots)
	}
	// return a deep copy of the roots
	return append([]types.Hash256(nil), roots...), nil
}

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
		cm.mu.Lock()
		defer cm.mu.Unlock()
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

// Contracts returns a paginated list of contracts matching the filter and the
// total number of contracts matching the filter.
func (cm *ContractManager) Contracts(filter ContractFilter) ([]Contract, int, error) {
	return cm.store.Contracts(filter)
}

// Contract returns the contract with the given id.
func (cm *ContractManager) Contract(id types.FileContractID) (Contract, error) {
	return cm.store.Contract(id)
}

// AddContract stores the provided contract, should error if the contract
// already exists.
func (cm *ContractManager) AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()
	if err := cm.store.AddContract(revision, formationSet, lockedCollateral, initialUsage, cm.chain.TipState().Index.Height); err != nil {
		return err
	}
	cm.log.Debug("contract formed", zap.Stringer("contractID", revision.Revision.ParentID))
	return nil
}

// RenewContract renews a contract. It is expected that the existing
// contract will be cleared.
func (cm *ContractManager) RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, initialUsage Usage) error {
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

	if err := cm.store.RenewContract(renewal, existing, formationSet, lockedCollateral, clearingUsage, initialUsage, cm.chain.TipState().Index.Height); err != nil {
		return err
	}
	cm.log.Debug("contract renewed", zap.Stringer("renewalID", renewal.Revision.ParentID), zap.Stringer("existingID", existing.Revision.ParentID))
	return nil
}

// SectorRoots returns the roots of all sectors stored by the contract.
func (cm *ContractManager) SectorRoots(id types.FileContractID) ([]types.Hash256, error) {
	done, err := cm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	return cm.getSectorRoots(id)
}

// ScanHeight returns the height of the last block processed by the contract
func (cm *ContractManager) ScanHeight() uint64 {
	return atomic.LoadUint64(&cm.blockHeight)
}

// ProcessConsensusChange applies a block update to the contract manager.
func (cm *ContractManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	done, err := cm.tg.Add()
	if err != nil {
		return
	}
	defer done()
	log := cm.log.Named("consensusChange")

	// calculate the block height of the first reverted diff
	blockHeight := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + uint64(len(cc.RevertedBlocks)) + 1
	var revertedFormations, revertedResolutions []contractChange
	revertedRevisions := make(map[types.FileContractID]contractChange)
	for _, reverted := range cc.RevertedBlocks {
		index := types.ChainIndex{
			Height: blockHeight,
			ID:     types.BlockID(reverted.ID()),
		}
		for _, transaction := range reverted.Transactions {
			for i := range transaction.FileContracts {
				contractID := types.FileContractID(transaction.FileContractID(uint64(i)))
				revertedFormations = append(revertedFormations, contractChange{contractID, index})
			}

			for _, rev := range transaction.FileContractRevisions {
				contractID := types.FileContractID(rev.ParentID)
				revertedRevisions[contractID] = contractChange{types.FileContractID(rev.ParentID), index} // TODO: revert to the previous revision number, instead of setting to 0
			}

			for _, proof := range transaction.StorageProofs {
				contractID := types.FileContractID(proof.ParentID)
				revertedResolutions = append(revertedResolutions, contractChange{contractID, index})
			}
		}
		blockHeight--
	}

	var appliedFormations, appliedResolutions []contractChange
	appliedRevisions := make(map[types.FileContractID]types.FileContractRevision)
	for _, applied := range cc.AppliedBlocks {
		index := types.ChainIndex{
			Height: blockHeight,
			ID:     types.BlockID(applied.ID()),
		}
		for _, transaction := range applied.Transactions {
			for i := range transaction.FileContracts {
				contractID := types.FileContractID(transaction.FileContractID(uint64(i)))
				appliedFormations = append(appliedFormations, contractChange{contractID, index})
			}

			for _, rev := range transaction.FileContractRevisions {
				contractID := types.FileContractID(rev.ParentID)
				var revision types.FileContractRevision
				convertToCore(rev, &revision)
				appliedRevisions[contractID] = revision
			}

			for _, proof := range transaction.StorageProofs {
				contractID := types.FileContractID(proof.ParentID)
				appliedResolutions = append(appliedResolutions, contractChange{contractID, index})
			}
		}
		blockHeight++
	}

	err = cm.store.UpdateContractState(cc.ID, uint64(cc.BlockHeight), func(tx UpdateStateTransaction) error {
		for _, reverted := range revertedFormations {
			if relevant, err := tx.ContractRelevant(reverted.id); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertFormation(reverted.id); err != nil {
				return fmt.Errorf("failed to revert formation: %w", err)
			}

			log.Warn("contract formation reverted", zap.Stringer("contractID", reverted.id), zap.Stringer("block", reverted.index))
			cm.alerts.Register(alerts.Alert{
				ID:       types.Hash256(reverted.id),
				Severity: alerts.SeverityWarning,
				Message:  "Contract formation reverted",
				Data: map[string]any{
					"contractID": reverted.id,
					"index":      reverted.index,
				},
				Timestamp: time.Now(),
			})
		}

		for _, reverted := range revertedRevisions {
			if relevant, err := tx.ContractRelevant(reverted.id); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertRevision(reverted.id); err != nil {
				return fmt.Errorf("failed to revert revision: %w", err)
			}

			log.Warn("contract revision reverted", zap.Stringer("contractID", reverted.id), zap.Stringer("block", reverted.index))
			cm.alerts.Register(alerts.Alert{
				ID:       types.Hash256(reverted.id),
				Severity: alerts.SeverityWarning,
				Message:  "Contract revision reverted",
				Data: map[string]any{
					"contractID": reverted.id,
					"index":      reverted.index,
				},
				Timestamp: time.Now(),
			})
		}

		for _, reverted := range revertedResolutions {
			if relevant, err := tx.ContractRelevant(reverted.id); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", reverted.id, err)
			} else if !relevant {
				continue
			} else if err := tx.RevertResolution(reverted.id); err != nil {
				return fmt.Errorf("failed to revert proof: %w", err)
			}

			log.Warn("contract resolution reverted", zap.Stringer("contractID", reverted.id), zap.Stringer("block", reverted.index))
			cm.alerts.Register(alerts.Alert{
				ID:       types.Hash256(reverted.id),
				Severity: alerts.SeverityWarning,
				Message:  "Contract resolution reverted",
				Data: map[string]any{
					"contractID": reverted.id,
					"index":      reverted.index,
				},
				Timestamp: time.Now(),
			})
		}

		for _, applied := range appliedFormations {
			if relevant, err := tx.ContractRelevant(applied.id); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied.id, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmFormation(applied.id); err != nil {
				return fmt.Errorf("failed to apply formation: %w", err)
			}

			log.Info("contract formation confirmed", zap.Stringer("contractID", applied.id), zap.Stringer("block", applied.index))
			cm.alerts.Dismiss(types.Hash256(applied.id)) // dismiss any lifecycle alerts for this contract
		}

		for _, applied := range appliedRevisions {
			if relevant, err := tx.ContractRelevant(applied.ParentID); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied.ParentID, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmRevision(applied); err != nil {
				return fmt.Errorf("failed to apply revision: %w", err)
			}

			log.Info("contract revision confirmed", zap.Stringer("contractID", applied.ParentID), zap.Uint64("revisionNumber", applied.RevisionNumber))
			cm.alerts.Dismiss(types.Hash256(applied.ParentID)) // dismiss any lifecycle alerts for this contract
		}

		for _, applied := range appliedResolutions {
			if relevant, err := tx.ContractRelevant(applied.id); err != nil {
				return fmt.Errorf("failed to check if contract %v is relevant: %w", applied, err)
			} else if !relevant {
				continue
			} else if err := tx.ConfirmResolution(applied.id, applied.index.Height); err != nil {
				return fmt.Errorf("failed to apply proof: %w", err)
			}

			log.Info("contract resolution confirmed", zap.Stringer("contractID", applied.id), zap.Stringer("block", applied.index))
			cm.alerts.Dismiss(types.Hash256(applied.id)) // dismiss any lifecycle alerts for this contract
		}
		return nil
	})
	if err != nil {
		log.Error("failed to process consensus change", zap.Error(err))
		return
	}

	scanHeight := uint64(cc.BlockHeight)
	log.Debug("consensus change applied", zap.Uint64("height", scanHeight), zap.String("changeID", cc.ID.String()))

	// if the last block is more than 3 days old, skip action processing until
	// consensus is caught up
	blockTime := time.Unix(int64(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].Timestamp), 0)
	if time.Since(blockTime) > 72*time.Hour {
		return
	}

	// perform actions in a separate goroutine to avoid deadlock in tpool.
	// triggers the processActions goroutine to process the block
	go func() {
		cm.processQueue <- uint64(cc.BlockHeight)
	}()
}

// ReviseContract initializes a new contract updater for the given contract.
func (cm *ContractManager) ReviseContract(contractID types.FileContractID) (*ContractUpdater, error) {
	done, err := cm.tg.Add()
	if err != nil {
		return nil, err
	}

	roots, err := cm.getSectorRoots(contractID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sector roots: %w", err)
	}
	return &ContractUpdater{
		store: cm.store,
		log:   cm.log.Named("contractUpdater"),

		rootsCache:  cm.rootsCache,
		contractID:  contractID,
		sectorRoots: roots, // roots is already a deep copy
		oldRoots:    append([]types.Hash256(nil), roots...),

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
func NewManager(store ContractStore, alerts Alerts, storage StorageManager, c ChainManager, tpool TransactionPool, wallet Wallet, log *zap.Logger) (*ContractManager, error) {
	cache, err := lru.New2Q[types.FileContractID, []types.Hash256](sectorRootCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}
	cm := &ContractManager{
		store:   store,
		tg:      threadgroup.New(),
		log:     log,
		alerts:  alerts,
		storage: storage,
		chain:   c,
		tpool:   tpool,
		wallet:  wallet,

		rootsCache: cache,

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
		if errors.Is(err, chain.ErrInvalidChangeID) {
			cm.log.Warn("rescanning blockchain due to unknown consensus change ID")
			if err := cm.chain.Subscribe(cm, modules.ConsensusChangeBeginning, cm.tg.Done()); err != nil {
				cm.log.Fatal("failed to reset consensus change subscription", zap.Error(err))
			}
		} else if err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
			cm.log.Fatal("failed to subscribe to consensus changes", zap.Error(err))
		}
	}()

	return cm, nil
}
