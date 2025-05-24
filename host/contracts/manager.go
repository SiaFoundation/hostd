package contracts

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	"go.uber.org/zap"
)

type (
	// ChainManager defines the interface required by the contract manager to
	// interact with the consensus set.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State
		BestIndex(height uint64) (types.ChainIndex, bool)
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		AddPoolTransactions([]types.Transaction) (known bool, err error)

		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
		RecommendedFee() types.Currency
	}

	// A Syncer broadcasts transactions to its peers
	Syncer interface {
		BroadcastTransactionSet([]types.Transaction) error
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction) error
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		UnlockConditions() types.UnlockConditions
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) error
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}

	// A StorageManager stores and retrieves sectors.
	StorageManager interface {
		// Read reads a sector from the store
		ReadSector(root types.Hash256) (*[rhp2.SectorSize]byte, error)
	}

	// Alerts registers and dismisses global alerts.
	Alerts interface {
		Register(alerts.Alert)
		Dismiss(...types.Hash256)
	}

	// A Manager manages contracts' lifecycle
	Manager struct {
		rejectBuffer             uint64
		revisionSubmissionBuffer uint64

		store ContractStore
		tg    *threadgroup.ThreadGroup
		log   *zap.Logger

		alerts  Alerts
		storage StorageManager
		chain   ChainManager
		syncer  Syncer
		wallet  Wallet

		locks *locker // contracts must be locked while they are being modified

		mu sync.Mutex // guards the following fields
		// caches the sector roots of all contracts to avoid long reads from
		// the store
		sectorRoots map[types.FileContractID][]types.Hash256
	}
)

func (cm *Manager) getSectorRoots(id types.FileContractID) []types.Hash256 {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	roots, ok := cm.sectorRoots[id]
	if !ok {
		return nil
	}
	// return a deep copy of the roots
	return append([]types.Hash256(nil), roots...)
}

func (cm *Manager) setSectorRoots(id types.FileContractID, roots []types.Hash256) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// deep copy the roots
	cm.sectorRoots[id] = append([]types.Hash256(nil), roots...)
}

// Contracts returns a paginated list of contracts matching the filter and the
// total number of contracts matching the filter.
func (cm *Manager) Contracts(filter ContractFilter) ([]Contract, int, error) {
	return cm.store.Contracts(filter)
}

// Contract returns the contract with the given id.
func (cm *Manager) Contract(id types.FileContractID) (Contract, error) {
	return cm.store.Contract(id)
}

// V2Contracts returns a paginated list of contracts matching the filter and the
// total number of contracts matching the filter.
func (cm *Manager) V2Contracts(filter V2ContractFilter) ([]V2Contract, int, error) {
	return cm.store.V2Contracts(filter)
}

// V2Contract returns the v2 contract with the given ID.
func (cm *Manager) V2Contract(id types.FileContractID) (V2Contract, error) {
	return cm.store.V2Contract(id)
}

// V2FileContractElement returns the chain index and file contract element for the
// given contract ID.
func (cm *Manager) V2FileContractElement(id types.FileContractID) (types.ChainIndex, types.V2FileContractElement, error) {
	return cm.store.V2ContractElement(id)
}

// AddContract stores the provided contract, should error if the contract
// already exists.
func (cm *Manager) AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage) error {
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
func (cm *Manager) RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, initialUsage Usage) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// sanity checks
	existingRoots := cm.getSectorRoots(existing.Revision.ParentID)
	if existing.Revision.FileMerkleRoot != (types.Hash256{}) {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.Filesize != 0 {
		return errors.New("existing contract must be cleared")
	} else if existing.Revision.RevisionNumber != types.MaxRevisionNumber {
		return errors.New("existing contract must be cleared")
	} else if renewal.Revision.Filesize != uint64(rhp2.SectorSize*len(existingRoots)) {
		return errors.New("renewal contract must have same file size as existing contract")
	} else if renewal.Revision.FileMerkleRoot != rhp2.MetaRoot(existingRoots) {
		return errors.New("renewal root does not match existing roots")
	}

	if err := cm.store.RenewContract(renewal, existing, formationSet, lockedCollateral, clearingUsage, initialUsage, cm.chain.TipState().Index.Height); err != nil {
		return err
	}
	cm.setSectorRoots(renewal.Revision.ParentID, existingRoots)
	cm.log.Debug("contract renewed", zap.Stringer("renewalID", renewal.Revision.ParentID), zap.Stringer("existingID", existing.Revision.ParentID))
	return nil
}

// ReviseV2Contract atomically updates a contract and its associated sector roots.
func (cm *Manager) ReviseV2Contract(contractID types.FileContractID, revision types.V2FileContract, newRoots []types.Hash256, usage proto4.Usage) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	existing, err := cm.store.V2Contract(contractID)
	if err != nil {
		return fmt.Errorf("failed to get existing contract: %w", err)
	}

	// note: not checking status here since that is only changed after the renewal is confirmed.
	if existing.RenewedTo != (types.FileContractID{}) {
		return errors.New("renewed contracts cannot be revised")
	}

	oldRoots := cm.getSectorRoots(contractID)

	// validate the contract revision fields
	switch {
	case existing.RenterPublicKey != revision.RenterPublicKey:
		return errors.New("renter public key does not match")
	case existing.HostPublicKey != revision.HostPublicKey:
		return errors.New("host public key does not match")
	case existing.ProofHeight != revision.ProofHeight:
		return errors.New("proof height does not match")
	case existing.ExpirationHeight != revision.ExpirationHeight:
		return errors.New("expiration height does not match")
	case revision.Filesize != uint64(rhp2.SectorSize*len(newRoots)):
		return errors.New("revision has incorrect file size")
	case revision.Capacity < revision.Filesize:
		return errors.New("revision capacity must be greater than or equal to file size")
	}

	// validate signatures
	sigHash := cm.chain.TipState().ContractSigHash(revision)
	if !revision.RenterPublicKey.VerifyHash(sigHash, revision.RenterSignature) {
		return errors.New("renter signature is invalid")
	} else if !revision.HostPublicKey.VerifyHash(sigHash, revision.HostSignature) {
		return errors.New("host signature is invalid")
	}

	// validate contract Merkle root
	metaRoot := rhp2.MetaRoot(newRoots)
	if revision.FileMerkleRoot != metaRoot {
		return errors.New("revision root does not match")
	}

	// revise the contract in the store
	err = cm.store.ReviseV2Contract(contractID, revision, oldRoots, newRoots, usage)
	if err != nil {
		return err
	}
	// update the sector roots cache
	cm.setSectorRoots(contractID, newRoots)
	cm.log.Debug("contract revised", zap.Stringer("contractID", contractID), zap.Uint64("revisionNumber", revision.RevisionNumber))
	return nil
}

// AddV2Contract stores the provided contract, should error if the contract
// already exists.
func (cm *Manager) AddV2Contract(formation rhp4.TransactionSet, usage proto4.Usage) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	formationSet := formation.Transactions
	if len(formationSet) == 0 {
		return errors.New("no formation transactions provided")
	} else if len(formationSet[len(formationSet)-1].FileContracts) != 1 {
		return errors.New("last transaction must contain one file contract")
	}

	formationTxn := formationSet[len(formationSet)-1]
	fc := formationTxn.FileContracts[0]
	contractID := formationTxn.V2FileContractID(formationTxn.ID(), 0)

	contract := V2Contract{
		V2FileContract: fc,

		ID:                contractID,
		Status:            V2ContractStatusPending,
		NegotiationHeight: cm.chain.Tip().Height,
		Usage:             usage,
	}

	if err := cm.store.AddV2Contract(contract, formation); err != nil {
		return err
	}
	cm.log.Debug("contract formed", zap.Stringer("contractID", contractID))
	return nil
}

// RenewV2Contract renews a contract. It is expected that the existing
// contract will be cleared.
func (cm *Manager) RenewV2Contract(renewal rhp4.TransactionSet, usage proto4.Usage) error {
	done, err := cm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	renewalSet := renewal.Transactions
	if len(renewalSet) == 0 {
		return errors.New("no renewal transactions provided")
	} else if len(renewalSet[len(renewalSet)-1].FileContractResolutions) != 1 {
		return errors.New("last transaction must contain one file contract resolution")
	}

	resolutionTxn := renewalSet[len(renewalSet)-1]
	resolution, ok := resolutionTxn.FileContractResolutions[0].Resolution.(*types.V2FileContractRenewal)
	if !ok {
		return fmt.Errorf("unexpected resolution type %T", resolutionTxn.FileContractResolutions[0].Resolution)
	}

	parentID := resolutionTxn.FileContractResolutions[0].Parent.ID
	existing, err := cm.store.V2Contract(types.FileContractID(parentID))
	if err != nil {
		return fmt.Errorf("failed to get existing contract: %w", err)
	}
	fc := resolution.NewContract

	// sanity checks
	if fc.Filesize != existing.Filesize {
		return errors.New("renewal contract must have same file size as existing contract")
	} else if fc.Capacity != existing.Capacity {
		return errors.New("renewal contract must have same capacity as existing contract")
	} else if fc.FileMerkleRoot != existing.FileMerkleRoot {
		return errors.New("renewal root does not match existing roots")
	}

	existingID := types.FileContractID(existing.ID)
	existingRoots := cm.getSectorRoots(existingID)
	if fc.FileMerkleRoot != rhp2.MetaRoot(existingRoots) {
		return errors.New("renewal root does not match existing roots")
	}

	contract := V2Contract{
		V2FileContract: fc,

		ID:                existingID.V2RenewalID(),
		Status:            V2ContractStatusPending,
		NegotiationHeight: cm.chain.Tip().Height,
		RenewedFrom:       existingID,
		Usage:             usage,
	}

	if err := cm.store.RenewV2Contract(contract, renewal, existingID, existingRoots); err != nil {
		return err
	}
	cm.setSectorRoots(contract.ID, existingRoots)
	cm.log.Debug("contract renewed", zap.Stringer("formedID", contract.ID), zap.Stringer("existingID", existingID))
	return nil
}

// SectorRoots returns the roots of all sectors stored by the contract.
func (cm *Manager) SectorRoots(id types.FileContractID) []types.Hash256 {
	return cm.getSectorRoots(id)
}

// ReviseContract initializes a new contract updater for the given contract.
func (cm *Manager) ReviseContract(contractID types.FileContractID) (*ContractUpdater, error) {
	done, err := cm.tg.Add()
	if err != nil {
		return nil, err
	}

	roots := cm.getSectorRoots(contractID)
	return &ContractUpdater{
		manager: cm,
		store:   cm.store,
		log:     cm.log.Named("contractUpdater"),

		contractID:  contractID,
		sectorRoots: roots, // roots is already a deep copy
		oldRoots:    append([]types.Hash256(nil), roots...),

		done: done, // decrements the threadgroup counter after the updater is closed
	}, nil
}

// Close closes the contract manager.
func (cm *Manager) Close() error {
	cm.tg.Stop()
	return nil
}

// isGoodForModification validates if a contract can be modified
func (cm *Manager) isGoodForModification(contract Contract) error {
	height := cm.chain.TipState().Index.Height
	switch {
	case contract.Status != ContractStatusActive && contract.Status != ContractStatusPending:
		return fmt.Errorf("contract status is %v, contract cannot be used", contract.Status)
	case (height + cm.revisionSubmissionBuffer) > contract.Revision.WindowStart:
		return fmt.Errorf("contract is too close to the proof window to be revised (%v > %v)", height+cm.revisionSubmissionBuffer, contract.Revision.WindowStart)
	case contract.Revision.RevisionNumber == math.MaxUint64:
		return fmt.Errorf("contract has reached the maximum number of revisions")
	}
	return nil
}

// NewManager creates a new contract manager.
func NewManager(store ContractStore, storage StorageManager, chain ChainManager, syncer Syncer, wallet Wallet, opts ...ManagerOption) (*Manager, error) {
	cm := &Manager{
		store:   store,
		storage: storage,
		chain:   chain,
		syncer:  syncer,
		wallet:  wallet,

		rejectBuffer:             18,
		revisionSubmissionBuffer: 144,

		alerts: alerts.NewNop(),
		tg:     threadgroup.New(),
		log:    zap.NewNop(),

		locks: newLocker(),
	}

	for _, opt := range opts {
		opt(cm)
	}

	start := time.Now()
	roots, err := store.SectorRoots()
	if err != nil {
		return nil, fmt.Errorf("failed to get sector roots: %w", err)
	}

	v2Roots, err := store.V2SectorRoots()
	if err != nil {
		return nil, fmt.Errorf("failed to get v2 sector roots: %w", err)
	}

	for id, contractRoots := range v2Roots {
		roots[id] = contractRoots
	}

	cm.sectorRoots = roots
	cm.log.Debug("loaded sector roots", zap.Duration("elapsed", time.Since(start)))
	return cm, nil
}
