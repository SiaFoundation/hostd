package contracts

import (
	"fmt"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
)

// chainIndexBuffer is the number of chain index elements to store and update
// in the database. Older elements will be deleted. The number of elements
// corresponds to the default proof window.
//
// This is less complex than storing an element per contract or
// tracking each contract's proof window.
const chainIndexBuffer = 144

type (
	stateUpdater interface {
		ForEachFileContractElement(func(types.FileContractElement, bool, *types.FileContractElement, bool, bool))
		ForEachV2FileContractElement(func(types.V2FileContractElement, bool, *types.V2FileContractElement, types.V2FileContractResolutionType))
	}

	// LifecycleActions contains the actions that need to be taken to maintain
	// the lifecycle of active contracts.
	LifecycleActions struct {
		RebroadcastFormation [][]types.Transaction
		BroadcastRevision    []SignedRevision
		BroadcastProof       []SignedRevision

		// V2 actions
		RebroadcastV2Formation []V2FormationTransactionSet
		BroadcastV2Revision    []types.V2FileContractRevision
		BroadcastV2Proof       []types.V2FileContractElement
		BroadcastV2Expiration  []types.V2FileContractElement
	}

	// StateChanges contains the changes to the state of contracts on the
	// blockchain
	StateChanges struct {
		Confirmed  []types.FileContractElement
		Revised    []types.FileContractElement
		Successful []types.FileContractID
		Failed     []types.FileContractID

		// V2 changes
		ConfirmedV2  []types.V2FileContractElement
		RevisedV2    []types.V2FileContractElement
		SuccessfulV2 []types.FileContractID
		FinalizedV2  []types.FileContractID
		RenewedV2    []types.FileContractID
		FailedV2     []types.FileContractID
	}

	// An UpdateStateTx atomically updates the state of contracts in the contract
	// store.
	UpdateStateTx interface {
		// ContractStateElements returns all state elements from the contract
		// store
		ContractStateElements() ([]types.StateElement, error)
		// UpdateContractStateElements updates the state elements in the host
		// contract store
		UpdateContractStateElements([]types.StateElement) error
		// ContractRelevant returns whether the contract with the provided id is
		// relevant to the host
		ContractRelevant(id types.FileContractID) (bool, error)
		// V2ContractRelevant returns whether the v2 contract with the
		// provided id is relevant to the host
		V2ContractRelevant(id types.FileContractID) (bool, error)
		// ApplyContracts applies relevant contract changes to the contract
		// store
		ApplyContracts(types.ChainIndex, StateChanges) error
		// RevertContracts reverts relevant contract changes from the contract
		// store
		RevertContracts(types.ChainIndex, StateChanges) error
		// RejectContracts sets the status of any v1 and v2 contracts with a
		// negotiation height before the provided height and that have not
		// been confirmed to rejected
		RejectContracts(height uint64) (v1, v2 []types.FileContractID, err error)

		// ContractChainIndexElements returns all chain index elements from the
		// contract store
		ContractChainIndexElements() (elements []types.ChainIndexElement, err error)
		// ApplyContractChainIndexElements adds or updates the merkle proof of
		// chain index state elements
		ApplyContractChainIndexElements(elements []types.ChainIndexElement) error
		// RevertContractChainIndexElements removes chain index state elements
		// that were reverted
		RevertContractChainIndexElement(types.ChainIndex) error
		// DeleteExpiredContractChainIndexElements deletes chain index state
		// elements that are no long necessary
		DeleteExpiredContractChainIndexElements(height uint64) error
	}
)

func (cm *Manager) buildStorageProof(revision types.FileContractRevision, index uint64, log *zap.Logger) (types.StorageProof, error) {
	if revision.Filesize == 0 {
		return types.StorageProof{
			ParentID: revision.ParentID,
		}, nil
	}

	sectorIndex := index / rhp2.LeavesPerSector
	segmentIndex := index % rhp2.LeavesPerSector

	roots := cm.getSectorRoots(revision.ParentID)
	contractRoot := rhp2.MetaRoot(roots)
	if contractRoot != revision.FileMerkleRoot {
		log.Error("unexpected contract merkle root", zap.Stringer("expectedRoot", revision.FileMerkleRoot), zap.Stringer("actualRoot", contractRoot))
		return types.StorageProof{}, fmt.Errorf("merkle root mismatch")
	} else if uint64(len(roots)) < sectorIndex {
		log.Error("unexpected proof index", zap.Uint64("sectorIndex", sectorIndex), zap.Uint64("segmentIndex", segmentIndex), zap.Int("rootsLength", len(roots)))
		return types.StorageProof{}, fmt.Errorf("invalid root index")
	}

	sectorRoot := roots[sectorIndex]
	sector, err := cm.storage.Read(sectorRoot)
	if err != nil {
		log.Error("failed to read sector data", zap.Error(err), zap.Stringer("sectorRoot", sectorRoot))
		return types.StorageProof{}, fmt.Errorf("failed to read sector data")
	} else if rhp2.SectorRoot(sector) != sectorRoot {
		log.Error("sector data corrupt", zap.Stringer("expectedRoot", sectorRoot), zap.Stringer("actualRoot", rhp2.SectorRoot(sector)))
		return types.StorageProof{}, fmt.Errorf("invalid sector root")
	}
	segmentProof := rhp2.ConvertProofOrdering(rhp2.BuildProof(sector, segmentIndex, segmentIndex+1, nil), segmentIndex)
	sectorProof := rhp2.ConvertProofOrdering(rhp2.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.StorageProof{
		ParentID: revision.ParentID,
		Proof:    append(segmentProof, sectorProof...),
	}
	copy(sp.Leaf[:], sector[segmentIndex*rhp2.LeafSize:])
	return sp, nil
}

func (cm *Manager) buildV2StorageProof(cs consensus.State, fce types.V2FileContractElement, pi types.ChainIndexElement, log *zap.Logger) (types.V2StorageProof, error) {
	if fce.V2FileContract.Filesize == 0 {
		return types.V2StorageProof{
			ProofIndex: pi,
		}, nil
	}

	revision := fce.V2FileContract
	contractID := types.FileContractID(fce.ID)

	leafIndex := cs.StorageProofLeafIndex(fce.V2FileContract.Filesize, types.BlockID(pi.ID), contractID)
	sectorIndex := leafIndex / rhp2.LeavesPerSector
	segmentIndex := leafIndex % rhp2.LeavesPerSector

	roots := cm.getSectorRoots(contractID)
	contractRoot := rhp2.MetaRoot(roots)
	if contractRoot != revision.FileMerkleRoot {
		log.Error("unexpected contract root", zap.Stringer("expectedRoot", revision.FileMerkleRoot), zap.Stringer("actualRoot", contractRoot))
		return types.V2StorageProof{}, fmt.Errorf("merkle root mismatch")
	} else if uint64(len(roots)) < sectorIndex {
		log.Error("unexpected root index", zap.Uint64("sectorIndex", sectorIndex), zap.Uint64("segmentIndex", segmentIndex), zap.Int("rootsLength", len(roots)))
		return types.V2StorageProof{}, fmt.Errorf("invalid root index")
	}

	sectorRoot := roots[sectorIndex]
	sector, err := cm.storage.Read(sectorRoot)
	if err != nil {
		log.Error("failed to read sector data", zap.Error(err), zap.Stringer("sectorRoot", sectorRoot))
		return types.V2StorageProof{}, fmt.Errorf("failed to read sector data")
	} else if rhp2.SectorRoot(sector) != sectorRoot {
		log.Error("sector data corrupt", zap.Stringer("expectedRoot", sectorRoot), zap.Stringer("actualRoot", rhp2.SectorRoot(sector)))
		return types.V2StorageProof{}, fmt.Errorf("invalid sector root")
	}
	segmentProof := rhp2.ConvertProofOrdering(rhp2.BuildProof(sector, segmentIndex, segmentIndex+1, nil), segmentIndex)
	sectorProof := rhp2.ConvertProofOrdering(rhp2.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.V2StorageProof{
		ProofIndex: pi,
		Proof:      append(segmentProof, sectorProof...),
	}
	copy(sp.Leaf[:], sector[segmentIndex*rhp2.LeafSize:])
	return sp, nil
}

// ProcessActions processes additional lifecycle actions after a new block is
// added to the index.
func (cm *Manager) ProcessActions(index types.ChainIndex) error {
	log := cm.log.Named("lifecycle").With(zap.Stringer("index", index))

	revisionBroadcastHeight := index.Height + cm.revisionSubmissionBuffer
	actions, err := cm.store.ContractActions(index, revisionBroadcastHeight)
	if err != nil {
		return fmt.Errorf("failed to get contract actions: %w", err)
	}

	for _, formationSet := range actions.RebroadcastFormation {
		if len(formationSet) == 0 {
			continue
		} else if _, err := cm.chain.AddPoolTransactions(formationSet); err != nil {
			log.Error("failed to add formation transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastTransactionSet(formationSet)
		log.Debug("rebroadcast formation transaction", zap.String("transactionID", formationSet[len(formationSet)-1].ID().String()))
	}

	for _, revision := range actions.BroadcastRevision {
		log := log.Named("broadcastRevision").With(zap.Stringer("contractID", revision.Revision.ParentID), zap.Uint64("windowStart", revision.Revision.WindowStart), zap.Uint64("revisionNumber", revision.Revision.RevisionNumber))
		revisionTxn := types.Transaction{
			FileContractRevisions: []types.FileContractRevision{revision.Revision},
			Signatures: []types.TransactionSignature{
				{
					ParentID:      types.Hash256(revision.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     revision.RenterSignature[:],
				},
				{
					ParentID:       types.Hash256(revision.Revision.ParentID),
					CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:      revision.HostSignature[:],
					PublicKeyIndex: 1,
				},
			},
		}

		fee := cm.chain.RecommendedFee().Mul64(1000)
		revisionTxn.MinerFees = append(revisionTxn.MinerFees, fee)
		toSign, err := cm.wallet.FundTransaction(&revisionTxn, fee, true)
		if err != nil {
			log.Error("failed to fund revision transaction", zap.Error(err))
			continue
		}
		cm.wallet.SignTransaction(&revisionTxn, toSign, types.CoveredFields{WholeTransaction: true})
		revisionTxnSet := append(cm.chain.UnconfirmedParents(revisionTxn), revisionTxn)
		if _, err := cm.chain.AddPoolTransactions(revisionTxnSet); err != nil {
			cm.wallet.ReleaseInputs(revisionTxnSet, nil)
			log.Error("failed to add revision transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastTransactionSet(revisionTxnSet)
		log.Debug("broadcast revision transaction", zap.String("transactionID", revisionTxn.ID().String()))
	}

	cs := cm.chain.TipState()
	for _, revision := range actions.BroadcastProof {
		log := log.Named("proof").With(zap.Stringer("contractID", revision.Revision.ParentID))
		validPayout, missedPayout := revision.Revision.ValidHostPayout(), revision.Revision.MissedHostPayout()
		if missedPayout.Cmp(validPayout) >= 0 {
			log.Debug("skipping storage proof, no benefit to host", zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()))
			continue
		}

		proofIndex, ok := cm.chain.BestIndex(revision.Revision.WindowStart - 1)
		if !ok {
			log.Error("proof index not found", zap.Uint64("windowStart", revision.Revision.WindowStart))
			continue
		}

		leafIndex := cs.StorageProofLeafIndex(revision.Revision.Filesize, proofIndex.ID, revision.Revision.ParentID)
		sp, err := cm.buildStorageProof(revision.Revision, leafIndex, log)
		if err != nil {
			log.Error("failed to build storage proof", zap.Error(err))
			continue
		}

		fee := cm.chain.RecommendedFee().Mul64(2000)
		resolutionTxnSet := []types.Transaction{
			{
				// intermediate funding transaction is required by v1 because
				// transactions with storage proofs cannot have change outputs
				SiacoinOutputs: []types.SiacoinOutput{
					{Address: cm.wallet.Address(), Value: fee},
				},
			},
			{
				MinerFees:     []types.Currency{fee},
				StorageProofs: []types.StorageProof{sp},
			},
		}

		intermediateToSign, err := cm.wallet.FundTransaction(&resolutionTxnSet[0], fee, true)
		if err != nil {
			log.Error("failed to fund resolution transaction", zap.Error(err))
			continue
		}
		cm.wallet.SignTransaction(&resolutionTxnSet[0], intermediateToSign, types.CoveredFields{WholeTransaction: true})
		resolutionTxnSet[1].SiacoinInputs = append(resolutionTxnSet[1].SiacoinInputs, types.SiacoinInput{
			ParentID:         resolutionTxnSet[0].SiacoinOutputID(0),
			UnlockConditions: cm.wallet.UnlockConditions(),
		})
		proofToSign := []types.Hash256{types.Hash256(resolutionTxnSet[1].SiacoinInputs[0].ParentID)}
		cm.wallet.SignTransaction(&resolutionTxnSet[1], proofToSign, types.CoveredFields{WholeTransaction: true})
		resolutionTxnSet = append(cm.chain.UnconfirmedParents(resolutionTxnSet[0]), resolutionTxnSet...)
		if _, err := cm.chain.AddPoolTransactions(resolutionTxnSet); err != nil {
			cm.wallet.ReleaseInputs(resolutionTxnSet, nil)
			log.Error("failed to add resolution transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastTransactionSet(resolutionTxnSet)
		log.Debug("broadcast transaction", zap.String("transactionID", resolutionTxnSet[1].ID().String()))
	}

	for _, formationSet := range actions.RebroadcastV2Formation {
		if len(formationSet.TransactionSet) == 0 {
			continue
		}
		formationTxn := formationSet.TransactionSet[len(formationSet.TransactionSet)-1]
		if len(formationTxn.FileContracts) == 0 {
			continue
		}

		contractID := formationTxn.V2FileContractID(formationTxn.ID(), 0)
		log := log.Named("v2 formation").With(zap.Stringer("basis", formationSet.Basis), zap.Stringer("contractID", contractID))

		if _, err := cm.chain.AddV2PoolTransactions(formationSet.Basis, formationSet.TransactionSet); err != nil {
			log.Error("failed to add formation transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastV2TransactionSet(formationSet.Basis, formationSet.TransactionSet)
		log.Debug("broadcast transaction", zap.String("transactionID", formationSet.TransactionSet[len(formationSet.TransactionSet)-1].ID().String()))
	}

	for _, fcr := range actions.BroadcastV2Revision {
		log := log.Named("v2 revision").With(zap.Stringer("contractID", fcr.Parent.ID))

		fee := cm.chain.RecommendedFee().Mul64(1000)
		revisionTxn := types.V2Transaction{
			MinerFee:              fee,
			FileContractRevisions: []types.V2FileContractRevision{fcr},
		}
		cs, toSign, err := cm.wallet.FundV2Transaction(&revisionTxn, fee, false) // TODO: true
		if err != nil {
			log.Error("failed to fund transaction", zap.Error(err))
			continue
		}
		cm.wallet.SignV2Inputs(cs, &revisionTxn, toSign)

		revisionTxnSet := []types.V2Transaction{revisionTxn}
		if _, err := cm.chain.AddV2PoolTransactions(cs.Index, revisionTxnSet); err != nil {
			log.Error("failed to add transaction set to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastV2TransactionSet(cs.Index, revisionTxnSet)
		log.Debug("broadcast transaction", zap.Stringer("transactionID", revisionTxn.ID()))
	}

	for _, fce := range actions.BroadcastV2Proof {
		log := log.Named("v2 proof").With(zap.Stringer("contractID", fce.ID))
		proofIndex, ok := cm.chain.BestIndex(fce.V2FileContract.ProofHeight)
		if !ok {
			log.Error("proof index not found", zap.Uint64("proofHeight", fce.V2FileContract.ProofHeight))
			continue
		}
		proofElement, err := cm.store.ContractChainIndexElement(proofIndex)
		if err != nil {
			log.Error("failed to get proof index element", zap.Stringer("proofIndex", proofIndex), zap.Error(err))
			continue
		}

		sp, err := cm.buildV2StorageProof(cs, fce, proofElement, log.Named("proof"))
		if err != nil {
			log.Error("failed to build storage proof", zap.Error(err))
			continue
		}

		resolution := types.V2FileContractResolution{
			Parent:     fce,
			Resolution: &sp,
		}

		fee := cm.chain.RecommendedFee().Mul64(2000)
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: cm.wallet.Address(), Value: fee},
			},
		}
		cs, toSign, err := cm.wallet.FundV2Transaction(&setupTxn, fee, false) // TODO: true
		if err != nil {
			log.Error("failed to fund resolution transaction", zap.Error(err))
			continue
		}
		cm.wallet.SignV2Inputs(cs, &setupTxn, toSign)
		resolutionTxn := types.V2Transaction{
			MinerFee:                fee,
			SiacoinInputs:           []types.V2SiacoinInput{{Parent: setupTxn.EphemeralSiacoinOutput(0)}},
			FileContractResolutions: []types.V2FileContractResolution{resolution},
		}
		cm.wallet.SignV2Inputs(cs, &resolutionTxn, []int{0})
		resolutionTxnSet := []types.V2Transaction{setupTxn, resolutionTxn}
		if _, err := cm.chain.AddV2PoolTransactions(cs.Index, resolutionTxnSet); err != nil {
			log.Error("failed to add resolution transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastV2TransactionSet(cs.Index, resolutionTxnSet)
		log.Debug("broadcast transaction", zap.String("transactionID", resolutionTxn.ID().String()))
	}

	for _, fce := range actions.BroadcastV2Expiration {
		log := log.Named("v2 expiration").With(zap.Stringer("contractID", fce.ID))

		fee := cm.chain.RecommendedFee().Mul64(1000)
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: cm.wallet.Address(), Value: fee},
			},
		}
		cs, toSign, err := cm.wallet.FundV2Transaction(&setupTxn, fee, false) // TODO: true
		if err != nil {
			log.Error("failed to fund resolution transaction", zap.Error(err))
			continue
		}
		cm.wallet.SignV2Inputs(cs, &setupTxn, toSign)
		resolutionTxn := types.V2Transaction{
			MinerFee: fee,
			SiacoinInputs: []types.V2SiacoinInput{
				{
					Parent: setupTxn.EphemeralSiacoinOutput(0),
				},
			},
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &types.V2FileContractExpiration{},
				},
			},
		}
		cm.wallet.SignV2Inputs(cs, &resolutionTxn, []int{0})

		resolutionTxnSet := []types.V2Transaction{setupTxn, resolutionTxn}
		if _, err := cm.chain.AddV2PoolTransactions(cs.Index, resolutionTxnSet); err != nil {
			cm.wallet.ReleaseInputs(nil, resolutionTxnSet)
			log.Error("failed to add resolution transaction to pool", zap.Error(err))
			continue
		}
		cm.syncer.BroadcastV2TransactionSet(cs.Index, resolutionTxnSet)
		log.Debug("broadcast transaction", zap.String("transactionID", resolutionTxn.ID().String()))
	}

	if err := cm.store.ExpireContractSectors(index.Height); err != nil {
		return fmt.Errorf("failed to expire contract sectors: %w", err)
	} else if err := cm.store.ExpireV2ContractSectors(index.Height); err != nil {
		return fmt.Errorf("failed to expire v2 contract sectors: %w", err)
	}
	return nil
}

// buildContractState helper to build state changes from a state update
func buildContractState(tx UpdateStateTx, u stateUpdater, revert bool, log *zap.Logger) (state StateChanges) {
	u.ForEachFileContractElement(func(fce types.FileContractElement, created bool, rev *types.FileContractElement, resolved, valid bool) {
		log := log.With(zap.Stringer("contractID", fce.ID))
		if relevant, err := tx.ContractRelevant(types.FileContractID(fce.ID)); err != nil {
			log.Fatal("failed to check contract relevance", zap.Error(err))
		} else if !relevant {
			return
		}

		switch {
		case created:
			state.Confirmed = append(state.Confirmed, fce)
			log.Debug("confirmed contract")
		case rev != nil:
			if revert {
				state.Revised = append(state.Revised, fce)
				log.Debug("revised contract", zap.Uint64("current", rev.FileContract.RevisionNumber), zap.Uint64("revised", fce.FileContract.RevisionNumber))
			} else {
				state.Revised = append(state.Revised, *rev)
				log.Debug("revised contract", zap.Uint64("current", fce.FileContract.RevisionNumber), zap.Uint64("revised", fce.FileContract.RevisionNumber))
			}
		case resolved && valid:
			state.Successful = append(state.Successful, types.FileContractID(fce.ID))
			log.Debug("successful contract")
		case resolved && !valid:
			successful := fce.FileContract.MissedHostPayout().Cmp(fce.FileContract.ValidHostPayout()) >= 0
			if successful {
				state.Successful = append(state.Successful, types.FileContractID(fce.ID))
			} else {
				state.Failed = append(state.Failed, types.FileContractID(fce.ID))
			}
			log.Debug("expired contract", zap.Bool("successful", successful))
		default:
			log.Fatal("unexpected contract state", zap.Bool("resolved", resolved), zap.Bool("valid", valid), zap.Bool("created", created), zap.Bool("revised", rev != nil), zap.Stringer("contractID", fce.ID))
		}
	})

	u.ForEachV2FileContractElement(func(fce types.V2FileContractElement, created bool, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		log := log.With(zap.Stringer("contractID", fce.ID))

		if relevant, err := tx.V2ContractRelevant(types.FileContractID(fce.ID)); err != nil {
			log.Fatal("failed to check contract relevance", zap.Error(err))
		} else if !relevant {
			return
		}

		switch {
		case created:
			state.ConfirmedV2 = append(state.ConfirmedV2, fce)
			log.Debug("confirmed v2 contract", zap.Stringer("contractID", fce.ID))
		case rev != nil:
			if revert {
				state.RevisedV2 = append(state.RevisedV2, fce)
				log.Debug("revised contract", zap.Uint64("current", rev.V2FileContract.RevisionNumber), zap.Uint64("revised", fce.V2FileContract.RevisionNumber))
			} else {
				state.RevisedV2 = append(state.RevisedV2, *rev)
				log.Debug("revised contract", zap.Uint64("current", fce.V2FileContract.RevisionNumber), zap.Uint64("revised", rev.V2FileContract.RevisionNumber))
			}
		case res != nil:
			switch res := res.(type) {
			case *types.V2FileContractFinalization:
				state.FinalizedV2 = append(state.FinalizedV2, types.FileContractID(fce.ID))
				log.Debug("finalized v2 contract", zap.Stringer("contractID", fce.ID))
			case *types.V2FileContractRenewal:
				state.RenewedV2 = append(state.RenewedV2, types.FileContractID(fce.ID))
				log.Debug("renewed v2 contract", zap.Stringer("contractID", fce.ID))
			case *types.V2FileContractExpiration:
				fc := fce.V2FileContract
				successful := fc.MissedHostValue.Cmp(fc.HostOutput.Value) >= 0
				if successful {
					state.SuccessfulV2 = append(state.SuccessfulV2, types.FileContractID(fce.ID))
				} else {
					state.FailedV2 = append(state.FailedV2, types.FileContractID(fce.ID))
				}
				log.Debug("expired v2 contract", zap.Stringer("contractID", fce.ID), zap.Bool("successful", successful))
			case *types.V2StorageProof:
				state.SuccessfulV2 = append(state.SuccessfulV2, types.FileContractID(fce.ID))
				log.Debug("successful v2 contract", zap.Stringer("contractID", fce.ID))
			default:
				panic(fmt.Sprintf("unexpected contract resolution type: %T", res))
			}
		default:
			log.Fatal("unexpected v2 contract state", zap.Bool("resolved", res != nil), zap.Bool("created", created), zap.Bool("revised", rev != nil), zap.Stringer("contractID", fce.ID))
		}
	})
	return
}

// UpdateChainState updates the state of the contracts on chain
func (cm *Manager) UpdateChainState(tx UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	log := cm.log.Named("updateChainState")

	chainElements, err := tx.ContractChainIndexElements()
	if err != nil {
		return fmt.Errorf("failed to get chain index state elements: %w", err)
	}

	v2ContractStateElements, err := tx.ContractStateElements()
	if err != nil {
		return fmt.Errorf("failed to get contract state elements: %w", err)
	}
	v2ContractElementMap := make(map[types.Hash256]*types.StateElement, len(v2ContractStateElements))
	for _, ele := range v2ContractStateElements {
		v2ContractElementMap[ele.ID] = &ele
	}

	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			ID:     cru.Block.ID(),
			Height: cru.State.Index.Height + 1,
		}

		// revert contract state changes
		state := buildContractState(tx, cru, true, log.Named("revert").With(zap.Stringer("index", revertedIndex)))
		if err := tx.RevertContracts(revertedIndex, state); err != nil {
			return fmt.Errorf("failed to revert contracts: %w", err)
		}

		// delete reverted contract state elements from the map
		for _, reverted := range state.ConfirmedV2 {
			delete(v2ContractElementMap, reverted.ID)
		}
		// update remaining contract state elements
		for key := range v2ContractElementMap {
			cru.UpdateElementProof(v2ContractElementMap[key])
		}

		// revert contract chain index element
		if err := tx.RevertContractChainIndexElement(revertedIndex); err != nil {
			return fmt.Errorf("failed to revert chain index state element: %w", err)
		}

		// update chain state elements
		if len(chainElements) > 0 {
			last := chainElements[len(chainElements)-1]
			if last.ChainIndex != revertedIndex {
				panic(fmt.Errorf("unexpected chain index: %v != %v", last.ChainIndex, revertedIndex)) // developer error
			}
			chainElements = chainElements[:len(chainElements)-1]
			for i := range chainElements {
				cru.UpdateElementProof(&chainElements[i].StateElement)
			}
		}
	}

	for _, cau := range applied {
		state := buildContractState(tx, cau, false, log.Named("apply").With(zap.Stringer("index", cau.State.Index)))
		// apply state changes
		if err := tx.ApplyContracts(cau.State.Index, state); err != nil {
			return fmt.Errorf("failed to revert contracts: %w", err)
		}

		// update existing contract state elements
		for id := range v2ContractElementMap {
			cau.UpdateElementProof(v2ContractElementMap[id])
		}
		// add new contract state elements
		for _, applied := range state.ConfirmedV2 {
			v2ContractElementMap[applied.ID] = &applied.StateElement
		}

		// update existing chain index elements proofs
		for i := range chainElements {
			cau.UpdateElementProof(&chainElements[i].StateElement)
		}
		// add new chain index element
		chainElements = append(chainElements, cau.ChainIndexElement())
		if len(chainElements) > chainIndexBuffer {
			chainElements = chainElements[len(chainElements)-chainIndexBuffer:]
		}

		// reject any contracts that have not been confirmed after the reject buffer
		index := cau.State.Index
		if index.Height >= cm.rejectBuffer {
			minNegotiationHeight := index.Height - cm.rejectBuffer
			rejectedV1, rejectedV2, err := tx.RejectContracts(minNegotiationHeight)
			if err != nil {
				return fmt.Errorf("failed to reject contracts: %w", err)
			}

			if len(rejectedV1) > 0 {
				log.Debug("rejected contracts", zap.Int("count", len(rejectedV1)))
			}
			if len(rejectedV2) > 0 {
				log.Debug("rejected v2 contracts", zap.Int("count", len(rejectedV2)))
			}
		}

		// delete any chain index elements outside of the proof window buffer
		if cau.State.Index.Height > chainIndexBuffer {
			minHeight := cau.State.Index.Height - chainIndexBuffer
			if err := tx.DeleteExpiredContractChainIndexElements(minHeight); err != nil {
				return fmt.Errorf("failed to delete expired chain index elements: %w", err)
			}
		}
	}

	// update chain index state elements
	if len(chainElements) > 0 {
		if err := tx.ApplyContractChainIndexElements(chainElements); err != nil {
			return fmt.Errorf("failed to update chain index state elements: %w", err)
		}
	}

	// update contract state elements
	if len(v2ContractElementMap) > 0 {
		contractStateElements := make([]types.StateElement, 0, len(v2ContractElementMap))
		for _, ele := range v2ContractElementMap {
			contractStateElements = append(contractStateElements, *ele)
		}
		if err := tx.UpdateContractStateElements(contractStateElements); err != nil {
			return fmt.Errorf("failed to update contract state elements: %w", err)
		}
	}
	return nil
}
