package contracts

import (
	"fmt"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

const (
	// chainIndexBuffer is the number of chain index elements to store and update
	// in the database. Older elements will be deleted. The number of elements
	// corresponds to the default proof window.
	//
	// This is less complex than storing an element per contract or
	// tracking each contract's proof window.
	chainIndexBuffer = 144

	// ReorgBuffer is the number of blocks after contract expiration before
	// storage will be reclaimed. This is to prevent small reorgs from
	// causing contracts to fail unnecessarily.
	ReorgBuffer = 6
)

type (
	// V2ProofElement groups the necessary chain index element with its
	// file contract element for building a v2 storage proof resolution.
	V2ProofElement struct {
		ChainIndexElement     types.ChainIndexElement
		V2FileContractElement types.V2FileContractElement
	}

	// LifecycleActions contains the actions that need to be taken to maintain
	// the lifecycle of active contracts.
	LifecycleActions struct {
		Basis types.ChainIndex

		RebroadcastV2Formation []rhp4.TransactionSet
		BroadcastV2Revision    []types.V2FileContractRevision
		BroadcastV2Proof       []V2ProofElement
		BroadcastV2Expiration  []types.V2FileContractElement
	}

	// RevisedContract contains information about a contract that has been
	// revised on chain.
	RevisedContract struct {
		ID types.FileContractID
		types.FileContract
	}

	// RevisedV2Contract contains information about a v2 contract that has been
	// revised on chain.
	RevisedV2Contract struct {
		ID types.FileContractID
		types.V2FileContract
	}

	// StateChanges contains the changes to the state of contracts on the
	// blockchain
	StateChanges struct {
		Confirmed  []types.FileContractElement
		Revised    []RevisedContract
		Successful []types.FileContractID
		Failed     []types.FileContractID

		// V2 changes
		ConfirmedV2  []types.V2FileContractElement
		RevisedV2    []RevisedV2Contract
		SuccessfulV2 []types.FileContractID
		RenewedV2    []types.FileContractID
		FailedV2     []types.FileContractID
	}

	// An UpdateStateTx atomically updates the state of contracts in the contract
	// store.
	UpdateStateTx interface {
		// UpdateContractElementProofs updates the state elements in the host
		// contract store
		UpdateContractElementProofs(wallet.ProofUpdater) error
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

		// AddContractChainIndexElement adds or updates the merkle proof of
		// chain index state elements
		AddContractChainIndexElement(elements types.ChainIndexElement) error
		// RevertContractChainIndexElements removes chain index state elements
		// that were reverted
		RevertContractChainIndexElement(types.ChainIndex) error
		// UpdateChainIndexElementProofs returns all chain index elements from the
		// contract store
		UpdateChainIndexElementProofs(wallet.ProofUpdater) error
		// DeleteExpiredContractChainIndexElements deletes chain index state
		// elements that are no long necessary
		DeleteExpiredChainIndexElements(height uint64) error
	}
)

func (cm *Manager) buildV2StorageProof(cs consensus.State, ele V2ProofElement, log *zap.Logger) (types.V2StorageProof, error) {
	fce := ele.V2FileContractElement
	if fce.V2FileContract.Filesize == 0 {
		return types.V2StorageProof{
			ProofIndex: ele.ChainIndexElement,
		}, nil
	}

	revision := fce.V2FileContract
	contractID := types.FileContractID(fce.ID)

	leafIndex := cs.StorageProofLeafIndex(fce.V2FileContract.Filesize, ele.ChainIndexElement.ID, contractID)
	sectorIndex := leafIndex / proto4.LeavesPerSector
	segmentIndex := leafIndex % proto4.LeavesPerSector

	roots := cm.getSectorRoots(contractID)
	contractRoot := proto4.MetaRoot(roots)
	if contractRoot != revision.FileMerkleRoot {
		log.Error("unexpected contract root", zap.Stringer("expectedRoot", revision.FileMerkleRoot), zap.Stringer("actualRoot", contractRoot))
		return types.V2StorageProof{}, fmt.Errorf("merkle root mismatch")
	} else if uint64(len(roots)) < sectorIndex {
		log.Error("unexpected root index", zap.Uint64("sectorIndex", sectorIndex), zap.Uint64("segmentIndex", segmentIndex), zap.Int("rootsLength", len(roots)))
		return types.V2StorageProof{}, fmt.Errorf("invalid root index")
	}

	sectorRoot := roots[sectorIndex]
	segment, proof, err := cm.storage.ReadSector(sectorRoot, segmentIndex*proto4.LeafSize, proto4.LeafSize)
	if err != nil {
		log.Error("failed to read sector data", zap.Error(err), zap.Stringer("sectorRoot", sectorRoot))
		return types.V2StorageProof{}, fmt.Errorf("failed to read sector data")
	}
	segmentProof := rhp2.ConvertProofOrdering(proof, segmentIndex)
	sectorProof := rhp2.ConvertProofOrdering(rhp2.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.V2StorageProof{
		ProofIndex: ele.ChainIndexElement,
		Leaf:       ([64]byte)(segment),
		Proof:      append(segmentProof, sectorProof...),
	}
	return sp, nil
}

func (cm *Manager) broadcastV2Revision(revisionBasis types.ChainIndex, fcr types.V2FileContractRevision, log *zap.Logger) error {
	fee := cm.wallet.RecommendedFee().Mul64(1000)
	revisionTxn := types.V2Transaction{
		MinerFee:              fee,
		FileContractRevisions: []types.V2FileContractRevision{fcr},
	}
	basis, toSign, err := cm.wallet.FundV2Transaction(&revisionTxn, fee, true)
	if err != nil {
		return fmt.Errorf("failed to fund revision transaction: %w", err)
	}

	if revisionBasis != basis {
		log.Debug("updating revision basis", zap.Stringer("current", revisionBasis), zap.Stringer("target", basis))
		updated, err := cm.chain.UpdateV2TransactionSet([]types.V2Transaction{
			{FileContractRevisions: []types.V2FileContractRevision{fcr}},
		}, revisionBasis, basis)
		if err != nil {
			cm.wallet.ReleaseInputs(nil, []types.V2Transaction{revisionTxn})
			return fmt.Errorf("failed to update transaction set basis: %w", err)
		}
		revisionTxn.FileContractRevisions[0] = updated[0].FileContractRevisions[0]
	}
	cm.wallet.SignV2Inputs(&revisionTxn, toSign)

	if basis, revisionTxnSet, err := cm.chain.V2TransactionSet(basis, revisionTxn); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{revisionTxn})
		return fmt.Errorf("failed to get transaction set parents: %w", err)
	} else if err := cm.wallet.BroadcastV2TransactionSet(basis, revisionTxnSet); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{revisionTxn})
		return fmt.Errorf("failed to broadcast transaction set: %w", err)
	}
	log.Debug("broadcast revision transaction", zap.Stringer("transactionID", revisionTxn.ID()))
	return nil
}

func (cm *Manager) broadcastV2StorageProof(cs consensus.State, proofBasis types.ChainIndex, ele V2ProofElement, log *zap.Logger) error {
	sp, err := cm.buildV2StorageProof(cs, ele, log.Named("proof"))
	if err != nil {
		return fmt.Errorf("failed to build storage proof: %w", err)
	}

	resolution := types.V2FileContractResolution{
		Parent:     ele.V2FileContractElement,
		Resolution: &sp,
	}

	fee := cm.wallet.RecommendedFee().Mul64(2000)
	resolutionTxn := types.V2Transaction{
		MinerFee:                fee,
		FileContractResolutions: []types.V2FileContractResolution{resolution},
	}
	basis, toSign, err := cm.wallet.FundV2Transaction(&resolutionTxn, fee, true)
	if err != nil {
		return fmt.Errorf("failed to fund resolution transaction: %w", err)
	}
	cm.wallet.SignV2Inputs(&resolutionTxn, toSign)

	if proofBasis != basis {
		log.Debug("updating proof basis", zap.Stringer("current", proofBasis), zap.Stringer("target", basis))
		updated, err := cm.chain.UpdateV2TransactionSet([]types.V2Transaction{
			{FileContractResolutions: []types.V2FileContractResolution{resolution}},
		}, proofBasis, basis)
		if err != nil {
			cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
			return fmt.Errorf("failed to update transaction set basis: %w", err)
		}
		resolutionTxn.FileContractResolutions[0] = updated[0].FileContractResolutions[0]
	}

	if basis, resolutionTxnSet, err := cm.chain.V2TransactionSet(basis, resolutionTxn); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
		return fmt.Errorf("failed to get transaction set parents: %w", err)
	} else if err := cm.wallet.BroadcastV2TransactionSet(basis, resolutionTxnSet); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
		return fmt.Errorf("failed to broadcast transaction set: %w", err)
	}
	log.Debug("broadcast v2 storage proof transaction", zap.Stringer("transactionID", resolutionTxn.ID()))
	return nil
}

func (cm *Manager) broadcastV2Expiration(elementBasis types.ChainIndex, fce types.V2FileContractElement, log *zap.Logger) error {
	fee := cm.wallet.RecommendedFee().Mul64(1000)
	resolutionTxn := types.V2Transaction{
		MinerFee: fee,
		FileContractResolutions: []types.V2FileContractResolution{
			{
				Parent:     fce,
				Resolution: &types.V2FileContractExpiration{},
			},
		},
	}
	basis, toSign, err := cm.wallet.FundV2Transaction(&resolutionTxn, fee, true)
	if err != nil {
		return fmt.Errorf("failed to fund resolution transaction: %w", err)
	}
	cm.wallet.SignV2Inputs(&resolutionTxn, toSign)

	if elementBasis != basis {
		log.Debug("updating expiration basis", zap.Stringer("current", elementBasis), zap.Stringer("target", basis))
		updated, err := cm.chain.UpdateV2TransactionSet([]types.V2Transaction{
			{FileContractResolutions: []types.V2FileContractResolution{resolutionTxn.FileContractResolutions[0]}},
		}, elementBasis, basis)
		if err != nil {
			cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
			return fmt.Errorf("failed to update transaction set basis: %w", err)
		}
		resolutionTxn.FileContractResolutions[0] = updated[0].FileContractResolutions[0]
	}

	if basis, resolutionTxnSet, err := cm.chain.V2TransactionSet(basis, resolutionTxn); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
		return fmt.Errorf("failed to get transaction set parents: %w", err)
	} else if err := cm.wallet.BroadcastV2TransactionSet(basis, resolutionTxnSet); err != nil {
		cm.wallet.ReleaseInputs(nil, []types.V2Transaction{resolutionTxn})
		return fmt.Errorf("failed to broadcast transaction set: %w", err)
	}
	log.Debug("broadcast expiration transaction", zap.Stringer("transactionID", resolutionTxn.ID()))
	return nil
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

	for _, formationSet := range actions.RebroadcastV2Formation {
		switch {
		case len(formationSet.Transactions) == 0:
			log.Debug("skipping empty formation set")
			continue
		case len(formationSet.Transactions[len(formationSet.Transactions)-1].FileContracts) == 0:
			log.Debug("skipping formation set missing file contract")
			continue
		}
		formationTxn := formationSet.Transactions[len(formationSet.Transactions)-1]
		contractID := formationTxn.V2FileContractID(formationTxn.ID(), 0)
		log := log.Named("v2Formation").With(zap.Stringer("transaction", formationTxn.ID()), zap.Stringer("basis", formationSet.Basis), zap.Stringer("contractID", contractID))

		if err := cm.wallet.BroadcastV2TransactionSet(formationSet.Basis, formationSet.Transactions); err != nil {
			log.Warn("failed to broadcast transaction set", zap.Error(err))
		} else {
			log.Debug("broadcast transaction")
		}
	}

	for _, fcr := range actions.BroadcastV2Revision {
		log := log.With(zap.Stringer("contractID", fcr.Parent.ID)).Named("v2Revision")
		if err := cm.broadcastV2Revision(actions.Basis, fcr, log); err != nil {
			log.Warn("failed to broadcast v2 final revision", zap.Error(err))
		}
	}

	cs := cm.chain.TipState()
	for _, pe := range actions.BroadcastV2Proof {
		log := log.With(zap.Stringer("contractID", pe.V2FileContractElement.ID)).Named("v2Proof")
		if err := cm.broadcastV2StorageProof(cs, actions.Basis, pe, log); err != nil {
			log.Warn("failed to broadcast v2 storage proof", zap.Error(err))
		}
	}

	for _, fce := range actions.BroadcastV2Expiration {
		log := log.With(zap.Stringer("contractID", fce.ID)).Named("v2Expiration")
		if err := cm.broadcastV2Expiration(actions.Basis, fce, log); err != nil {
			log.Warn("failed to broadcast v2 expiration", zap.Error(err))
		}
	}

	if index.Height < ReorgBuffer {
		return nil
	}
	expireHeight := index.Height - ReorgBuffer

	// 6 block buffer for reorg protection
	if err := cm.store.ExpireContractSectors(expireHeight); err != nil {
		return fmt.Errorf("failed to expire contract sectors: %w", err)
	} else if err := cm.store.ExpireV2ContractSectors(expireHeight); err != nil {
		return fmt.Errorf("failed to expire v2 contract sectors: %w", err)
	}
	return nil
}

// buildContractState helper to build state changes from a state update
func buildContractState(tx UpdateStateTx, fces []consensus.FileContractElementDiff, v2Fces []consensus.V2FileContractElementDiff, revert bool, log *zap.Logger) (state StateChanges, _ error) {
	for _, diff := range fces {
		fce, rev, created, resolved, valid := diff.FileContractElement, diff.Revision, diff.Created, diff.Resolved, diff.Valid
		log := log.With(zap.Stringer("contractID", fce.ID))
		if relevant, err := tx.ContractRelevant(types.FileContractID(fce.ID)); err != nil {
			return StateChanges{}, fmt.Errorf("failed to check contract relevance: %w", err)
		} else if !relevant {
			continue
		}

		switch {
		case created:
			state.Confirmed = append(state.Confirmed, fce)
			log.Debug("confirmed contract")
		case rev != nil:
			if revert {
				state.Revised = append(state.Revised, RevisedContract{
					ID:           fce.ID,
					FileContract: fce.FileContract,
				})
				log.Debug("revised contract", zap.Uint64("current", rev.RevisionNumber), zap.Uint64("revised", fce.FileContract.RevisionNumber))
			} else {
				state.Revised = append(state.Revised, RevisedContract{
					ID:           fce.ID,
					FileContract: *rev,
				})
				log.Debug("revised contract", zap.Uint64("current", fce.FileContract.RevisionNumber), zap.Uint64("revised", rev.RevisionNumber))
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
			return StateChanges{}, fmt.Errorf("unexpected contract state (resolved: %v) (valid: %v) (created: %v) (revised: %v) (contractID: %v)", resolved, valid, created, rev != nil, fce.ID)
		}
	}

	for _, diff := range v2Fces {
		fce, rev, res, created := diff.V2FileContractElement, diff.Revision, diff.Resolution, diff.Created
		log := log.With(zap.Stringer("contractID", fce.ID))
		if relevant, err := tx.V2ContractRelevant(types.FileContractID(fce.ID)); err != nil {
			return StateChanges{}, fmt.Errorf("failed to check contract relevance: %w", err)
		} else if !relevant {
			continue
		}

		if created {
			state.ConfirmedV2 = append(state.ConfirmedV2, fce)
			log.Debug("confirmed v2 contract")
		}

		if rev != nil {
			if revert {
				state.RevisedV2 = append(state.RevisedV2, RevisedV2Contract{
					ID:             fce.ID,
					V2FileContract: fce.V2FileContract,
				})
				log.Debug("revised contract", zap.Uint64("current", rev.RevisionNumber), zap.Uint64("revised", fce.V2FileContract.RevisionNumber))
			} else {
				log.Debug("revised contract", zap.Uint64("current", fce.V2FileContract.RevisionNumber), zap.Uint64("revised", rev.RevisionNumber))
				state.RevisedV2 = append(state.RevisedV2, RevisedV2Contract{
					ID:             fce.ID,
					V2FileContract: *rev,
				})
			}
		}

		if res != nil {
			switch res := res.(type) {
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
				return StateChanges{}, fmt.Errorf("unexpected contract resolution type: %T", res)
			}
		}
	}
	return
}

// UpdateChainState updates the state of the contracts on chain
func (cm *Manager) UpdateChainState(tx UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	log := cm.log.Named("updateChainState")

	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			ID:     cru.Block.ID(),
			Height: cru.State.Index.Height + 1,
		}

		// revert contract state changes
		state, err := buildContractState(tx, cru.FileContractElementDiffs(), cru.V2FileContractElementDiffs(), true, log.Named("revert").With(zap.Stringer("index", revertedIndex)))
		if err != nil {
			return fmt.Errorf("failed to build contract state: %w", err)
		} else if err := tx.RevertContracts(revertedIndex, state); err != nil {
			return fmt.Errorf("failed to revert contracts: %w", err)
		} else if err := tx.RevertContractChainIndexElement(revertedIndex); err != nil {
			return fmt.Errorf("failed to revert chain index state element: %w", err)
		} else if err := tx.UpdateChainIndexElementProofs(cru); err != nil {
			return fmt.Errorf("failed to update chain index elements: %w", err)
		} else if err := tx.UpdateContractElementProofs(cru); err != nil {
			return fmt.Errorf("failed to update contract element proofs: %w", err)
		}
	}

	for _, cau := range applied {
		state, err := buildContractState(tx, cau.FileContractElementDiffs(), cau.V2FileContractElementDiffs(), false, log.Named("apply").With(zap.Stringer("index", cau.State.Index)))
		if err != nil {
			return fmt.Errorf("failed to build contract state: %w", err)
		}
		// apply state changes
		if err := tx.ApplyContracts(cau.State.Index, state); err != nil {
			return fmt.Errorf("failed to apply contracts: %w", err)
		}

		if err := tx.UpdateChainIndexElementProofs(cau); err != nil {
			return fmt.Errorf("failed to update chain index elements: %w", err)
		} else if err := tx.UpdateContractElementProofs(cau); err != nil {
			return fmt.Errorf("failed to update contract element proofs: %w", err)
		} else if err := tx.AddContractChainIndexElement(cau.ChainIndexElement()); err != nil {
			return fmt.Errorf("failed to add chain index state element: %w", err)
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
				log.Debug("rejected contracts", zap.Int("count", len(rejectedV1)), zap.Stringers("contracts", rejectedV1))
			}
			if len(rejectedV2) > 0 {
				log.Debug("rejected v2 contracts", zap.Int("count", len(rejectedV2)), zap.Stringers("contracts", rejectedV2))
			}
		}

		// delete any chain index elements outside of the proof window buffer
		if cau.State.Index.Height > chainIndexBuffer {
			minHeight := cau.State.Index.Height - chainIndexBuffer
			if err := tx.DeleteExpiredChainIndexElements(minHeight); err != nil {
				return fmt.Errorf("failed to delete expired chain index elements: %w", err)
			}
		}
	}
	return nil
}
