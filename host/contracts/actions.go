package contracts

import (
	"fmt"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// An action determines what lifecycle event should be performed on a contract.
const (
	ActionBroadcastFormation     LifecycleAction = "formation"
	ActionBroadcastFinalRevision LifecycleAction = "revision"
	ActionBroadcastResolution    LifecycleAction = "resolution"
)

type (
	// LifecycleAction is an action that should be performed on a contract.
	LifecycleAction string
)

func (cm *ContractManager) buildStorageProof(id types.FileContractID, index uint64) (types.StorageProof, error) {
	sectorIndex := index / rhpv2.LeavesPerSector
	segmentIndex := index % rhpv2.LeavesPerSector

	roots, err := cm.SectorRoots(id, 0, 0)
	if err != nil {
		return types.StorageProof{}, err
	}
	root := roots[sectorIndex]
	sector, err := cm.storage.Read(root)
	if err != nil {
		return types.StorageProof{}, err
	}
	segmentProof := rhpv2.ConvertProofOrdering(rhpv2.BuildProof(sector, segmentIndex, segmentIndex+1, nil), segmentIndex)
	sectorProof := rhpv2.ConvertProofOrdering(rhpv2.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.StorageProof{
		ParentID: id,
		Proof:    append(segmentProof, sectorProof...),
	}
	copy(sp.Leaf[:], sector[segmentIndex*rhpv2.LeafSize:])
	return sp, nil
}

// handleContractAction performs a lifecycle action on a contract.
func (cm *ContractManager) handleContractAction(id types.FileContractID, action LifecycleAction) error {
	cm.log.Debug("performing contract action", zap.String("action", string(action)), zap.String("contract", id.String()))
	contract, err := cm.store.Contract(id)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	cs := consensus.State{Index: types.ChainIndex{Height: cm.blockHeight}}

	switch action {
	case ActionBroadcastFormation:
		formationSet, err := cm.store.ContractFormationSet(id)
		if err != nil {
			return fmt.Errorf("failed to get formation set: %w", err)
		} else if err := cm.tpool.AcceptTransactionSet(formationSet); err != nil {
			// TODO: recalc financials
			return fmt.Errorf("failed to broadcast formation txn: %w", err)
		}
		cm.log.Debug("broadcast formation txn", zap.String("contract", id.String()), zap.String("txn", formationSet[0].ID().String()))
	case ActionBroadcastFinalRevision:
		revisionTxn := types.Transaction{
			FileContractRevisions: []types.FileContractRevision{contract.Revision},
			Signatures: []types.TransactionSignature{
				{
					ParentID:      types.Hash256(contract.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     contract.RenterSignature[:],
				},
				{
					ParentID:      types.Hash256(contract.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     contract.HostSignature[:],
				},
			},
		}

		fee := cm.tpool.RecommendedFee().Mul64(1000)
		revisionTxn.MinerFees = append(revisionTxn.MinerFees, fee)
		toSign, discard, err := cm.wallet.FundTransaction(&revisionTxn, fee)
		if err != nil {
			return fmt.Errorf("failed to fund revision txn: %w", err)
		}
		defer discard()
		if err := cm.wallet.SignTransaction(cs, &revisionTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			return fmt.Errorf("failed to sign revision txn: %w", err)
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{revisionTxn}); err != nil {
			return fmt.Errorf("failed to broadcast revision txn: %w", err)
		}
		cm.log.Debug("broadcast revision txn", zap.String("contract", id.String()), zap.Uint64("finalRevisionNumber", contract.Revision.RevisionNumber), zap.String("txn", revisionTxn.ID().String()))
	case ActionBroadcastResolution:
		windowStart, err := cm.chain.IndexAtHeight(uint64(contract.Revision.WindowStart - 1))
		if err != nil {
			return fmt.Errorf("failed to get chain index at height %v: %w", contract.Revision.WindowStart-1, err)
		}
		// get the proof
		leafIndex := cs.StorageProofLeafIndex(contract.Revision.Filesize, windowStart, contract.Revision.ParentID)
		sp, err := cm.buildStorageProof(contract.Revision.ParentID, leafIndex)
		if err != nil {
			return fmt.Errorf("failed to build storage proof: %w", err)
		}

		// TODO: consider cost of proof submission and build proof.
		resolutionTxn := types.Transaction{
			StorageProofs: []types.StorageProof{sp},
		}
		fee := cm.tpool.RecommendedFee().Mul64(1000)
		resolutionTxn.MinerFees = append(resolutionTxn.MinerFees, fee)
		toSign, discard, err := cm.wallet.FundTransaction(&resolutionTxn, fee)
		if err != nil {
			return fmt.Errorf("failed to fund resolution txn: %w", err)
		}
		defer discard()
		if err := cm.wallet.SignTransaction(cs, &resolutionTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			return fmt.Errorf("failed to sign resolution txn: %w", err)
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{resolutionTxn}); err != nil {
			return fmt.Errorf("failed to broadcast resolution txn: %w", err)
		}
		cm.log.Debug("broadcast storage proof", zap.String("contract", id.String()), zap.String("txn", resolutionTxn.ID().String()))
	}
	return nil
}
