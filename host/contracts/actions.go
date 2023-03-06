package contracts

import (
	"time"

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
func (cm *ContractManager) handleContractAction(id types.FileContractID, action LifecycleAction) {
	log := cm.log.Named("lifecycle")
	contract, err := cm.store.Contract(id)
	if err != nil {
		log.Error("failed to get contract", zap.String("contract", id.String()), zap.Error(err))
		return
	}
	log.Info("performing contract action", zap.String("action", string(action)), zap.String("contract", id.String()), zap.Uint64("negotiationHeight", contract.NegotiationHeight), zap.Uint64("windowStart", contract.Revision.WindowStart), zap.Uint64("windowEnd", contract.Revision.WindowEnd))
	start := time.Now()

	cs := cm.chain.TipState()

	switch action {
	case ActionBroadcastFormation:
		formationSet, err := cm.store.ContractFormationSet(id)
		if err != nil {
			log.Error("failed to get formation set", zap.String("contract", id.String()), zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(formationSet); err != nil {
			// TODO: recalc financials
			log.Error("failed to broadcast formation transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		log.Info("broadcast formation transaction", zap.String("contract", id.String()), zap.String("transactionID", formationSet[len(formationSet)-1].ID().String()))
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
			log.Error("failed to fund revision transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		defer discard()
		if err := cm.wallet.SignTransaction(cs, &revisionTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			log.Error("failed to sign revision transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{revisionTxn}); err != nil {
			log.Error("failed to broadcast revision transaction", zap.String("contract", id.String()), zap.Error(err))
		}
		log.Info("broadcast revision transaction", zap.String("contract", id.String()), zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.String("transactionID", revisionTxn.ID().String()))
	case ActionBroadcastResolution:
		// get the block before the proof window starts
		windowStart, err := cm.chain.IndexAtHeight(contract.Revision.WindowStart - 1)
		if err != nil {
			log.Error("failed to get chain index at height", zap.String("contract", id.String()), zap.Uint64("height", contract.Revision.WindowStart-1), zap.Error(err))
			return
		}
		// get the proof leaf index
		leafIndex := cs.StorageProofLeafIndex(contract.Revision.Filesize, windowStart, contract.Revision.ParentID)
		sp, err := cm.buildStorageProof(contract.Revision.ParentID, leafIndex)
		if err != nil {
			log.Error("failed to build storage proof", zap.String("contract", id.String()), zap.Error(err))
			return
		}

		// TODO: consider cost of proof submission and build proof.
		fee := cm.tpool.RecommendedFee().Mul64(1000)
		resolutionTxnSet := []types.Transaction{
			{
				// intermediate funding transaction is required by siad because
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
		intermediateToSign, discard, err := cm.wallet.FundTransaction(&resolutionTxnSet[0], fee)
		if err != nil {
			log.Error("failed to fund resolution transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		defer discard()

		// add the intermediate output to the proof transaction
		resolutionTxnSet[1].SiacoinInputs = append(resolutionTxnSet[1].SiacoinInputs, types.SiacoinInput{
			ParentID:         resolutionTxnSet[0].SiacoinOutputID(0),
			UnlockConditions: cm.wallet.UnlockConditions(),
		})
		proofToSign := []types.Hash256{types.Hash256(resolutionTxnSet[1].SiacoinInputs[0].ParentID)}

		if err := cm.wallet.SignTransaction(cs, &resolutionTxnSet[0], intermediateToSign, types.CoveredFields{WholeTransaction: true}); err != nil { // sign the intermediate transaction
			log.Error("failed to sign resolution intermediate transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		} else if err := cm.wallet.SignTransaction(cs, &resolutionTxnSet[1], proofToSign, types.CoveredFields{WholeTransaction: true}); err != nil { // sign the proof transaction
			log.Error("failed to sign resolution transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(resolutionTxnSet); err != nil { // broadcast the transaction set
			log.Error("failed to broadcast resolution transaction set", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		log.Info("broadcast storage proof", zap.String("contract", id.String()), zap.String("transactionID", resolutionTxnSet[1].ID().String()))
	}
	log.Info("contract action completed", zap.String("action", string(action)), zap.String("contract", id.String()), zap.Duration("elapsed", time.Since(start)))
}
