package contracts

import (
	"encoding/json"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// An action determines what lifecycle event should be performed on a contract.
const (
	ActionBroadcastFormation     = "formation"
	ActionReject                 = "reject"
	ActionBroadcastFinalRevision = "revision"
	ActionBroadcastResolution    = "resolve"
	ActionExpire                 = "expire"
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

// processActions performs lifecycle actions on contracts. Triggerd by a
// consensus change, changes are processed in the order they were received.
func (cm *ContractManager) processActions() {
	log := cm.log.Named("lifecycle")
	for {
		var height uint64
		select {
		case height = <-cm.processQueue:
		case <-cm.tg.Done():
			return
		}
		log.Debug("processing actions", zap.Uint64("height", height))
		err := func() error {
			done, err := cm.tg.Add()
			if err != nil {
				return nil
			}
			defer done()

			err = cm.store.ContractAction(height, cm.handleContractAction)
			if err != nil {
				return fmt.Errorf("failed to process contract actions: %w", err)
			} else if err = cm.store.ExpireContractSectors(height); err != nil {
				return fmt.Errorf("failed to expire contract sectors: %w", err)
			}
			return nil
		}()
		if err != nil {
			cm.log.Panic("failed to process contract actions", zap.Error(err), zap.Stack("stack"))
		}
	}
}

// handleContractAction performs a lifecycle action on a contract.
func (cm *ContractManager) handleContractAction(id types.FileContractID, height uint64, action string) {
	log := cm.log.Named("lifecycle")
	contract, err := cm.store.Contract(id)
	if err != nil {
		log.Error("failed to get contract", zap.String("contract", id.String()), zap.Error(err))
		return
	}
	log.Info("performing contract action", zap.String("action", string(action)), zap.String("contract", id.String()), zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Uint64("negotiationHeight", contract.NegotiationHeight), zap.Uint64("windowStart", contract.Revision.WindowStart), zap.Uint64("windowEnd", contract.Revision.WindowEnd))
	start := time.Now()

	cs := cm.chain.TipState()

	switch action {
	case ActionBroadcastFormation:
		if (height-contract.NegotiationHeight)%3 != 0 {
			// debounce formation broadcasts to prevent spamming
			return
		}
		formationSet, err := cm.store.ContractFormationSet(id)
		if err != nil {
			log.Error("failed to get formation set", zap.String("contract", id.String()), zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(formationSet); err != nil {
			log.Error("failed to broadcast formation transaction", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		log.Info("broadcast formation transaction", zap.String("contract", id.String()), zap.String("transactionID", formationSet[len(formationSet)-1].ID().String()))
	case ActionBroadcastFinalRevision:
		if (contract.Revision.WindowStart-height)%3 != 0 {
			// debounce final revision broadcasts to prevent spamming
			return
		}
		revisionTxn := types.Transaction{
			FileContractRevisions: []types.FileContractRevision{contract.Revision},
			Signatures: []types.TransactionSignature{
				{
					ParentID:      types.Hash256(contract.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     contract.RenterSignature[:],
				},
				{
					ParentID:       types.Hash256(contract.Revision.ParentID),
					CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:      contract.HostSignature[:],
					PublicKeyIndex: 1,
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
			return
		}
		log.Info("broadcast revision transaction", zap.String("contract", id.String()), zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.String("transactionID", revisionTxn.ID().String()))
	case ActionBroadcastResolution:
		if (height-contract.Revision.WindowStart)%3 != 0 {
			// debounce resolution broadcasts to prevent spamming
			return
		}
		validPayout, missedPayout := contract.Revision.ValidHostPayout(), contract.Revision.MissedHostPayout()
		if missedPayout.Cmp(validPayout) >= 0 {
			log.Info("skipping storage proof, no benefit to host", zap.String("contract", id.String()), zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()))
			if err := cm.store.SetContractStatus(id, ContractStatusSuccessful); err != nil {
				log.Error("failed to set contract status", zap.String("contract", id.String()), zap.Error(err))
			}
			return
		}

		// get the block before the proof window starts
		windowStart, err := cm.chain.IndexAtHeight(contract.Revision.WindowStart - 1)
		if err != nil {
			log.Error("failed to get chain index at height", zap.String("contract", id.String()), zap.Uint64("height", contract.Revision.WindowStart-1), zap.Error(err))
			return
		}
		// get the proof leaf index
		start := time.Now()
		leafIndex := cs.StorageProofLeafIndex(contract.Revision.Filesize, windowStart, contract.Revision.ParentID)
		sp, err := cm.buildStorageProof(contract.Revision.ParentID, leafIndex)
		if err != nil {
			log.Error("failed to build storage proof", zap.String("contract", id.String()), zap.Error(err))
			return
		}
		log.Debug("built storage proof", zap.String("contract", id.String()), zap.Duration("elapsed", time.Since(start)), zap.Uint64("leafIndex", leafIndex))

		// TODO: consider cost of broadcasting the proof
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
		start = time.Now()
		if err := cm.wallet.SignTransaction(cs, &resolutionTxnSet[0], intermediateToSign, types.CoveredFields{WholeTransaction: true}); err != nil { // sign the intermediate transaction
			log.Error("failed to sign resolution intermediate transaction", zap.String("contractID", id.String()), zap.Error(err))
			return
		} else if err := cm.wallet.SignTransaction(cs, &resolutionTxnSet[1], proofToSign, types.CoveredFields{WholeTransaction: true}); err != nil { // sign the proof transaction
			log.Error("failed to sign resolution transaction", zap.String("contractID", id.String()), zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(resolutionTxnSet); err != nil { // broadcast the transaction set
			buf, _ := json.Marshal(resolutionTxnSet)
			log.Error("failed to broadcast resolution transaction set", zap.String("contractID", id.String()), zap.Error(err), zap.ByteString("transactionSet", buf))
			return
		}
		log.Debug("broadcast storage proof", zap.String("contractID", id.String()), zap.String("transactionID", resolutionTxnSet[1].ID().String()), zap.Duration("elapsed", time.Since(start)))
	case ActionReject:
		if err := cm.store.SetContractStatus(id, ContractStatusRejected); err != nil {
			log.Error("failed to set contract status", zap.String("contract", id.String()), zap.Error(err))
		}
	case ActionExpire:
		validPayout, missedPayout := contract.Revision.ValidHostPayout(), contract.Revision.MissedHostPayout()
		if validPayout.Cmp(missedPayout) > 0 {
			// if the host valid payout is greater than the missed payout, the
			// host lost potential revenue.
			if err := cm.store.SetContractStatus(id, ContractStatusFailed); err != nil {
				log.Error("failed to set contract status", zap.String("contract", id.String()), zap.Error(err))
			}
			log.Error("contract failed, revenue lost", zap.String("contractID", id.String()), zap.Uint64("windowStart", contract.Revision.WindowStart), zap.Uint64("windowEnd", contract.Revision.WindowEnd), zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()))
			return
		}
		// note: this should always be a no-op, but it's good to be explicit
		if err := cm.store.SetContractStatus(id, ContractStatusSuccessful); err != nil {
			log.Error("failed to set contract status", zap.String("contract", id.String()), zap.Error(err))
		}
	default:
		log.Panic("unrecognized contract action", zap.String("action", string(action)), zap.Stack("stack"))
	}
	log.Info("contract action completed", zap.String("action", string(action)), zap.String("contract", id.String()), zap.Duration("elapsed", time.Since(start)))
}
