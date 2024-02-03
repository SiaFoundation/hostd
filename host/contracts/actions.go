package contracts

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// An action determines what lifecycle event should be performed on a contract.
const (
	ActionBroadcastFormation     = "formation"
	ActionReject                 = "reject"
	ActionBroadcastFinalRevision = "revision"
	ActionBroadcastResolution    = "resolve"
	ActionExpire                 = "expire"
)

func (cm *ContractManager) buildStorageProof(id types.FileContractID, filesize uint64, index uint64, log *zap.Logger) (types.StorageProof, error) {
	if filesize == 0 {
		return types.StorageProof{
			ParentID: id,
		}, nil
	}

	sectorIndex := index / rhp2.LeavesPerSector
	segmentIndex := index % rhp2.LeavesPerSector

	roots, err := cm.SectorRoots(id, 0, 0)
	if err != nil {
		return types.StorageProof{}, fmt.Errorf("failed to get sector roots: %w", err)
	} else if uint64(len(roots)) < sectorIndex {
		log.Error("failed to build storage proof. invalid root index", zap.Uint64("sectorIndex", sectorIndex), zap.Uint64("segmentIndex", segmentIndex), zap.Int("rootsLength", len(roots)))
		return types.StorageProof{}, fmt.Errorf("invalid root index")
	}
	root := roots[sectorIndex]
	sector, err := cm.storage.Read(root)
	if err != nil {
		log.Error("failed to build storage proof. unable to read sector data", zap.Error(err), zap.Stringer("sectorRoot", root))
		return types.StorageProof{}, fmt.Errorf("failed to read sector data")
	}
	segmentProof := rhp2.ConvertProofOrdering(rhp2.BuildProof(sector, segmentIndex, segmentIndex+1, nil), segmentIndex)
	sectorProof := rhp2.ConvertProofOrdering(rhp2.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.StorageProof{
		ParentID: id,
		Proof:    append(segmentProof, sectorProof...),
	}
	copy(sp.Leaf[:], sector[segmentIndex*rhp2.LeafSize:])
	return sp, nil
}

// processActions performs lifecycle actions on contracts. Triggered by a
// consensus change, changes are processed in the order they were received.
func (cm *ContractManager) processActions() {
	for {
		select {
		case height := <-cm.processQueue:
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
			atomic.StoreUint64(&cm.blockHeight, height)
		case <-cm.tg.Done():
			return
		}
	}
}

// handleContractAction performs a lifecycle action on a contract.
func (cm *ContractManager) handleContractAction(id types.FileContractID, height uint64, action string) {
	log := cm.log.Named("lifecycle").With(zap.String("contractID", id.String()), zap.Uint64("height", height), zap.String("action", action))
	contract, err := cm.store.Contract(id)
	if err != nil {
		log.Error("failed to get contract", zap.Error(err))
		return
	}
	log = log.With(zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.Uint64("size", contract.Revision.Filesize), zap.Stringer("merkleRoot", contract.Revision.FileMerkleRoot), zap.Uint64("scanHeight", cm.chain.TipState().Index.Height))
	log.Debug("performing contract action", zap.Uint64("negotiationHeight", contract.NegotiationHeight), zap.Uint64("windowStart", contract.Revision.WindowStart), zap.Uint64("windowEnd", contract.Revision.WindowEnd))
	start := time.Now()
	cs := cm.chain.TipState()

	switch action {
	case ActionBroadcastFormation:
		if (height-contract.NegotiationHeight)%3 != 0 {
			// debounce formation broadcasts to prevent spamming
			log.Debug("skipping rebroadcast", zap.Uint64("negotiationHeight", contract.NegotiationHeight))
			return
		}
		formationSet, err := cm.store.ContractFormationSet(id)
		if err != nil {
			log.Error("failed to get formation set", zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(formationSet); err != nil {
			log.Error("failed to broadcast formation transaction", zap.Error(err))
			return
		}
		log.Info("rebroadcast formation transaction", zap.String("transactionID", formationSet[len(formationSet)-1].ID().String()))
	case ActionBroadcastFinalRevision:
		if (contract.Revision.WindowStart-height)%3 != 0 {
			// debounce final revision broadcasts to prevent spamming
			log.Debug("skipping revision", zap.Uint64("windowStart", contract.Revision.WindowStart))
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
			log.Error("failed to fund revision transaction", zap.Error(err))
			return
		}
		defer discard()
		if err := cm.wallet.SignTransaction(cs, &revisionTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			log.Error("failed to sign revision transaction", zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{revisionTxn}); err != nil {
			log.Error("failed to broadcast revision transaction", zap.Error(err))
			return
		}
		log.Info("broadcast final revision", zap.Uint64("revisionNumber", contract.Revision.RevisionNumber), zap.String("transactionID", revisionTxn.ID().String()))
	case ActionBroadcastResolution:
		if (height-contract.Revision.WindowStart)%3 != 0 {
			// debounce resolution broadcasts to prevent spamming
			log.Debug("skipping resolution", zap.Uint64("windowStart", contract.Revision.WindowStart))
			return
		}
		validPayout, missedPayout := contract.Revision.ValidHostPayout(), contract.Revision.MissedHostPayout()
		if missedPayout.Cmp(validPayout) >= 0 {
			log.Info("skipping storage proof, no benefit to host", zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()))
			return
		}

		// get the block before the proof window starts
		windowStart, err := cm.chain.IndexAtHeight(contract.Revision.WindowStart - 1)
		if err != nil {
			log.Error("failed to get chain index at height", zap.Uint64("height", contract.Revision.WindowStart-1), zap.Error(err))
			return
		}

		// get the proof leaf index
		leafIndex := cs.StorageProofLeafIndex(contract.Revision.Filesize, windowStart.ID, contract.Revision.ParentID)
		sp, err := cm.buildStorageProof(contract.Revision.ParentID, contract.Revision.Filesize, leafIndex, log.Named("buildStorageProof"))
		if err != nil {
			log.Error("failed to build storage proof", zap.Error(err))
			return
		}

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
			log.Error("failed to fund resolution transaction", zap.Error(err))
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
			log.Error("failed to sign resolution intermediate transaction", zap.Error(err))
			return
		} else if err := cm.wallet.SignTransaction(cs, &resolutionTxnSet[1], proofToSign, types.CoveredFields{WholeTransaction: true}); err != nil { // sign the proof transaction
			log.Error("failed to sign resolution transaction", zap.Error(err))
			return
		} else if err := cm.tpool.AcceptTransactionSet(resolutionTxnSet); err != nil { // broadcast the transaction set
			buf, _ := json.Marshal(resolutionTxnSet)
			log.Error("failed to broadcast resolution transaction set", zap.Error(err), zap.ByteString("transactionSet", buf))
			return
		}
		log.Info("broadcast storage proof", zap.String("transactionID", resolutionTxnSet[1].ID().String()), zap.Duration("elapsed", time.Since(start)))
	case ActionReject:
		if err := cm.store.ExpireContract(id, ContractStatusRejected); err != nil {
			log.Error("failed to set contract status", zap.Error(err))
		}
		log.Info("contract rejected", zap.Uint64("negotiationHeight", contract.NegotiationHeight))
	case ActionExpire:
		validPayout, missedPayout := contract.Revision.ValidHostPayout(), contract.Revision.MissedHostPayout()
		switch {
		case !contract.FormationConfirmed:
			// if the contract was never confirmed, nothing was ever lost or
			// gained
			if err := cm.store.ExpireContract(id, ContractStatusRejected); err != nil {
				log.Error("failed to set contract status", zap.Error(err))
			}
		case validPayout.Cmp(missedPayout) <= 0 || contract.ResolutionHeight != 0:
			// if the host valid payout is less than or equal to the missed
			// payout or if a resolution was confirmed, the contract was
			// successful
			if err := cm.store.ExpireContract(id, ContractStatusSuccessful); err != nil {
				log.Error("failed to set contract status", zap.Error(err))
			}
			payout := validPayout
			if contract.ResolutionHeight != 0 {
				payout = missedPayout
			}
			log.Info("contract successful", zap.String("payout", payout.ExactString()))
		case validPayout.Cmp(missedPayout) > 0 && contract.ResolutionHeight == 0:
			// if the host valid payout is greater than the missed payout and a
			// proof was not broadcast, the contract failed
			if err := cm.store.ExpireContract(id, ContractStatusFailed); err != nil {
				log.Error("failed to set contract status", zap.Error(err))
			}
			cm.alerts.Register(alerts.Alert{
				ID:       frand.Entropy256(),
				Severity: alerts.SeverityWarning,
				Message:  "Contract failed without storage proof",
				Data: map[string]any{
					"contractID":  id,
					"blockHeight": height,
				},
				Timestamp: time.Now(),
			})
			log.Error("contract failed, revenue lost", zap.Uint64("windowStart", contract.Revision.WindowStart), zap.Uint64("windowEnd", contract.Revision.WindowEnd), zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()))
		default:
			log.Panic("unrecognized contract state", zap.Stack("stack"), zap.String("validPayout", validPayout.ExactString()), zap.String("missedPayout", missedPayout.ExactString()), zap.Uint64("resolutionHeight", contract.ResolutionHeight), zap.Bool("formationConfirmed", contract.FormationConfirmed))
		}
	default:
		log.Panic("unrecognized contract action", zap.Stack("stack"))
	}
	log.Debug("contract action completed", zap.Duration("elapsed", time.Since(start)))
}
