package contracts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

// storageProofSegment returns the segment index for which a storage proof must
// be provided, given a contract and the block at the beginning of its proof
// window.
func storageProofSegment(bid types.BlockID, fcid types.FileContractID, filesize uint64) uint64 {
	if filesize == 0 {
		return 0
	}
	seed := blake2b.Sum256(append(bid[:], fcid[:]...))
	numSegments := filesize / merkle.LeafSize
	if filesize%merkle.LeafSize != 0 {
		numSegments++
	}
	var r uint64
	for i := 0; i < 4; i++ {
		_, r = bits.Div64(r, binary.BigEndian.Uint64(seed[i*8:]), numSegments)
	}
	return r
}

func (cm *ContractManager) buildStorageProof(id types.FileContractID, index uint64) (types.StorageProof, error) {
	sectorIndex := index / merkle.LeavesPerSector
	segmentIndex := index % merkle.LeavesPerSector

	roots, err := cm.SectorRoots(id)
	if err != nil {
		return types.StorageProof{}, err
	}
	root := roots[sectorIndex]
	sector, err := cm.storage.Sector(root)
	if err != nil {
		return types.StorageProof{}, err
	}
	segmentProof := merkle.ConvertProofOrdering(merkle.BuildProof(sector, segmentIndex, segmentIndex+1, nil), segmentIndex)
	sectorProof := merkle.ConvertProofOrdering(merkle.BuildSectorRangeProof(roots, sectorIndex, sectorIndex+1), sectorIndex)
	sp := types.StorageProof{
		ParentID: id,
		HashSet:  append(segmentProof, sectorProof...),
	}
	copy(sp.Segment[:], sector[segmentIndex*merkle.LeafSize:])
	return sp, nil
}

// handleContractAction performs a lifecycle action on a contract.
func (cm *ContractManager) handleContractAction(height uint64, id types.FileContractID) error {
	contract, err := cm.store.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	switch {
	case contract.ShouldBroadcastTransaction(height):
		if err := cm.tpool.AcceptTransactionSet(contract.FormationTransaction); err != nil {
			// TODO: recalc financials
			return fmt.Errorf("failed to broadcast formation txn: %w", err)
		}
	case contract.ShouldBroadcastRevision(height):
		revisionTxn := types.Transaction{
			FileContractRevisions: []types.FileContractRevision{contract.Revision},
			TransactionSignatures: []types.TransactionSignature{
				{
					ParentID:      crypto.Hash(contract.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     contract.RenterSignature[:],
				},
				{
					ParentID:      crypto.Hash(contract.Revision.ParentID),
					CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
					Signature:     contract.HostSignature[:],
				},
			},
		}

		_, max := cm.tpool.FeeEstimate()
		fee := max.Mul64(1000)
		revisionTxn.MinerFees = append(revisionTxn.MinerFees, fee)
		toSign, discard, err := cm.wallet.FundTransaction(&revisionTxn, fee, nil)
		if err != nil {
			return fmt.Errorf("failed to fund revision txn: %w", err)
		}
		defer discard()
		if err := cm.wallet.SignTransaction(&revisionTxn, toSign); err != nil {
			return fmt.Errorf("failed to sign revision txn: %w", err)
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{revisionTxn}); err != nil {
			return fmt.Errorf("failed to broadcast revision txn: %w", err)
		}
	case contract.ShouldBroadcastStorageProof(height):
		block, ok := cm.consensus.BlockAtHeight(contract.Revision.NewWindowStart - 1)
		if !ok {
			return errors.New("failed to get block for storage proof")
		}
		// get the
		index := storageProofSegment(block.ID(), contract.Revision.ParentID, contract.Revision.NewFileSize)
		sp, err := cm.buildStorageProof(contract.Revision.ParentID, index)
		if err != nil {
			return fmt.Errorf("failed to build storage proof: %w", err)
		}

		// TODO: consider cost of proof submission and build proof.
		resolutionTxn := types.Transaction{
			StorageProofs: []types.StorageProof{sp},
		}
		_, max := cm.tpool.FeeEstimate()
		fee := max.Mul64(1000)
		resolutionTxn.MinerFees = append(resolutionTxn.MinerFees, fee)
		toSign, discard, err := cm.wallet.FundTransaction(&resolutionTxn, fee, nil)
		if err != nil {
			return fmt.Errorf("failed to fund resolution txn: %w", err)
		}
		defer discard()
		if err := cm.wallet.SignTransaction(&resolutionTxn, toSign); err != nil {
			return fmt.Errorf("failed to sign resolution txn: %w", err)
		} else if err := cm.tpool.AcceptTransactionSet([]types.Transaction{resolutionTxn}); err != nil {
			return fmt.Errorf("failed to broadcast resolution txn: %w", err)
		}
	}
	return nil
}
