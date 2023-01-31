package contracts

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

const (
	sectorActionAppend sectorActionType = "append"
	sectorActionUpdate sectorActionType = "update"
	sectorActionSwap   sectorActionType = "swap"
	sectorActionTrim   sectorActionType = "trim"
)

type (
	sectorActionType string

	// ContractState is the current lifecycle stage of a contract.
	ContractState string

	// A SignedRevision pairs a contract revision with the signatures of the host
	// and renter needed to broadcast the revision.
	SignedRevision struct {
		Revision types.FileContractRevision

		HostSignature   []byte
		RenterSignature []byte
	}

	// A Contract contains metadata on the current lifecycle stage of a file
	// contract.
	Contract struct {
		SignedRevision

		LockedCollateral types.Currency

		// NegotiationHeight is the height the contract was negotiated at.
		NegotiationHeight uint64
		// FormationConfirmed is true if the contract formation transaction
		// has been confirmed on the blockchain.
		FormationConfirmed bool
		// RevisionConfirmed is true if the contract revision transaction has
		// been confirmed on the blockchain.
		RevisionConfirmed bool
		// ResolutionConfirmed is true if the contract's resolution has been
		// confirmed on the blockchain.
		ResolutionConfirmed bool
		// Error is an error encountered while interacting with the contract. if
		// an error is set, the host may refuse to use the contract.
		Error error
		// State is the current lifecycle state of the contract.
		State ContractState
	}

	contractSectorAction struct {
		Root   crypto.Hash
		A, B   uint64
		Action sectorActionType
	}

	ContractUpdater struct {
		store ContractStore

		sectorActions []contractSectorAction
		sectorRoots   []crypto.Hash
	}
)

var (
	// ContractStateUnresolved is a contract that has not yet been resolved.
	ContractStateUnresolved ContractState = "unresolved"
	// ContractStateRenewed is a contract that has been renewed.
	ContractStateRenewed ContractState = "renewed"
	// ContractStateValid is a contract with a successfully confirmed storage
	// proof.
	ContractStateValid ContractState = "valid"
	// ContractStateMissed is a contract that was resolved after the proof
	// window ended.
	ContractStateMissed ContractState = "missed"

	ErrNotFound = errors.New("contract not found")
)

// RenterKey returns the renter's public key.
func (sr SignedRevision) RenterKey() ed25519.PublicKey {
	return ed25519.PublicKey(sr.Revision.UnlockConditions.PublicKeys[0].Key)
}

// Signatures returns the host and renter transaction signatures for the
// contract revision.
func (sr SignedRevision) Signatures() []types.TransactionSignature {
	return []types.TransactionSignature{
		{
			ParentID:      crypto.Hash(sr.Revision.ParentID),
			Signature:     sr.RenterSignature[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
		{
			ParentID:      crypto.Hash(sr.Revision.ParentID),
			Signature:     sr.HostSignature[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
	}
}

func (cu *ContractUpdater) AppendSector(root crypto.Hash) {
	cu.sectorActions = append(cu.sectorActions, contractSectorAction{
		Root:   root,
		Action: sectorActionAppend,
	})
	cu.sectorRoots = append(cu.sectorRoots, root)
}

func (cu *ContractUpdater) SwapSectors(a, b uint64) error {
	if a >= uint64(len(cu.sectorRoots)) || b >= uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector indices %v, %v", a, b)
	}
	cu.sectorActions = append(cu.sectorActions, contractSectorAction{
		A:      a,
		B:      b,
		Action: sectorActionSwap,
	})
	cu.sectorRoots[a], cu.sectorRoots[b] = cu.sectorRoots[b], cu.sectorRoots[a]
	return nil
}

func (cu *ContractUpdater) TrimSectors(n uint64) error {
	if n > uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector count %v", n)
	}
	cu.sectorActions = append(cu.sectorActions, contractSectorAction{
		A:      n,
		Action: sectorActionTrim,
	})
	cu.sectorRoots = cu.sectorRoots[:uint64(len(cu.sectorRoots))-n]
	return nil
}

func (cu *ContractUpdater) UpdateSectors(root crypto.Hash, i uint64) error {
	if i >= uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector index %v", i)
	}
	cu.sectorActions = append(cu.sectorActions, contractSectorAction{
		Root:   root,
		A:      i,
		Action: sectorActionUpdate,
	})
	cu.sectorRoots[i] = root
	return nil
}

func (cu *ContractUpdater) SectorLength() uint64 {
	return uint64(len(cu.sectorRoots))
}

func (cu *ContractUpdater) SectorRoot(i uint64) (crypto.Hash, error) {
	if i >= uint64(len(cu.sectorRoots)) {
		return crypto.Hash{}, fmt.Errorf("invalid sector index %v", i)
	}
	return cu.sectorRoots[i], nil
}

func (cu *ContractUpdater) MerkleRoot() crypto.Hash {
	return merkle.MetaRoot(cu.sectorRoots)
}

// SectorRoots returns a copy of the current state of the contract's sector roots.
func (cu *ContractUpdater) SectorRoots() []crypto.Hash {
	return append([]crypto.Hash(nil), cu.sectorRoots...)
}

func (cu *ContractUpdater) Commit(revision SignedRevision) error {
	return cu.store.UpdateContract(revision.Revision.ParentID, func(tx UpdateContractTransaction) error {
		for i, action := range cu.sectorActions {
			switch action.Action {
			case sectorActionAppend:
				if err := tx.AppendSector(action.Root); err != nil {
					return fmt.Errorf("failed to apply action %v: append sector: %w", i, err)
				}
			case sectorActionUpdate:
				if err := tx.UpdateSector(action.A, action.Root); err != nil {
					return fmt.Errorf("failed to update sector: %w", err)
				}
			case sectorActionSwap:
				if err := tx.SwapSectors(action.A, action.B); err != nil {
					return fmt.Errorf("failed to swap sectors: %w", err)
				}
			case sectorActionTrim:
				if err := tx.TrimSectors(action.A); err != nil {
					return fmt.Errorf("failed to trim sectors: %w", err)
				}
			}
		}

		if err := tx.ReviseContract(revision); err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		}
		return nil
	})
}
