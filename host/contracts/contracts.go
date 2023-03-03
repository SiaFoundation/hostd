package contracts

import (
	"errors"
	"fmt"
	"sync"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

const (
	sectorActionAppend sectorActionType = "append"
	sectorActionUpdate sectorActionType = "update"
	sectorActionSwap   sectorActionType = "swap"
	sectorActionTrim   sectorActionType = "trim"
)

type (
	// A sectorActionType denotes the type of action to be performed on a
	// contract's sectors.
	sectorActionType string

	// A SignedRevision pairs a contract revision with the signatures of the host
	// and renter needed to broadcast the revision.
	SignedRevision struct {
		Revision types.FileContractRevision `json:"revision"`

		HostSignature   types.Signature `json:"hostSignature"`
		RenterSignature types.Signature `json:"renterSignature"`
	}

	// ContractRevenue tracks the usage of a contract's funds.
	ContractRevenue struct {
		RPC            types.Currency `json:"rpc"`
		Storage        types.Currency `json:"storage"`
		Egress         types.Currency `json:"egress"`
		Ingress        types.Currency `json:"ingress"`
		AccountFunding types.Currency `json:"accountFunding"`
	}

	// A Contract contains metadata on the current state of a file contract.
	Contract struct {
		SignedRevision
		ContractRevenue

		LockedCollateral types.Currency `json:"lockedCollateral"`

		// NegotiationHeight is the height the contract was negotiated at.
		NegotiationHeight uint64 `json:"negotiationHeight"`
		// FormationConfirmed is true if the contract formation transaction
		// has been confirmed on the blockchain.
		FormationConfirmed bool `json:"formationConfirmed"`
		// RevisionConfirmed is true if the contract revision transaction has
		// been confirmed on the blockchain.
		RevisionConfirmed bool `json:"revisionConfirmed"`
		// ResolutionConfirmed is true if the contract's resolution has been
		// confirmed on the blockchain.
		ResolutionConfirmed bool `json:"resolutionConfirmed"`
		// RenewedTwo is the ID of the contract that renewed this contract. If
		// this contract was not renewed, this field is the zero value.
		RenewedTo types.FileContractID `json:"renewedTo"`
		// Error is an error encountered while interacting with the contract. if
		// an error is set, the host may refuse to use the contract.
		Error error `json:"error"`
	}

	// A contractSectorAction defines an action to be performed on a contract's
	// sectors.
	contractSectorAction struct {
		Root   types.Hash256
		A, B   uint64
		Action sectorActionType
	}

	// A ContractUpdater is used to atomically update a contract's sectors
	// and metadata.
	ContractUpdater struct {
		store ContractStore

		once sync.Once
		done func() // done is called when the updater is closed.

		sectorActions []contractSectorAction
		sectorRoots   []types.Hash256
	}
)

var (
	// ErrNotFound is returned by the contract store when a contract is not
	// found.
	ErrNotFound = errors.New("contract not found")
	// ErrContractExists is returned by the contract store during formation when
	// the contract already exists.
	ErrContractExists = errors.New("contract already exists")
)

// RenterKey returns the renter's public key.
func (sr SignedRevision) RenterKey() types.PublicKey {
	return *(*types.PublicKey)(sr.Revision.UnlockConditions.PublicKeys[0].Key)
}

// Signatures returns the host and renter transaction signatures for the
// contract revision.
func (sr SignedRevision) Signatures() []types.TransactionSignature {
	return []types.TransactionSignature{
		{
			ParentID:      types.Hash256(sr.Revision.ParentID),
			Signature:     sr.RenterSignature[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
		{
			ParentID:      types.Hash256(sr.Revision.ParentID),
			Signature:     sr.HostSignature[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
	}
}

// AppendSector appends a sector to the contract.
func (cu *ContractUpdater) AppendSector(root types.Hash256) {
	cu.sectorActions = append(cu.sectorActions, contractSectorAction{
		Root:   root,
		Action: sectorActionAppend,
	})
	cu.sectorRoots = append(cu.sectorRoots, root)
}

// SwapSectors swaps the sectors at the given indices.
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

// TrimSectors removes the last n sectors from the contract.
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

// UpdateSector updates the Merkle root of the sector at the given index.
func (cu *ContractUpdater) UpdateSector(root types.Hash256, i uint64) error {
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

// SectorCount returns the number of sectors in the contract.
func (cu *ContractUpdater) SectorCount() uint64 {
	return uint64(len(cu.sectorRoots))
}

// SectorRoot returns the Merkle root of the sector at the given index.
func (cu *ContractUpdater) SectorRoot(i uint64) (types.Hash256, error) {
	if i >= uint64(len(cu.sectorRoots)) {
		return types.Hash256{}, fmt.Errorf("invalid sector index %v", i)
	}
	return cu.sectorRoots[i], nil
}

// MerkleRoot returns the merkle root of the contract's sector roots.
func (cu *ContractUpdater) MerkleRoot() types.Hash256 {
	return rhpv2.MetaRoot(cu.sectorRoots)
}

// SectorRoots returns a copy of the current state of the contract's sector roots.
func (cu *ContractUpdater) SectorRoots() []types.Hash256 {
	return append([]types.Hash256(nil), cu.sectorRoots...)
}

// Close must be called when the contract updater is no longer needed.
func (cu *ContractUpdater) Close() error {
	cu.once.Do(cu.done)
	return nil
}

// Commit atomically applies all changes to the contract store.
func (cu *ContractUpdater) Commit(revision SignedRevision, revenue ContractRevenue) error {
	err := cu.store.UpdateContract(revision.Revision.ParentID, func(tx UpdateContractTransaction) error {
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

		if err := tx.AddRevenue(revenue); err != nil {
			return fmt.Errorf("failed to add revenue: %w", err)
		} else if err := tx.ReviseContract(revision); err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		}
		return nil
	})
	// clear the committed sector actions
	cu.sectorActions = cu.sectorActions[:0]
	return err
}
