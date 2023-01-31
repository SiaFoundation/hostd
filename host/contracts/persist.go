package contracts

import (
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// UpdateContractTransaction atomically updates a single contract and its
	// associated sector roots.
	UpdateContractTransaction interface {
		// AppendSector appends a sector root to the end of the contract
		AppendSector(root crypto.Hash) error
		// SwapSectors swaps the sector roots at the given indices.
		SwapSectors(i, j uint64) error
		// TrimSectors removes the last n sector roots from the contract.
		TrimSectors(n uint64) error
		// UpdateSector updates the sector root at the given index.
		UpdateSector(index uint64, newRoot crypto.Hash) error

		// ReviseContract updates the current revision associated with a contract.
		ReviseContract(revision SignedRevision) error
	}

	// UpdateStateTransaction atomically updates the contract manager's state.
	UpdateStateTransaction interface {
		ApplyContractFormation(types.FileContractID) error
		ApplyFinalRevision(types.FileContractID, types.FileContractRevision) error
		ApplyContractResolution(types.FileContractID, types.StorageProof) error

		RevertFormationConfirmed(types.FileContractID) error
		RevertFinalRevision(types.FileContractID) error
		RevertContractResolution(types.FileContractID) error

		SetLastChangeID(modules.ConsensusChangeID) error
	}

	// A ContractStore stores contracts for the host. It also updates stored
	// contracts and determines which contracts need lifecycle actions.
	ContractStore interface {
		LastContractChange() (id modules.ConsensusChangeID, err error)
		// Contract returns the contract with the given ID.
		Contract(types.FileContractID) (Contract, error)
		// ContractFormationSet returns the formation transaction set for the
		// contract with the given ID.
		ContractFormationSet(types.FileContractID) ([]types.Transaction, error)
		// Add stores the provided contract, should error if the contract
		// already exists in the store.
		AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error
		// RenewContract renews a contract. It is expected that the existing
		// contract will be cleared.
		RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error
		// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
		// are returned.
		SectorRoots(id types.FileContractID, limit, offset uint64) ([]crypto.Hash, error)
		// ContractAction calls contractFn on every contract in the store that
		// needs a lifecycle action performed.
		ContractAction(cc *modules.ConsensusChange, contractFn func(types.FileContractID, LifecycleAction) error) error
		// UpdateContract atomically updates a contract and its sector roots.
		UpdateContract(types.FileContractID, func(UpdateContractTransaction) error) error
		// UpdateContractState atomically updates the contract manager's state.
		UpdateContractState(func(UpdateStateTransaction) error) error
	}
)
