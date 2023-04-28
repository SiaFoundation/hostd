package contracts

import (
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
)

type (
	// UpdateContractTransaction atomically updates a single contract and its
	// associated sector roots.
	UpdateContractTransaction interface {
		// AppendSector appends a sector root to the end of the contract
		AppendSector(root types.Hash256) error
		// SwapSectors swaps the sector roots at the given indices.
		SwapSectors(i, j uint64) error
		// TrimSectors removes the last n sector roots from the contract.
		TrimSectors(n int) error
		// UpdateSector updates the sector root at the given index.
		UpdateSector(index uint64, newRoot types.Hash256) error

		// AddUsage adds the additional usage costs to the contract.
		AddUsage(Usage) error
		// ReviseContract updates the current revision associated with a contract.
		ReviseContract(SignedRevision) error
	}

	// UpdateStateTransaction atomically updates the contract manager's state.
	UpdateStateTransaction interface {
		ContractRelevant(types.FileContractID) (bool, error)

		SetStatus(types.FileContractID, ContractStatus) error
		ConfirmFormation(types.FileContractID) error
		ConfirmRevision(types.FileContractRevision) error
		ConfirmResolution(id types.FileContractID, height uint64) error

		RevertFormation(types.FileContractID) error
		RevertRevision(types.FileContractID) error
		RevertResolution(types.FileContractID) error
	}

	// A ContractStore stores contracts for the host. It also updates stored
	// contracts and determines which contracts need lifecycle actions.
	ContractStore interface {
		LastContractChange() (id modules.ConsensusChangeID, err error)
		// Contracts returns a paginated list of contracts sorted by expiration
		// asc.
		Contracts(ContractFilter) ([]Contract, int, error)
		// Contract returns the contract with the given ID.
		Contract(types.FileContractID) (Contract, error)
		// ContractFormationSet returns the formation transaction set for the
		// contract with the given ID.
		ContractFormationSet(types.FileContractID) ([]types.Transaction, error)
		SetContractStatus(types.FileContractID, ContractStatus) error
		// Add stores the provided contract, should error if the contract
		// already exists in the store.
		AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage, negotationHeight uint64) error
		// RenewContract renews a contract. It is expected that the existing
		// contract will be cleared.
		RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, initialUsage Usage, negotationHeight uint64) error
		// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
		// are returned.
		SectorRoots(id types.FileContractID, limit, offset uint64) ([]types.Hash256, error)
		// ContractAction calls contractFn on every contract in the store that
		// needs a lifecycle action performed.
		ContractAction(height uint64, contractFn func(types.FileContractID, uint64, string)) error
		// UpdateContract atomically updates a contract and its sector roots.
		UpdateContract(types.FileContractID, func(UpdateContractTransaction) error) error
		// UpdateContractState atomically updates the contract manager's state.
		UpdateContractState(modules.ConsensusChangeID, uint64, func(UpdateStateTransaction) error) error
		// ExpireContractSectors removes sector roots for any contracts that are
		// past their proof window.
		ExpireContractSectors(height uint64) error
	}
)
