package contracts

import (
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
)

type (
	// UpdateStateTransaction atomically updates the contract manager's state.
	UpdateStateTransaction interface {
		ContractRelevant(types.FileContractID) (bool, error)

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
		// ExpireContract is used to mark a contract as complete. It should only
		// be used on active or pending contracts.
		ExpireContract(types.FileContractID, ContractStatus) error
		// Add stores the provided contract, should error if the contract
		// already exists in the store.
		AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage, negotationHeight uint64) error
		// RenewContract renews a contract. It is expected that the existing
		// contract will be cleared.
		RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, initialUsage Usage, negotationHeight uint64) error
		// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
		// are returned.
		SectorRoots(id types.FileContractID) ([]types.Hash256, error)
		// ContractAction calls contractFn on every contract in the store that
		// needs a lifecycle action performed.
		ContractAction(height uint64, contractFn func(types.FileContractID, uint64, string)) error
		// ReviseContract atomically updates a contract and its associated
		// sector roots.
		ReviseContract(revision SignedRevision, oldRoots []types.Hash256, usage Usage, sectorChanges []SectorChange) error
		// UpdateContractState atomically updates the contract manager's state.
		UpdateContractState(modules.ConsensusChangeID, uint64, func(UpdateStateTransaction) error) error
	}
)
