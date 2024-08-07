package contracts

import (
	"go.sia.tech/core/types"
)

type (
	// A ContractStore stores contracts for the host. It also updates stored
	// contracts and determines which contracts need lifecycle actions.
	ContractStore interface {
		// ContractActions returns the lifecycle actions for the contract at the
		// given index.
		ContractActions(index types.ChainIndex, revisionBroadcastHeight uint64) (LifecycleActions, error)
		// ContractChainIndexElement returns the chain index element for the given height.
		ContractChainIndexElement(types.ChainIndex) (types.ChainIndexElement, error)

		// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
		// are returned.
		SectorRoots() (map[types.FileContractID][]types.Hash256, error)

		// Contracts returns a paginated list of contracts sorted by expiration
		// asc.
		Contracts(ContractFilter) ([]Contract, int, error)
		// Contract returns the contract with the given ID.
		Contract(types.FileContractID) (Contract, error)
		// AddContract stores the provided contract, should error if the contract
		// already exists in the store.
		AddContract(revision SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage Usage, negotationHeight uint64) error
		// RenewContract renews a contract. It is expected that the existing
		// contract will be cleared.
		RenewContract(renewal SignedRevision, existing SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, initialUsage Usage, negotationHeight uint64) error
		// ReviseContract atomically updates a contract and its associated
		// sector roots.
		ReviseContract(revision SignedRevision, oldRoots []types.Hash256, usage Usage, sectorChanges []SectorChange) error

		// ExpireContractSectors removes sector roots for any contracts that are
		// rejected or past their proof window.
		ExpireContractSectors(height uint64) error

		// V2ContractElement returns the latest v2 state element with the given ID.
		V2ContractElement(types.FileContractID) (types.V2FileContractElement, error)
		// V2Contract returns the v2 contract with the given ID.
		V2Contract(types.FileContractID) (V2Contract, error)
		// AddV2Contract stores the provided contract, should error if the contract
		// already exists in the store.
		AddV2Contract(V2Contract, V2FormationTransactionSet) error
		// RenewV2Contract renews a contract. It is expected that the existing
		// contract will be cleared.
		RenewV2Contract(renewal V2Contract, renewalSet V2FormationTransactionSet, renewedID types.FileContractID, finalRevision types.V2FileContract, finalUsage Usage) error
		// ReviseV2Contract atomically updates a contract and its associated
		// sector roots.
		ReviseV2Contract(id types.FileContractID, revision types.V2FileContract, roots []types.Hash256, usage Usage) error

		// ExpireV2ContractSectors removes sector roots for any v2 contracts that are
		// rejected or past their proof window.
		ExpireV2ContractSectors(height uint64) error
	}
)
