package contracts

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// A SectorAction denotes the type of action to be performed on a
// contract sector.
const (
	SectorActionAppend SectorAction = "append"
	SectorActionUpdate SectorAction = "update"
	SectorActionSwap   SectorAction = "swap"
	SectorActionTrim   SectorAction = "trim"
)

// ContractStatus is an enum that indicates the current status of a contract.
const (
	// ContractStatusPending indicates that the contract has been formed but
	// has not yet been confirmed on the blockchain. The contract is still
	// usable, but there is a risk that the contract will never be confirmed.
	ContractStatusPending ContractStatus = iota
	// ContractStatusRejected indicates that the contract formation transaction
	// was never confirmed on the blockchain
	ContractStatusRejected
	// ContractStatusActive indicates that the contract has been confirmed on
	// the blockchain and is currently active.
	ContractStatusActive
	// ContractStatusSuccessful indicates that a storage proof has been
	// confirmed or the contract expired without requiring the host to burn
	// Siacoin (e.g. renewal, unused contracts).
	ContractStatusSuccessful
	// ContractStatusFailed indicates that the contract ended without a storage proof
	// and the host was required to burn Siacoin.
	ContractStatusFailed
)

// V2ContractStatus is an enum that indicates the current status of a v2 contract.
const (
	// V2ContractStatusPending indicates that the contract has been formed but
	// has not yet been confirmed on the blockchain. The contract is still
	// usable, but there is a risk that the contract will never be confirmed.
	V2ContractStatusPending V2ContractStatus = "pending"
	// V2ContractStatusRejected indicates that the contract formation transaction
	// was never confirmed on the blockchain
	V2ContractStatusRejected V2ContractStatus = "rejected"
	// V2ContractStatusActive indicates that the contract has been confirmed on
	// the blockchain and is currently active.
	V2ContractStatusActive V2ContractStatus = "active"
	// V2ContractStatusFinalized indicates that the contract has been finalized.
	V2ContractStatusFinalized V2ContractStatus = "finalized"
	// V2ContractStatusRenewed indicates that the contract has been renewed.
	V2ContractStatusRenewed V2ContractStatus = "renewed"
	// V2ContractStatusSuccessful indicates that a storage proof has been
	// confirmed or the contract expired without requiring the host to burn
	// Siacoin.
	V2ContractStatusSuccessful V2ContractStatus = "successful"
	// V2ContractStatusFailed indicates that the contract ended without a storage proof
	// and the host was required to burn Siacoin.
	V2ContractStatusFailed V2ContractStatus = "failed"
)

// fields that the contracts can be sorted by.
const (
	ContractSortStatus            = "status"
	ContractSortNegotiationHeight = "negotiationHeight"
	ContractSortExpirationHeight  = "expirationHeight"
)

type (
	// A SectorAction denotes the type of action to be performed on a
	// contract sector.
	SectorAction string

	// ContractStatus is an enum that indicates the current status of a contract.
	ContractStatus uint8

	// V2ContractStatus is an enum that indicates the current status of a v2 contract.
	V2ContractStatus string

	// A SignedRevision pairs a contract revision with the signatures of the host
	// and renter needed to broadcast the revision.
	SignedRevision struct {
		Revision types.FileContractRevision `json:"revision"`

		HostSignature   types.Signature `json:"hostSignature"`
		RenterSignature types.Signature `json:"renterSignature"`
	}

	// Usage tracks the usage of a contract's funds.
	Usage struct {
		RPCRevenue       types.Currency `json:"rpc"`
		StorageRevenue   types.Currency `json:"storage"`
		EgressRevenue    types.Currency `json:"egress"`
		IngressRevenue   types.Currency `json:"ingress"`
		RegistryRead     types.Currency `json:"registryRead"`
		RegistryWrite    types.Currency `json:"registryWrite"`
		AccountFunding   types.Currency `json:"accountFunding"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
	}

	// V2Usage tracks the usage of a contract's funds.
	V2Usage struct {
		RPCRevenue       types.Currency `json:"rpc"`
		StorageRevenue   types.Currency `json:"storage"`
		EgressRevenue    types.Currency `json:"egress"`
		IngressRevenue   types.Currency `json:"ingress"`
		AccountFunding   types.Currency `json:"accountFunding"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
	}

	// A V2Contract contains metadata on the current state of a v2 file contract.
	V2Contract struct {
		types.V2FileContract

		ID     types.FileContractID `json:"id"`
		Status V2ContractStatus     `json:"status"`
		Usage  V2Usage              `json:"usage"`

		// NegotiationHeight is the height the contract was negotiated at.
		NegotiationHeight uint64 `json:"negotiationHeight"`
		// RevisionConfirmed is true if the contract revision transaction has
		// been confirmed on the blockchain.
		RevisionConfirmed bool `json:"revisionConfirmed"`
		// FormationConfirmed is true if the contract formation transaction
		// has been confirmed on the blockchain.
		FormationIndex types.ChainIndex `json:"formationIndex"`
		// ResolutionIndex is the height the resolution was confirmed
		// at. If the contract has not been resolved, the field is the zero
		// value.
		ResolutionIndex types.ChainIndex `json:"resolutionHeight"`
		// RenewedTo is the ID of the contract that renewed this contract. If
		// this contract was not renewed, this field is the zero value.
		RenewedTo types.FileContractID `json:"renewedTo"`
		// RenewedFrom is the ID of the contract that this contract renewed. If
		// this contract is not a renewal, the field is the zero value.
		RenewedFrom types.FileContractID `json:"renewedFrom"`
	}

	// A V2FormationTransactionSet contains the formation transaction set for a
	// v2 contract.
	V2FormationTransactionSet struct {
		TransactionSet []types.V2Transaction
		Basis          types.ChainIndex
	}

	// A Contract contains metadata on the current state of a file contract.
	Contract struct {
		SignedRevision

		Status           ContractStatus `json:"status"`
		LockedCollateral types.Currency `json:"lockedCollateral"`
		Usage            Usage          `json:"usage"`

		// NegotiationHeight is the height the contract was negotiated at.
		NegotiationHeight uint64 `json:"negotiationHeight"`
		// FormationConfirmed is true if the contract formation transaction
		// has been confirmed on the blockchain.
		FormationConfirmed bool `json:"formationConfirmed"`
		// RevisionConfirmed is true if the contract revision transaction has
		// been confirmed on the blockchain.
		RevisionConfirmed bool `json:"revisionConfirmed"`
		// ResolutionHeight is the height the storage proof was confirmed
		// at. If the contract has not been resolved, the field is the zero
		// value.
		ResolutionHeight uint64 `json:"resolutionHeight"`
		// RenewedTo is the ID of the contract that renewed this contract. If
		// this contract was not renewed, this field is the zero value.
		RenewedTo types.FileContractID `json:"renewedTo"`
		// RenewedFrom is the ID of the contract that this contract renewed. If
		// this contract is not a renewal, the field is the zero value.
		RenewedFrom types.FileContractID `json:"renewedFrom"`
	}

	// ContractFilter defines the filter criteria for a contract query.
	ContractFilter struct {
		// filters
		Statuses    []ContractStatus       `json:"statuses"`
		ContractIDs []types.FileContractID `json:"contractIDs"`
		RenewedFrom []types.FileContractID `json:"renewedFrom"`
		RenewedTo   []types.FileContractID `json:"renewedTo"`
		RenterKey   []types.PublicKey      `json:"renterKey"`

		MinNegotiationHeight uint64 `json:"minNegotiationHeight"`
		MaxNegotiationHeight uint64 `json:"maxNegotiationHeight"`

		MinExpirationHeight uint64 `json:"minExpirationHeight"`
		MaxExpirationHeight uint64 `json:"maxExpirationHeight"`

		// pagination
		Limit  int `json:"limit"`
		Offset int `json:"offset"`

		// sorting
		SortField string `json:"sortField"`
		SortDesc  bool   `json:"sortDesc"`
	}

	// V2ContractFilter defines the filter criteria for a contract query.
	V2ContractFilter struct {
		// filters
		Statuses    []V2ContractStatus     `json:"statuses"`
		ContractIDs []types.FileContractID `json:"contractIDs"`
		RenewedFrom []types.FileContractID `json:"renewedFrom"`
		RenewedTo   []types.FileContractID `json:"renewedTo"`
		RenterKey   []types.PublicKey      `json:"renterKey"`

		MinNegotiationHeight uint64 `json:"minNegotiationHeight"`
		MaxNegotiationHeight uint64 `json:"maxNegotiationHeight"`

		MinExpirationHeight uint64 `json:"minExpirationHeight"`
		MaxExpirationHeight uint64 `json:"maxExpirationHeight"`

		// pagination
		Limit  int `json:"limit"`
		Offset int `json:"offset"`

		// sorting
		SortField string `json:"sortField"`
		SortDesc  bool   `json:"sortDesc"`
	}

	// A SectorChange defines an action to be performed on a contract's sectors.
	SectorChange struct {
		Action SectorAction
		Root   types.Hash256
		A, B   uint64
	}

	// A ContractUpdater is used to atomically update a contract's sectors
	// and metadata.
	ContractUpdater struct {
		manager *Manager
		store   ContractStore
		log     *zap.Logger

		once sync.Once
		done func() // done is called when the updater is closed.

		contractID    types.FileContractID
		sectorActions []SectorChange
		sectorRoots   []types.Hash256
		oldRoots      []types.Hash256
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

// Add returns u + b
func (a Usage) Add(b Usage) (c Usage) {
	return Usage{
		RPCRevenue:       a.RPCRevenue.Add(b.RPCRevenue),
		StorageRevenue:   a.StorageRevenue.Add(b.StorageRevenue),
		EgressRevenue:    a.EgressRevenue.Add(b.EgressRevenue),
		IngressRevenue:   a.IngressRevenue.Add(b.IngressRevenue),
		AccountFunding:   a.AccountFunding.Add(b.AccountFunding),
		RiskedCollateral: a.RiskedCollateral.Add(b.RiskedCollateral),
		RegistryRead:     a.RegistryRead.Add(b.RegistryRead),
		RegistryWrite:    a.RegistryWrite.Add(b.RegistryWrite),
	}
}

// Sub returns a - b
func (a Usage) Sub(b Usage) (c Usage) {
	return Usage{
		RPCRevenue:       a.RPCRevenue.Sub(b.RPCRevenue),
		StorageRevenue:   a.StorageRevenue.Sub(b.StorageRevenue),
		EgressRevenue:    a.EgressRevenue.Sub(b.EgressRevenue),
		IngressRevenue:   a.IngressRevenue.Sub(b.IngressRevenue),
		AccountFunding:   a.AccountFunding.Sub(b.AccountFunding),
		RiskedCollateral: a.RiskedCollateral.Sub(b.RiskedCollateral),
		RegistryRead:     a.RegistryRead.Sub(b.RegistryRead),
		RegistryWrite:    a.RegistryWrite.Sub(b.RegistryWrite),
	}
}

// Add returns u + b
func (a V2Usage) Add(b V2Usage) (c V2Usage) {
	return V2Usage{
		RPCRevenue:       a.RPCRevenue.Add(b.RPCRevenue),
		StorageRevenue:   a.StorageRevenue.Add(b.StorageRevenue),
		EgressRevenue:    a.EgressRevenue.Add(b.EgressRevenue),
		IngressRevenue:   a.IngressRevenue.Add(b.IngressRevenue),
		AccountFunding:   a.AccountFunding.Add(b.AccountFunding),
		RiskedCollateral: a.RiskedCollateral.Add(b.RiskedCollateral),
	}
}

// Sub returns a - b
func (a V2Usage) Sub(b V2Usage) (c V2Usage) {
	return V2Usage{
		RPCRevenue:       a.RPCRevenue.Sub(b.RPCRevenue),
		StorageRevenue:   a.StorageRevenue.Sub(b.StorageRevenue),
		EgressRevenue:    a.EgressRevenue.Sub(b.EgressRevenue),
		IngressRevenue:   a.IngressRevenue.Sub(b.IngressRevenue),
		AccountFunding:   a.AccountFunding.Sub(b.AccountFunding),
		RiskedCollateral: a.RiskedCollateral.Sub(b.RiskedCollateral),
	}
}

// String returns the string representation of a ContractStatus.
func (c ContractStatus) String() string {
	switch c {
	case ContractStatusPending:
		return "pending"
	case ContractStatusRejected:
		return "rejected"
	case ContractStatusActive:
		return "active"
	case ContractStatusSuccessful:
		return "successful"
	case ContractStatusFailed:
		return "failed"
	default:
		panic("unrecognized contract status") // developer error
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (c ContractStatus) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`%q`, c.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *ContractStatus) UnmarshalJSON(b []byte) error {
	status := strings.Trim(string(b), `"`)
	switch status {
	case "pending":
		*c = ContractStatusPending
	case "rejected":
		*c = ContractStatusRejected
	case "active":
		*c = ContractStatusActive
	case "successful":
		*c = ContractStatusSuccessful
	case "failed":
		*c = ContractStatusFailed
	default:
		return fmt.Errorf("unrecognized contract status: %v", status)
	}
	return nil
}

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
			ParentID:       types.Hash256(sr.Revision.ParentID),
			Signature:      sr.HostSignature[:],
			CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
			PublicKeyIndex: 1,
		},
	}
}

// AppendSector appends a sector to the contract.
func (cu *ContractUpdater) AppendSector(root types.Hash256) {
	cu.sectorActions = append(cu.sectorActions, SectorChange{
		Root:   root,
		Action: SectorActionAppend,
	})
	cu.sectorRoots = append(cu.sectorRoots, root)
}

// SwapSectors swaps the sectors at the given indices.
func (cu *ContractUpdater) SwapSectors(a, b uint64) error {
	if a >= uint64(len(cu.sectorRoots)) || b >= uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector indices %v, %v", a, b)
	}
	cu.sectorActions = append(cu.sectorActions, SectorChange{
		A:      a,
		B:      b,
		Action: SectorActionSwap,
	})
	cu.sectorRoots[a], cu.sectorRoots[b] = cu.sectorRoots[b], cu.sectorRoots[a]
	return nil
}

// TrimSectors removes the last n sectors from the contract.
func (cu *ContractUpdater) TrimSectors(n uint64) error {
	if n > uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector count %v", n)
	}
	cu.sectorActions = append(cu.sectorActions, SectorChange{
		A:      n,
		Action: SectorActionTrim,
	})
	cu.sectorRoots = cu.sectorRoots[:uint64(len(cu.sectorRoots))-n]
	return nil
}

// UpdateSector updates the Merkle root of the sector at the given index.
func (cu *ContractUpdater) UpdateSector(root types.Hash256, i uint64) error {
	if i >= uint64(len(cu.sectorRoots)) {
		return fmt.Errorf("invalid sector index %v", i)
	}
	cu.sectorActions = append(cu.sectorActions, SectorChange{
		Root:   root,
		A:      i,
		Action: SectorActionUpdate,
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
	return rhp2.MetaRoot(cu.sectorRoots)
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
func (cu *ContractUpdater) Commit(revision SignedRevision, usage Usage) error {
	if revision.Revision.ParentID != cu.contractID {
		panic("contract updater used with wrong contract")
	}

	start := time.Now()
	// revise the contract
	err := cu.store.ReviseContract(revision, cu.oldRoots, usage, cu.sectorActions)
	if err != nil {
		return err
	}

	// clear the committed sector actions
	cu.sectorActions = cu.sectorActions[:0]
	// update the roots cache
	cu.manager.setSectorRoots(cu.contractID, cu.sectorRoots)
	cu.log.Debug("contract update committed", zap.String("contractID", revision.Revision.ParentID.String()), zap.Uint64("revision", revision.Revision.RevisionNumber), zap.Duration("elapsed", time.Since(start)))
	return nil
}
