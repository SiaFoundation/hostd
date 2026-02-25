package contracts

import (
	"errors"
	"fmt"
	"strings"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
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

	// A V2Contract contains metadata on the current state of a v2 file contract.
	V2Contract struct {
		types.V2FileContract

		ID     types.FileContractID `json:"id"`
		Status V2ContractStatus     `json:"status"`
		Usage  proto4.Usage         `json:"usage"`

		// NegotiationHeight is the height the contract was negotiated at.
		NegotiationHeight uint64 `json:"negotiationHeight"`
		// FormationIndex is the height at which the contract has been confirmed
		// on the blockchain. If the contract has not been confirmed, the field
		// is the zero value.
		FormationIndex types.ChainIndex `json:"formationIndex"`
		// RevisionConfirmed is true if the latest revision has been confirmed
		// on the blockchain.
		RevisionConfirmed bool `json:"revisionConfirmed"`
		// ResolutionIndex is the height the resolution was confirmed
		// at. If the contract has not been resolved, the field is the zero
		// value.
		ResolutionIndex types.ChainIndex `json:"resolutionIndex"`
		// RenewedTo is the ID of the contract that renewed this contract. If
		// this contract was not renewed, this field is the zero value.
		RenewedTo types.FileContractID `json:"renewedTo"`
		// RenewedFrom is the ID of the contract that this contract renewed. If
		// this contract is not a renewal, the field is the zero value.
		RenewedFrom types.FileContractID `json:"renewedFrom"`
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
	return fmt.Appendf(nil, `%q`, c.String()), nil
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
