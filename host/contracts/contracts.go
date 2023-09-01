package contracts

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	// RebroadcastBuffer is the number of blocks after the negotiation height to
	// attempt to rebroadcast the contract.
	RebroadcastBuffer = 36 // 6 hours
	// RevisionSubmissionBuffer number of blocks before the proof window to
	// submit a revision and prevent modification of the contract.
	RevisionSubmissionBuffer = 144 // 24 hours
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
		AccountFunding   types.Currency `json:"accountFunding"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
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

	// A SectorChange defines an action to be performed on a contract's sectors.
	SectorChange struct {
		Action SectorAction
		Root   types.Hash256
		A, B   uint64
	}

	// A ContractUpdater is used to atomically update a contract's sectors
	// and metadata.
	ContractUpdater struct {
		store ContractStore
		log   *zap.Logger

		rootsCache *lru.TwoQueueCache[types.FileContractID, []types.Hash256] // reference to the cache in the contract manager
		once       sync.Once
		done       func() // done is called when the updater is closed.

		sectors       uint64
		contractID    types.FileContractID
		sectorActions []SectorChange
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

// Add returns the sum of two usages.
func (u Usage) Add(b Usage) (c Usage) {
	return Usage{
		RPCRevenue:       u.RPCRevenue.Add(b.RPCRevenue),
		StorageRevenue:   u.StorageRevenue.Add(b.StorageRevenue),
		EgressRevenue:    u.EgressRevenue.Add(b.EgressRevenue),
		IngressRevenue:   u.IngressRevenue.Add(b.IngressRevenue),
		AccountFunding:   u.AccountFunding.Add(b.AccountFunding),
		RiskedCollateral: u.RiskedCollateral.Add(b.RiskedCollateral),
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
	return []byte(fmt.Sprintf(`"%s"`, c.String())), nil
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
func (cu *ContractUpdater) Commit(revision SignedRevision, usage Usage) error {
	if revision.Revision.ParentID != cu.contractID {
		panic("contract updater used with wrong contract")
	}

	start := time.Now()
	// revise the contract
	err := cu.store.ReviseContract(revision, usage, cu.sectors, cu.sectorActions)
	if err == nil {
		// clear the committed sector actions
		cu.sectorActions = cu.sectorActions[:0]
	}
	// update the roots cache
	cu.rootsCache.Add(revision.Revision.ParentID, cu.sectorRoots[:])
	cu.log.Debug("contract update committed", zap.String("contractID", revision.Revision.ParentID.String()), zap.Uint64("revision", revision.Revision.RevisionNumber), zap.Duration("elapsed", time.Since(start)))
	return err
}
