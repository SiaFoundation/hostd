package contracts

import (
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

type (
	// ContractState is the current lifecycle stage of a contract.
	ContractState string

	// A SignedRevision pairs a contract revision with the signatures of the host
	// and renter needed to broadcast the revision.
	SignedRevision struct {
		Revision types.FileContractRevision

		HostKey   types.SiaPublicKey
		RenterKey types.SiaPublicKey

		HostSignature   []byte
		RenterSignature []byte
	}

	// A Contract contains metadata on the current lifecycle stage of a file
	// contract.
	Contract struct {
		SignedRevision

		// FormationTransaction is the transaction created by the host and
		// renter during contract formation. A reference is kept in case it
		// needs to be rebroadcast.
		Confirmed            bool
		FormationTransaction []types.Transaction

		// Error is an error encountered while interacting with the contract.
		// if an error is set, the host may refuse to interact with the
		// contract.
		Error error
		// FormationHeight is the height the contract was formed, should never
		// be 0.
		FormationHeight uint64
		// RevisionHeight is the height the contract was revised or 0 if the
		// contract has not broadcast its final revision.
		RevisionHeight uint64
		// ResolutionHeight is the height the contract was resolved  or 0 if the
		// contract is unresolved.
		ResolutionHeight uint64
		// State is the current lifecycle state of the contract.
		State ContractState
	}
)

var (
	// ContractStateUnresolved is a contract that has not yet been resolved.
	ContractStateUnresolved ContractState = "unresolved"
	// ContractStateRenewed is a contract that has been renewed.
	ContractStateRenewed ContractState = "renewed"
	// ContractStateValid is a contract with a successfully confirmed storage proof.
	ContractStateValid ContractState = "valid"
	// ContractStateMissed is a contract that was resolved after the proof window
	// ended.
	ContractStateMissed ContractState = "missed"
)

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

// ShouldBroadcastTransaction returns true if the host should redbroadcast the
// contract formation transaction. The transaction should be rebroadcast if the
// contract has not been seen on the blockchain, there is not an existing error,
// and the height is before the proof deadline.
func (c *Contract) ShouldBroadcastTransaction(height uint64) bool {
	// if the contract has not been confirmed, and the height is before the
	// proof deadline, attempt to rebroadcast the transaction every 6 blocks
	return !c.Confirmed && c.Error == nil && c.Revision.NewWindowStart-6 > types.BlockHeight(height) && (height-c.FormationHeight)%6 == 0
}

// ShouldBroadcastRevision returns true if the host should broadcast the final
// revision. The final revision should be broadcast if the height is within 6
// blocks of the proof window and the host's current revision number is higher
// than the parent's.
func (c *Contract) ShouldBroadcastRevision(height uint64) bool {
	return c.RevisionHeight == 0 && types.BlockHeight(height) >= c.Revision.NewWindowStart-6 && c.Revision.NewWindowStart > types.BlockHeight(height)
}

// ShouldBroadcastStorageProof returns true if the host should broadcast a contract
// resolution. The contract resolution should be broadcast if the contract is in
// the proof window and has not already been resolved.
func (c *Contract) ShouldBroadcastStorageProof(height uint64) bool {
	// if the current index is past window start and the contract has not been resolved, attempt to resolve it. If the
	// resolution fails, retry every 6 blocks.
	return c.ResolutionHeight == 0 && c.Revision.NewWindowStart <= types.BlockHeight(height) && c.Revision.NewWindowEnd > types.BlockHeight(height) && (types.BlockHeight(height)-c.Revision.NewWindowStart)%6 == 0
}
