package rhp

import (
	"bytes"
	"strings"
	"time"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

type (
	// HostSettings are the settings reported by the host
	HostSettings struct {
		AcceptingContracts     bool             `json:"acceptingcontracts"`
		UnlockHash             types.UnlockHash `json:"unlockhash"`
		NetAddress             string           `json:"netaddress"`
		Version                string           `json:"version"`
		MaxDownloadBatchSize   uint64           `json:"maxdownloadbatchsize"`
		MaxDuration            uint64           `json:"maxduration"`
		MaxReviseBatchSize     uint64           `json:"maxrevisebatchsize"`
		RemainingStorage       uint64           `json:"remainingstorage"`
		RevisionNumber         uint64           `json:"revisionnumber"`
		SectorSize             uint64           `json:"sectorsize"`
		TotalStorage           uint64           `json:"totalstorage"`
		WindowSize             uint64           `json:"windowsize"`
		BaseRPCPrice           types.Currency   `json:"baserpcprice"`
		Collateral             types.Currency   `json:"collateral"`
		ContractPrice          types.Currency   `json:"contractprice"`
		DownloadBandwidthPrice types.Currency   `json:"downloadbandwidthprice"`
		MaxCollateral          types.Currency   `json:"maxcollateral"`
		SectorAccessPrice      types.Currency   `json:"sectoraccessprice"`
		StoragePrice           types.Currency   `json:"storageprice"`
		UploadBandwidthPrice   types.Currency   `json:"uploadbandwidthprice"`

		// RHP3 additions
		SiaMuxPort                 string         `json:"siamuxport"`
		EphemeralAccountExpiry     time.Duration  `json:"ephemeralaccountexpiry"`
		MaxEphemeralAccountBalance types.Currency `json:"maxephemeralaccountbalance"`
	}
)

type (
	objCurrency             types.Currency
	objUnlockConditions     types.UnlockConditions
	objSiacoinInput         types.SiacoinInput
	objSiacoinOutput        types.SiacoinOutput
	objSiafundInput         types.SiafundInput
	objSiafundOutput        types.SiafundOutput
	objSiaPublicKey         types.SiaPublicKey
	objFileContract         types.FileContract
	objFileContractRevision types.FileContractRevision
	objStorageProof         types.StorageProof
	objCoveredFields        types.CoveredFields
	objTransactionSignature types.TransactionSignature
	objTransaction          types.Transaction
)

type (
	Specifier [16]byte

	loopKeyExchangeRequest struct {
		PublicKey crypto.X25519PublicKey
		Ciphers   []Specifier
	}

	loopKeyExchangeResponse struct {
		PublicKey crypto.X25519PublicKey
		Signature []byte
		Cipher    Specifier
	}

	// rpcFormContractRequest contains the request parameters for the
	// FormContract and RenewContract RPCs.
	rpcFormContractRequest struct {
		Transactions []types.Transaction
		RenterKey    types.SiaPublicKey
	}

	// rpcRenewAndClearContractRequest contains the request parameters for the
	// RenewAndClearContract RPC.
	rpcRenewAndClearContractRequest struct {
		Transactions           []types.Transaction
		RenterKey              types.SiaPublicKey
		FinalValidProofValues  []types.Currency
		FinalMissedProofValues []types.Currency
	}

	// rpcFormContractAdditions contains the parent transaction, inputs, and
	// outputs added by the host when negotiating a file contract.
	rpcFormContractAdditions struct {
		Parents []types.Transaction
		Inputs  []types.SiacoinInput
		Outputs []types.SiacoinOutput
	}

	// rpcFormContractSignatures contains the signatures for a contract
	// transaction and initial revision. These signatures are sent by both the
	// renter and host during contract formation and renewal.
	rpcFormContractSignatures struct {
		ContractSignatures []types.TransactionSignature
		RevisionSignature  types.TransactionSignature
	}

	// rpcRenewAndClearContractSignatures contains the signatures for a contract
	// transaction, initial revision, and final revision of the contract being
	// renewed. These signatures are sent by both the renter and host during the
	// RenewAndClear RPC.
	rpcRenewAndClearContractSignatures struct {
		ContractSignatures     []types.TransactionSignature
		RevisionSignature      types.TransactionSignature
		FinalRevisionSignature []byte
	}

	// rpcLockRequest contains the request parameters for the Lock RPC.
	rpcLockRequest struct {
		ContractID types.FileContractID
		Signature  []byte
		Timeout    uint64
	}

	// rpcLockResponse contains the response data for the Lock RPC.
	rpcLockResponse struct {
		Acquired     bool
		NewChallenge [16]byte
		Revision     types.FileContractRevision
		Signatures   []types.TransactionSignature
	}

	// rpcReadRequestSection is a section requested in RPCReadRequest.
	rpcReadRequestSection struct {
		MerkleRoot crypto.Hash
		Offset     uint64
		Length     uint64
	}

	// rpcReadRequest contains the request parameters for the Read RPC.
	rpcReadRequest struct {
		Sections    []rpcReadRequestSection
		MerkleProof bool

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		Signature            []byte
	}

	// rpcReadResponse contains the response data for the Read RPC.
	rpcReadResponse struct {
		Signature   []byte
		Data        []byte
		MerkleProof []crypto.Hash
	}

	// rpcSectorRootsRequest contains the request parameters for the SectorRoots RPC.
	rpcSectorRootsRequest struct {
		RootOffset uint64
		NumRoots   uint64

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		Signature            []byte
	}

	// rpcSectorRootsResponse contains the response data for the SectorRoots RPC.
	rpcSectorRootsResponse struct {
		Signature   []byte
		SectorRoots []crypto.Hash
		MerkleProof []crypto.Hash
	}

	// rpcSettingsResponse contains the response data for the SettingsResponse RPC.
	rpcSettingsResponse struct {
		Settings []byte // JSON-encoded hostdb.HostSettings
	}

	// rpcWriteAction is a generic Write action. The meaning of each field
	// depends on the Type of the action.
	rpcWriteAction struct {
		Type Specifier
		A, B uint64
		Data []byte
	}

	// rpcWriteRequest contains the request parameters for the Write RPC.
	rpcWriteRequest struct {
		Actions     []rpcWriteAction
		MerkleProof bool

		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
	}

	// rpcWriteMerkleProof contains the optional Merkle proof for response data
	// for the Write RPC.
	rpcWriteMerkleProof struct {
		OldSubtreeHashes []crypto.Hash
		OldLeafHashes    []crypto.Hash
		NewMerkleRoot    crypto.Hash
	}

	// rpcWriteResponse contains the response data for the Write RPC.
	rpcWriteResponse struct {
		Signature []byte
	}
)

func (s Specifier) String() string {
	return string(bytes.Trim(s[:], "\x00"))
}

// Error implements the error interface.
func (e *rpcError) Error() string {
	return e.Description
}

// Is reports whether this error matches target.
func (e *rpcError) Is(target error) bool {
	return strings.Contains(e.Description, target.Error())
}

// newSpecifier constructs a Specifier from the provided string, which must not
// be longer than 16 bytes.
func newSpecifier(str string) Specifier {
	if len(str) > 16 {
		panic("Specifier is too long")
	}
	var s Specifier
	copy(s[:], str)
	return s
}
