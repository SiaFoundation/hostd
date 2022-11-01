package rhp

import (
	"bytes"
	"strings"
	"time"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
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

	objAccountID accounts.AccountID

	Specifier [16]byte

	// A PriceTable contains the cost of interacting with the host through
	// RHP3 RPCs.
	PriceTable struct {
		// UID is a unique specifier that identifies this price table
		UID Specifier `json:"uid"`

		// Validity is a duration that specifies how long the host guarantees these
		// prices for and are thus considered valid.
		Validity time.Duration `json:"validity"`

		// HostBlockHeight is the block height of the host. This allows the renter
		// to create valid withdrawal messages in case it is not synced yet.
		HostBlockHeight types.BlockHeight `json:"hostblockheight"`

		// UpdatePriceTableCost refers to the cost of fetching a new price table
		// from the host.
		UpdatePriceTableCost types.Currency `json:"updatepricetablecost"`

		// AccountBalanceCost refers to the cost of fetching the balance of an
		// ephemeral account.
		AccountBalanceCost types.Currency `json:"accountbalancecost"`

		// FundAccountCost refers to the cost of funding an ephemeral account on the
		// host.
		FundAccountCost types.Currency `json:"fundaccountcost"`

		// LatestRevisionCost refers to the cost of asking the host for the latest
		// revision of a contract.
		LatestRevisionCost types.Currency `json:"latestrevisioncost"`

		// SubscriptionMemoryCost is the cost of storing a byte of data for
		// SubscriptionPeriod time.
		SubscriptionMemoryCost types.Currency `json:"subscriptionmemorycost"`

		// SubscriptionNotificationCost is the cost of a single notification on top
		// of what is charged for bandwidth.
		SubscriptionNotificationCost types.Currency `json:"subscriptionnotificationcost"`

		// MDM related costs
		//
		// InitBaseCost is the amount of cost that is incurred when an MDM program
		// starts to run. This doesn't include the memory used by the program data.
		// The total cost to initialize a program is calculated as
		// InitCost = InitBaseCost + MemoryTimeCost * Time
		InitBaseCost types.Currency `json:"initbasecost"`

		// MemoryTimeCost is the amount of cost per byte per time that is incurred
		// by the memory consumption of the program.
		MemoryTimeCost types.Currency `json:"memorytimecost"`

		// Cost values specific to the bandwidth consumption.
		DownloadBandwidthCost types.Currency `json:"downloadbandwidthcost"`
		UploadBandwidthCost   types.Currency `json:"uploadbandwidthcost"`

		// Cost values specific to the DropSectors instruction.
		DropSectorsBaseCost types.Currency `json:"dropsectorsbasecost"`
		DropSectorsUnitCost types.Currency `json:"dropsectorsunitcost"`

		// Cost values specific to the HasSector command.
		HasSectorBaseCost types.Currency `json:"hassectorbasecost"`

		// Cost values specific to the Read instruction.
		ReadBaseCost   types.Currency `json:"readbasecost"`
		ReadLengthCost types.Currency `json:"readlengthcost"`

		// Cost values specific to the RenewContract instruction.
		RenewContractCost types.Currency `json:"renewcontractcost"`

		// Cost values specific to the Revision command.
		RevisionBaseCost types.Currency `json:"revisionbasecost"`

		// SwapSectorCost is the cost of swapping 2 full sectors by root.
		SwapSectorCost types.Currency `json:"swapsectorcost"`

		// Cost values specific to the Write instruction.
		WriteBaseCost   types.Currency `json:"writebasecost"`   // per write
		WriteLengthCost types.Currency `json:"writelengthcost"` // per byte written
		WriteStoreCost  types.Currency `json:"writestorecost"`  // per byte / block of additional storage

		// TxnFee estimations.
		TxnFeeMinRecommended types.Currency `json:"txnfeeminrecommended"`
		TxnFeeMaxRecommended types.Currency `json:"txnfeemaxrecommended"`

		// ContractPrice is the additional fee a host charges when forming/renewing
		// a contract to cover the miner fees when submitting the contract and
		// revision to the blockchain.
		ContractPrice types.Currency `json:"contractprice"`

		// CollateralCost is the amount of money per byte the host is promising to
		// lock away as collateral when adding new data to a contract. It's paid out
		// to the host regardless of the outcome of the storage proof.
		CollateralCost types.Currency `json:"collateralcost"`

		// MaxCollateral is the maximum amount of collateral the host is willing to
		// put into a single file contract.
		MaxCollateral types.Currency `json:"maxcollateral"`

		// MaxDuration is the max duration for which the host is willing to form a
		// contract.
		MaxDuration types.BlockHeight `json:"maxduration"`

		// WindowSize is the minimum time in blocks the host requests the
		// renewWindow of a new contract to be.
		WindowSize types.BlockHeight `json:"windowsize"`

		// Registry related fields.
		RegistryEntriesLeft  uint64 `json:"registryentriesleft"`
		RegistryEntriesTotal uint64 `json:"registryentriestotal"`
	}

	// An rpcError may be sent instead of a response object to any RPC.
	rpcError struct {
		Type        Specifier
		Data        []byte // structure depends on Type
		Description string // human-readable error string
	}

	rpcResponse struct {
		err *rpcError
		obj rpcObject
	}

	rpcUpdatePriceTableResponse struct {
		PriceTableJSON []byte
	}

	rpcTrackingResponse struct {
	}

	rpcPayByContractRequest struct {
		ContractID           types.FileContractID
		NewRevisionNumber    uint64
		NewValidProofValues  []types.Currency
		NewMissedProofValues []types.Currency
		RefundAccount        accounts.AccountID
		Signature            []byte
	}

	withdrawalMessage struct {
		AccountID accounts.AccountID
		Expiry    types.BlockHeight
		Amount    types.Currency
		Nonce     [8]byte
	}

	rpcPayByEphemeralAccountRequest struct {
		Message   withdrawalMessage
		Signature crypto.Signature
		Priority  uint64
	}

	rpcPayByContractResponse struct {
		Signature crypto.Signature
	}

	fundReceipt struct {
		Host      types.SiaPublicKey
		Account   accounts.AccountID
		Amount    types.Currency
		Timestamp time.Time
	}

	rpcFundAccountRequest struct {
		Account accounts.AccountID
	}

	rpcFundAccountResponse struct {
		Balance   types.Currency
		Receipt   fundReceipt
		Signature crypto.Signature
	}

	rpcAccountBalanceRequest struct {
		AccountID accounts.AccountID
	}

	rpcAccountBalanceResponse struct {
		Balance types.Currency
	}

	rpcExecuteOutput struct {
		AdditionalCollateral types.Currency
		OutputLength         uint64
		NewMerkleRoot        crypto.Hash
		NewSize              uint64
		Proof                []crypto.Hash
		Error                error
		TotalCost            types.Currency
		FailureRefund        types.Currency
	}

	rpcExecuteProgramRequest struct {
		// FileContractID is the id of the filecontract we would like to modify.
		FileContractID types.FileContractID
		// Program is the program to execute
		Program program
		// ProgramDataLength is the length of the programData following this
		// request.
		ProgramDataLength int
	}
)

var (
	keyEd25519 = newSpecifier("ed25519")
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
