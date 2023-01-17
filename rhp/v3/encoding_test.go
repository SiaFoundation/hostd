package rhp

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"math"
	"testing"
	"time"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

func testEncodingCompat(t *testing.T, hostd rpcObject, siad any) {
	hostdBuf, siadBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

	// marshal hostd
	encoder := newEncoder(hostdBuf)
	writeResponse(encoder, hostd)
	if err := encoder.Flush(); err != nil {
		t.Fatalf("error encoding hostd: %v", err)
	}

	// marshal siad
	if err := modules.RPCWrite(siadBuf, siad); err != nil {
		t.Fatalf("error encoding siad: %v", err)
	}

	// compare
	if !bytes.Equal(hostdBuf.Bytes(), siadBuf.Bytes()) {
		t.Log("hostd:", hostdBuf.Bytes())
		t.Log("siad: ", siadBuf.Bytes())
		t.Fatal("encodings differ")
	}
}

func TestRPCEncodingCompat(t *testing.T) {
	t.Run("Currency", func(t *testing.T) {
		c := types.NewCurrency64(frand.Uint64n(math.MaxUint64))
		testEncodingCompat(t, (*objCurrency)(&c), c)
	})
	t.Run("rpcUpdatePriceTableResponse", func(t *testing.T) {
		hostdPT := PriceTable{
			UID:                          frand.Entropy128(),
			Validity:                     time.Duration(frand.Intn(1e9)),
			HostBlockHeight:              frand.Uint64n(math.MaxUint64),
			UpdatePriceTableCost:         types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			AccountBalanceCost:           types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			FundAccountCost:              types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			LatestRevisionCost:           types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			SubscriptionMemoryCost:       types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			SubscriptionNotificationCost: types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			InitBaseCost:                 types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			MemoryTimeCost:               types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			DownloadBandwidthCost:        types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			UploadBandwidthCost:          types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			DropSectorsBaseCost:          types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			DropSectorsUnitCost:          types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			HasSectorBaseCost:            types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			ReadBaseCost:                 types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			ReadLengthCost:               types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			RenewContractCost:            types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			RevisionBaseCost:             types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			SwapSectorCost:               types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			WriteBaseCost:                types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			WriteLengthCost:              types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			WriteStoreCost:               types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			TxnFeeMinRecommended:         types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			TxnFeeMaxRecommended:         types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			ContractPrice:                types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			CollateralCost:               types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			MaxCollateral:                types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			MaxDuration:                  frand.Uint64n(math.MaxUint64),
			WindowSize:                   frand.Uint64n(math.MaxUint64),
			RegistryEntriesLeft:          frand.Uint64n(math.MaxUint64),
			RegistryEntriesTotal:         frand.Uint64n(math.MaxUint64),
		}
		hbuf, err := json.Marshal(hostdPT)
		if err != nil {
			t.Fatal(err)
		}
		hostd := &rpcUpdatePriceTableResponse{
			PriceTableJSON: hbuf,
		}

		siadPT := modules.RPCPriceTable{
			UID:                          modules.UniqueID(hostdPT.UID),
			Validity:                     hostdPT.Validity,
			HostBlockHeight:              types.BlockHeight(hostdPT.HostBlockHeight),
			UpdatePriceTableCost:         hostdPT.UpdatePriceTableCost,
			AccountBalanceCost:           hostdPT.AccountBalanceCost,
			FundAccountCost:              hostdPT.FundAccountCost,
			LatestRevisionCost:           hostdPT.LatestRevisionCost,
			SubscriptionMemoryCost:       hostdPT.SubscriptionMemoryCost,
			SubscriptionNotificationCost: hostdPT.SubscriptionNotificationCost,
			InitBaseCost:                 hostdPT.InitBaseCost,
			MemoryTimeCost:               hostdPT.MemoryTimeCost,
			DownloadBandwidthCost:        hostdPT.DownloadBandwidthCost,
			UploadBandwidthCost:          hostdPT.UploadBandwidthCost,
			DropSectorsBaseCost:          hostdPT.DropSectorsBaseCost,
			DropSectorsUnitCost:          hostdPT.DropSectorsUnitCost,
			HasSectorBaseCost:            hostdPT.HasSectorBaseCost,
			ReadBaseCost:                 hostdPT.ReadBaseCost,
			ReadLengthCost:               hostdPT.ReadLengthCost,
			RenewContractCost:            hostdPT.RenewContractCost,
			RevisionBaseCost:             hostdPT.RevisionBaseCost,
			SwapSectorCost:               hostdPT.SwapSectorCost,
			WriteBaseCost:                hostdPT.WriteBaseCost,
			WriteLengthCost:              hostdPT.WriteLengthCost,
			WriteStoreCost:               hostdPT.WriteStoreCost,
			TxnFeeMinRecommended:         hostdPT.TxnFeeMinRecommended,
			TxnFeeMaxRecommended:         hostdPT.TxnFeeMaxRecommended,
			ContractPrice:                hostdPT.ContractPrice,
			CollateralCost:               hostdPT.CollateralCost,
			MaxCollateral:                hostdPT.MaxCollateral,
			MaxDuration:                  types.BlockHeight(hostdPT.MaxDuration),
			WindowSize:                   types.BlockHeight(hostdPT.WindowSize),
			RegistryEntriesLeft:          hostdPT.RegistryEntriesLeft,
			RegistryEntriesTotal:         hostdPT.RegistryEntriesTotal,
		}
		sbuf, err := json.Marshal(siadPT)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(hbuf, sbuf) {
			t.Log("hostd json:", string(hbuf))
			t.Log("siad json:", string(sbuf))
			t.Fatal("price table json mismatch")
		}
		siad := modules.RPCUpdatePriceTableResponse{
			PriceTableJSON: sbuf,
		}

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcTrackingResponse", func(t *testing.T) {
		hostd := &rpcTrackingResponse{}
		siad := modules.RPCTrackedPriceTableResponse{}
		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcPayByContractRequest", func(t *testing.T) {
		accountID := accounts.AccountID(frand.Entropy256())
		hostd := &rpcPayByContractRequest{
			ContractID:        types.FileContractID(frand.Entropy256()),
			NewRevisionNumber: frand.Uint64n(math.MaxUint64),
			NewValidProofValues: []types.Currency{
				types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
				types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			},
			NewMissedProofValues: []types.Currency{
				types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
				types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
				types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			},
			RefundAccount: accountID,
			Signature:     frand.Bytes(64),
		}

		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(accountID.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.PayByContractRequest{
			ContractID:           hostd.ContractID,
			NewRevisionNumber:    hostd.NewRevisionNumber,
			NewValidProofValues:  hostd.NewValidProofValues,
			NewMissedProofValues: hostd.NewMissedProofValues,
			RefundAccount:        siadAccountID,
			Signature:            hostd.Signature,
		}

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("WithdrawalMessage", func(t *testing.T) {
		privKey := ed25519.NewKeyFromSeed(frand.Bytes(32))
		pubKey := privKey.Public().(ed25519.PublicKey)

		accountID := accounts.AccountID(*(*[32]byte)(pubKey))
		hostd := &withdrawalMessage{
			AccountID: accountID,
			Expiry:    frand.Uint64n(math.MaxUint64),
			Amount:    types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			Nonce:     *(*[8]byte)(frand.Bytes(8)),
		}

		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(accountID.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.WithdrawalMessage{
			Account: siadAccountID,
			Expiry:  types.BlockHeight(hostd.Expiry),
			Amount:  hostd.Amount,
			Nonce:   hostd.Nonce,
		}

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcPayByEphemeralAccountRequest", func(t *testing.T) {
		privKey := ed25519.NewKeyFromSeed(frand.Bytes(32))
		pubKey := privKey.Public().(ed25519.PublicKey)

		accountID := accounts.AccountID(*(*[32]byte)(pubKey))
		hostd := &rpcPayByEphemeralAccountRequest{
			Message: withdrawalMessage{
				AccountID: accountID,
				Expiry:    frand.Uint64n(math.MaxUint64),
				Amount:    types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
				Nonce:     *(*[8]byte)(frand.Bytes(8)),
			},
		}
		hostdSigHash := hashWithdrawalMessage(hostd.Message)
		hostd.Signature = *(*crypto.Signature)(ed25519.Sign(privKey, hostdSigHash[:]))

		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(accountID.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.PayByEphemeralAccountRequest{
			Message: modules.WithdrawalMessage{
				Account: siadAccountID,
				Expiry:  types.BlockHeight(hostd.Message.Expiry),
				Amount:  hostd.Message.Amount,
				Nonce:   hostd.Message.Nonce,
			},
		}
		siadSigHash := crypto.HashObject(siad.Message)
		if siadSigHash != hostdSigHash {
			t.Fatalf("hash mismatch: %v != %v", siadSigHash, hostdSigHash)
		}
		siad.Signature = crypto.SignHash(siadSigHash, *(*crypto.SecretKey)(privKey))

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcPayByContractResponse", func(t *testing.T) {
		sig := *(*crypto.Signature)(frand.Bytes(64))
		hostd := &rpcPayByContractResponse{
			Signature: sig,
		}
		siad := modules.PayByContractResponse{
			Signature: sig,
		}
		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcFundAccountRequest", func(t *testing.T) {
		accountID := accounts.AccountID(frand.Entropy256())
		hostd := &rpcFundAccountRequest{
			Account: accountID,
		}
		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(accountID.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.FundAccountRequest{
			Account: siadAccountID,
		}

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcFundAccountResponse", func(t *testing.T) {
		privKey := ed25519.NewKeyFromSeed(frand.Bytes(32))
		hostd := &rpcFundAccountResponse{
			Balance: types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			Receipt: fundReceipt{
				Host: types.SiaPublicKey{
					Algorithm: types.SignatureEd25519,
					Key:       privKey.Public().(ed25519.PublicKey),
				},
				Account:   accounts.AccountID(frand.Entropy256()),
				Amount:    types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
				Timestamp: time.Now(),
			},
		}
		hostdSigHash := hashFundReceipt(hostd.Receipt)
		hostd.Signature = *(*crypto.Signature)(ed25519.Sign(privKey, hostdSigHash[:]))
		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(hostd.Receipt.Account.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.FundAccountResponse{
			Balance: hostd.Balance,
			Receipt: modules.Receipt{
				Host:      hostd.Receipt.Host,
				Account:   siadAccountID,
				Amount:    hostd.Receipt.Amount,
				Timestamp: hostd.Receipt.Timestamp.Unix(),
			},
		}
		siadSigHash := crypto.HashObject(siad.Receipt)
		if siadSigHash != hostdSigHash {
			t.Fatalf("hash mismatch: %v != %v", siadSigHash, hostdSigHash)
		}
		siad.Signature = crypto.SignHash(siadSigHash, *(*crypto.SecretKey)(privKey))

		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcAccountBalanceRequest", func(t *testing.T) {
		hostd := &rpcAccountBalanceRequest{
			AccountID: accounts.AccountID(frand.Entropy256()),
		}
		var siadAccountID modules.AccountID
		if err := siadAccountID.LoadString(hostd.AccountID.String()); err != nil {
			t.Fatal(err)
		}
		siad := modules.AccountBalanceRequest{
			Account: siadAccountID,
		}
		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcAccountBalanceResponse", func(t *testing.T) {
		hostd := &rpcAccountBalanceResponse{
			Balance: types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
		}
		siad := modules.AccountBalanceResponse{
			Balance: hostd.Balance,
		}
		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcExecuteProgramRequest", func(t *testing.T) {
		hostd := &rpcExecuteProgramRequest{
			FileContractID: types.FileContractID(frand.Entropy256()),
			Program: program{
				Instructions: []instruction{
					&instrAppendSector{
						SectorDataOffset: 0,
						ProofRequired:    true,
					},
				},
				RequiresContract:     true,
				RequiresFinalization: true,
			},
			ProgramDataLength: frand.Intn(1e6),
		}

		pb := modules.NewProgramBuilder(&modules.RPCPriceTable{}, 0)
		if err := pb.AddAppendInstruction(make([]byte, 4096), true, 0); err != nil {
			t.Fatal(err)
		}
		prog, _ := pb.Program()
		siad := modules.RPCExecuteProgramRequest{
			FileContractID:    hostd.FileContractID,
			Program:           prog,
			ProgramDataLength: uint64(hostd.ProgramDataLength),
		}
		testEncodingCompat(t, hostd, siad)
	})
	t.Run("rpcExecuteOutput", func(t *testing.T) {
		hostd := &rpcExecuteOutput{
			AdditionalCollateral: types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			OutputLength:         frand.Uint64n(math.MaxUint64),
			NewMerkleRoot:        frand.Entropy256(),
			NewSize:              frand.Uint64n(math.MaxUint64),
			Proof: []crypto.Hash{
				frand.Entropy256(),
				frand.Entropy256(),
				frand.Entropy256(),
			},
			TotalCost:     types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
			FailureRefund: types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
		}
		siad := modules.RPCExecuteProgramResponse{
			AdditionalCollateral: hostd.AdditionalCollateral,
			OutputLength:         hostd.OutputLength,
			NewMerkleRoot:        hostd.NewMerkleRoot,
			NewSize:              hostd.NewSize,
			Proof:                hostd.Proof,
			TotalCost:            hostd.TotalCost,
			FailureRefund:        hostd.FailureRefund,
		}
		testEncodingCompat(t, hostd, siad)
	})
}
