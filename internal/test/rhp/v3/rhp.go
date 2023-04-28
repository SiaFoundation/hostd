package rhp

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"net"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/wallet"
)

type (
	accountPayment struct {
		cm ChainManager

		Account    rhpv3.Account
		PrivateKey types.PrivateKey
	}

	contractPayment struct {
		Revision      *rhpv2.ContractRevision
		RefundAccount rhpv3.Account
		RenterKey     types.PrivateKey
	}

	paymentMethod interface {
		Pay(types.Currency) (rhpv3.PaymentMethod, bool)
	}

	// A ChainManager is used to get the current consensus state
	ChainManager interface {
		TipState() consensus.State
	}
)

type (
	// A Session is an RHP3 session with the host
	Session struct {
		hostKey types.PublicKey
		cm      ChainManager
		w       *wallet.SingleAddressWallet
		t       *rhpv3.Transport

		pt rhpv3.HostPriceTable
	}

	// A PaymentSession is an RHP3 session with the host that supports payments
	// either from a contract or an ephemeral account
	PaymentSession struct {
		*Session
		payment paymentMethod
	}
)

func (cp *contractPayment) Pay(amount types.Currency) (rhpv3.PaymentMethod, bool) {
	req, ok := rhpv3.PayByContract(&cp.Revision.Revision, amount, cp.RefundAccount, cp.RenterKey)
	return &req, ok
}

func (ap *accountPayment) Pay(amount types.Currency) (rhpv3.PaymentMethod, bool) {
	height := ap.cm.TipState().Index.Height
	req := rhpv3.PayByEphemeralAccount(ap.Account, amount, height+6, ap.PrivateKey)
	return &req, true
}

func (ps *PaymentSession) processPayment(s *rhpv3.Stream, amount types.Currency) error {
	pm, ok := ps.payment.Pay(amount)
	if !ok {
		return fmt.Errorf("payment method cannot pay %v", amount)
	}
	switch pm := pm.(type) {
	case *rhpv3.PayByEphemeralAccountRequest:
		if err := s.WriteResponse(&rhpv3.PaymentTypeEphemeralAccount); err != nil {
			return fmt.Errorf("failed to write payment request type: %w", err)
		} else if err := s.WriteResponse(pm); err != nil {
			return fmt.Errorf("failed to write request: %w", err)
		}
	case *rhpv3.PayByContractRequest:
		if err := s.WriteResponse(&rhpv3.PaymentTypeContract); err != nil {
			return fmt.Errorf("failed to write payment request type: %w", err)
		} else if err := s.WriteResponse(pm); err != nil {
			return fmt.Errorf("failed to write request: %w", err)
		}
		var hostSigResp rhpv3.PaymentResponse
		if err := s.ReadResponse(&hostSigResp, 4096); err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}
	}

	return nil
}

// RegisterPriceTable registers the price table with the host
func (ps *PaymentSession) RegisterPriceTable() (rhpv3.HostPriceTable, error) {
	stream := ps.t.DialStream()
	defer stream.Close()

	if err := stream.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to write request: %w", err)
	}
	var resp rhpv3.RPCUpdatePriceTableResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to read response: %w", err)
	}

	var pt rhpv3.HostPriceTable
	if err := json.Unmarshal(resp.PriceTableJSON, &pt); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to unmarshal price table: %w", err)
	} else if err := ps.processPayment(stream, pt.UpdatePriceTableCost); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to pay: %w", err)
	}
	var confirmResp rhpv3.RPCPriceTableResponse
	if err := stream.ReadResponse(&confirmResp, 4096); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to read response: %w", err)
	}
	ps.pt = pt
	return pt, nil
}

// FundAccount funds the account with the given amount
func (ps *PaymentSession) FundAccount(account rhpv3.Account, amount types.Currency) (types.Currency, error) {
	stream := ps.t.DialStream()
	defer stream.Close()

	if err := stream.WriteRequest(rhpv3.RPCFundAccountID, &ps.pt.UID); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to write request: %w", err)
	}

	req := &rhpv3.RPCFundAccountRequest{
		Account: account,
	}
	if err := stream.WriteResponse(req); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to write response: %w", err)
	} else if err := ps.processPayment(stream, ps.pt.FundAccountCost.Add(amount)); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to pay: %w", err)
	}

	var resp rhpv3.RPCFundAccountResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to read response: %w", err)
	}
	return resp.Balance, nil
}

// AccountBalance retrieves the balance of the given account
func (ps *PaymentSession) AccountBalance(account rhpv3.Account) (types.Currency, error) {
	stream := ps.t.DialStream()
	defer stream.Close()

	if err := stream.WriteRequest(rhpv3.RPCAccountBalanceID, &ps.pt.UID); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to write request: %w", err)
	} else if err := ps.processPayment(stream, ps.pt.AccountBalanceCost); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to pay: %w", err)
	}

	req := rhpv3.RPCAccountBalanceRequest{
		Account: account,
	}
	if err := stream.WriteResponse(&req); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to write response: %w", err)
	}

	var resp rhpv3.RPCAccountBalanceResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to read response: %w", err)
	}
	return resp.Balance, nil
}

// LatestRevision retrieves the latest revision of the given contract
func (ps *PaymentSession) LatestRevision(contractID types.FileContractID) (types.FileContractRevision, error) {
	stream := ps.t.DialStream()
	defer stream.Close()

	req := rhpv3.RPCLatestRevisionRequest{
		ContractID: contractID,
	}
	if err := stream.WriteRequest(rhpv3.RPCLatestRevisionID, &req); err != nil {
		return types.FileContractRevision{}, fmt.Errorf("failed to write request: %w", err)
	}
	var resp rhpv3.RPCLatestRevisionResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return types.FileContractRevision{}, fmt.Errorf("failed to read response: %w", err)
	} else if err := stream.WriteResponse(&ps.pt.UID); err != nil {
		return types.FileContractRevision{}, fmt.Errorf("failed to write price table uid: %w", err)
	} else if err := ps.processPayment(stream, ps.pt.LatestRevisionCost); err != nil {
		return types.FileContractRevision{}, fmt.Errorf("failed to pay: %w", err)
	}
	return resp.Revision, nil
}

// StoreSector stores the given sector for the given duration
func (ps *PaymentSession) StoreSector(sector *[rhpv2.SectorSize]byte, duration uint64, budget types.Currency) error {
	stream := ps.t.DialStream()
	defer stream.Close()

	req := rhpv3.RPCExecuteProgramRequest{
		Program: []rhpv3.Instruction{
			&rhpv3.InstrStoreSector{
				DataOffset: 0,
				Duration:   duration,
			},
		},
		ProgramData: sector[:],
	}

	if err := stream.WriteRequest(rhpv3.RPCExecuteProgramID, &ps.pt.UID); err != nil {
		return fmt.Errorf("failed to write request: %w", err)
	} else if ps.processPayment(stream, ps.pt.InitBaseCost.Add(budget)) != nil {
		return fmt.Errorf("failed to pay: %w", err)
	} else if err := stream.WriteResponse(&req); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	var cancelToken types.Specifier // unused
	if err := stream.ReadResponse(&cancelToken, 4096); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var resp rhpv3.RPCExecuteProgramResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	} else if resp.Error != nil {
		return fmt.Errorf("failed to append sector: %w", resp.Error)
	}
	return nil
}

// AppendSector appends a sector to the contract
func (ps *PaymentSession) AppendSector(sector *[rhpv2.SectorSize]byte, revision *rhpv2.ContractRevision, renterKey types.PrivateKey, budget types.Currency) error {
	stream := ps.t.DialStream()
	defer stream.Close()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: revision.ID(),
		Program: []rhpv3.Instruction{
			&rhpv3.InstrAppendSector{
				SectorDataOffset: 0,
				ProofRequired:    true,
			},
		},
		ProgramData: sector[:],
	}

	if err := stream.WriteRequest(rhpv3.RPCExecuteProgramID, &ps.pt.UID); err != nil {
		return fmt.Errorf("failed to write request: %w", err)
	} else if ps.processPayment(stream, ps.pt.InitBaseCost.Add(budget)) != nil {
		return fmt.Errorf("failed to pay: %w", err)
	} else if err := stream.WriteResponse(&req); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	var cancelToken types.Specifier // unused
	if err := stream.ReadResponse(&cancelToken, 4096); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var resp rhpv3.RPCExecuteProgramResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	} else if resp.Error != nil {
		return fmt.Errorf("failed to append sector: %w", resp.Error)
	} else if resp.NewSize != revision.Revision.Filesize+rhpv2.SectorSize {
		return fmt.Errorf("unexpected filesize: %v != %v", resp.NewSize, revision.Revision.Filesize+rhpv2.SectorSize)
	}
	//TODO: validate proof
	// revise the contract
	revised := revision.Revision
	revised.RevisionNumber++
	revised.Filesize = resp.NewSize
	revised.FileMerkleRoot = resp.NewMerkleRoot
	revised.ValidProofOutputs = make([]types.SiacoinOutput, len(revision.Revision.ValidProofOutputs))
	revised.MissedProofOutputs = make([]types.SiacoinOutput, len(revision.Revision.MissedProofOutputs))
	for i := range revision.Revision.ValidProofOutputs {
		revised.ValidProofOutputs[i].Address = revision.Revision.ValidProofOutputs[i].Address
		revised.ValidProofOutputs[i].Value = revision.Revision.ValidProofOutputs[i].Value
	}
	for i := range revision.Revision.MissedProofOutputs {
		revised.MissedProofOutputs[i].Address = revision.Revision.MissedProofOutputs[i].Address
		revised.MissedProofOutputs[i].Value = revision.Revision.MissedProofOutputs[i].Value
	}
	// subtract the storage revenue and collateral from the host's missed proof
	// output and add it to the void
	transfer := resp.AdditionalCollateral.Add(resp.FailureRefund)
	revised.MissedProofOutputs[1].Value = revised.MissedProofOutputs[1].Value.Sub(transfer)
	revised.MissedProofOutputs[2].Value = revised.MissedProofOutputs[2].Value.Add(transfer)
	validProofValues := make([]types.Currency, len(revised.ValidProofOutputs))
	for i := range validProofValues {
		validProofValues[i] = revised.ValidProofOutputs[i].Value
	}
	missedProofValues := make([]types.Currency, len(revised.MissedProofOutputs))
	for i := range missedProofValues {
		missedProofValues[i] = revised.MissedProofOutputs[i].Value
	}

	sigHash := hashRevision(revised)
	finalizeReq := rhpv3.RPCFinalizeProgramRequest{
		Signature:         renterKey.SignHash(sigHash),
		RevisionNumber:    revised.RevisionNumber,
		ValidProofValues:  validProofValues,
		MissedProofValues: missedProofValues,
	}
	if err := stream.WriteResponse(&finalizeReq); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	var finalizeResp rhpv3.RPCFinalizeProgramResponse
	if err := stream.ReadResponse(&finalizeResp, 4096); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	revision.Revision = revised
	revision.Signatures = [2]types.TransactionSignature{
		{
			ParentID:       types.Hash256(revised.ParentID),
			PublicKeyIndex: 0,
			CoveredFields: types.CoveredFields{
				FileContractRevisions: []uint64{0},
			},
			Signature: finalizeReq.Signature[:],
		},
		{
			ParentID:       types.Hash256(revised.ParentID),
			PublicKeyIndex: 1,
			CoveredFields: types.CoveredFields{
				FileContractRevisions: []uint64{0},
			},
			Signature: finalizeResp.Signature[:],
		},
	}
	return nil
}

// ReadSector downloads a sector from the host.
func (ps *PaymentSession) ReadSector(root types.Hash256, offset, length uint64, budget types.Currency) ([]byte, error) {
	stream := ps.t.DialStream()
	defer stream.Close()

	programData := make([]byte, 48)
	binary.LittleEndian.PutUint64(programData[0:8], length)
	binary.LittleEndian.PutUint64(programData[8:16], offset)
	copy(programData[16:], root[:])

	req := rhpv3.RPCExecuteProgramRequest{
		Program: []rhpv3.Instruction{
			&rhpv3.InstrReadSector{
				LengthOffset:     0,
				OffsetOffset:     8,
				MerkleRootOffset: 16,
				ProofRequired:    true,
			},
		},
		ProgramData: programData,
	}

	if err := stream.WriteRequest(rhpv3.RPCExecuteProgramID, &ps.pt.UID); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	} else if ps.processPayment(stream, ps.pt.InitBaseCost.Add(budget)) != nil {
		return nil, fmt.Errorf("failed to pay: %w", err)
	} else if err := stream.WriteResponse(&req); err != nil {
		return nil, fmt.Errorf("failed to write response: %w", err)
	}
	var cancelToken types.Specifier // unused
	if err := stream.ReadResponse(&cancelToken, 4096); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var resp rhpv3.RPCExecuteProgramResponse
	if err := stream.ReadResponse(&resp, 4096+length); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	} else if resp.Error != nil {
		return nil, fmt.Errorf("failed to append sector: %w", resp.Error)
	} else if len(resp.Output) != int(length) {
		return nil, fmt.Errorf("unexpected output length: %v != %v", len(resp.Output), length)
	}
	return resp.Output, nil
}

// ScanPriceTable retrieves the host's current price table
func (s *Session) ScanPriceTable() (rhpv3.HostPriceTable, error) {
	stream := s.t.DialStream()
	defer stream.Close()

	if err := stream.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to write request: %w", err)
	}
	var resp rhpv3.RPCUpdatePriceTableResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to read response: %w", err)
	}

	var pt rhpv3.HostPriceTable
	if err := json.Unmarshal(resp.PriceTableJSON, &pt); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("failed to unmarshal price table: %w", err)
	}
	return pt, nil
}

// RenewContract renews an existing contract with the host
func (s *Session) RenewContract(revision *rhpv2.ContractRevision, hostAddr types.Address, renterKey types.PrivateKey, renterPayout, newCollateral types.Currency, endHeight uint64) (rhpv2.ContractRevision, []types.Transaction, error) {
	stream := s.t.DialStream()
	defer stream.Close()

	state := s.cm.TipState()

	pt := s.pt
	if err := stream.WriteRequest(rhpv3.RPCRenewContractID, &pt.UID); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to write request: %w", err)
	} else if pt.UID == (rhpv3.SettingsID{}) {
		// if the price table UID is the zero value, the host sends
		// a temporary price table
		var priceTableResp rhpv3.RPCUpdatePriceTableResponse
		if err := stream.ReadResponse(&priceTableResp, 4096); err != nil {
			return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read response: %w", err)
		}
		if err := json.Unmarshal(priceTableResp.PriceTableJSON, &pt); err != nil {
			return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to unmarshal price table: %w", err)
		}
	}

	clearingValues := make([]types.Currency, len(revision.Revision.ValidProofOutputs))
	for i := range revision.Revision.ValidProofOutputs {
		clearingValues[i] = revision.Revision.ValidProofOutputs[i].Value
	}

	clearingRevision, err := clearingRevision(revision.Revision, clearingValues)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to create clearing revision: %w", err)
	}

	txnFee := types.Siacoins(1)
	renewal, baseCost := prepareContractRenewal(revision.Revision, s.w.Address(), renterKey, renterPayout, newCollateral, s.hostKey, hostAddr, pt, endHeight)
	renewTxn := types.Transaction{
		MinerFees:             []types.Currency{txnFee},
		FileContractRevisions: []types.FileContractRevision{clearingRevision},
		FileContracts:         []types.FileContract{renewal},
	}
	renterCost := rhpv2.ContractRenewalCost(state, renewal, pt.ContractPrice, txnFee, baseCost)
	toSign, release, err := s.w.FundTransaction(&renewTxn, renterCost)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()

	clearingSigHash := hashFinalRevision(clearingRevision, renewal)
	renewReq := &rhpv3.RPCRenewContractRequest{
		TransactionSet:         []types.Transaction{renewTxn},
		RenterKey:              renterKey.PublicKey().UnlockKey(),
		FinalRevisionSignature: renterKey.SignHash(clearingSigHash),
	}
	if err := stream.WriteResponse(renewReq); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to write renew request: %w", err)
	}

	var hostAdditions rhpv3.RPCRenewContractHostAdditions
	if err := stream.ReadResponse(&hostAdditions, 4096); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read host additions response: %w", err)
	} else if !s.hostKey.VerifyHash(clearingSigHash, hostAdditions.FinalRevisionSignature) {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("host final revision signature invalid")
	}
	// add the host's additions to the transaction set
	renewalParents := hostAdditions.Parents
	renewTxn.SiacoinInputs = append(renewTxn.SiacoinInputs, hostAdditions.SiacoinInputs...)
	renewTxn.SiacoinOutputs = append(renewTxn.SiacoinOutputs, hostAdditions.SiacoinOutputs...)

	// sign the transaction
	if err := s.w.SignTransaction(state, &renewTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	renewRevision := initialRevision(&renewTxn, s.hostKey.UnlockKey(), renterKey.PublicKey().UnlockKey())
	renewSigHash := hashRevision(renewRevision)
	renterSig := renterKey.SignHash(renewSigHash)
	renterSigsResp := &rhpv3.RPCRenewSignatures{
		TransactionSignatures: renewTxn.Signatures,
		RevisionSignature: types.TransactionSignature{
			ParentID:       types.Hash256(renewRevision.ParentID),
			PublicKeyIndex: 0,
			CoveredFields: types.CoveredFields{
				FileContractRevisions: []uint64{0},
			},
			Signature: renterSig[:],
		},
	}
	if err := stream.WriteResponse(renterSigsResp); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to write renter signatures: %w", err)
	}

	var hostSigsResp rhpv3.RPCRenewSignatures
	if err := stream.ReadResponse(&hostSigsResp, 4096); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read host signatures: %w", err)
	} else if err := validateHostRevisionSignature(hostSigsResp.RevisionSignature, renewRevision.ParentID, renewSigHash, s.hostKey); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("invalid host revision signature: %w", err)
	}
	return rhpv2.ContractRevision{
		Revision: renewRevision,
		Signatures: [2]types.TransactionSignature{
			renterSigsResp.RevisionSignature,
			hostSigsResp.RevisionSignature,
		},
	}, append(renewalParents, renewTxn), nil
}

// WithContractPayment creates a new payment session for a contract
func (s *Session) WithContractPayment(revision *rhpv2.ContractRevision, renterKey types.PrivateKey, refundAccount rhpv3.Account) *PaymentSession {
	payment := &PaymentSession{
		Session: s,
		payment: &contractPayment{
			Revision:      revision,
			RenterKey:     renterKey,
			RefundAccount: refundAccount,
		},
	}
	return payment
}

// WithAccountPayment creates a new payment session for an account
func (s *Session) WithAccountPayment(account rhpv3.Account, privateKey types.PrivateKey) *PaymentSession {
	payment := &PaymentSession{
		Session: s,
		payment: &accountPayment{
			cm:         s.cm,
			Account:    account,
			PrivateKey: privateKey,
		},
	}
	return payment
}

// Close closes the session
func (s *Session) Close() error {
	return s.t.Close()
}

// clearingRevision returns a revision that locks a contract and sets the missed
// proof outputs to the valid proof outputs.
func clearingRevision(revision types.FileContractRevision, outputValues []types.Currency) (types.FileContractRevision, error) {
	if revision.RevisionNumber == math.MaxUint64 {
		return types.FileContractRevision{}, errors.New("contract is locked")
	} else if len(outputValues) != len(revision.ValidProofOutputs) {
		return types.FileContractRevision{}, errors.New("incorrect number of outputs")
	}

	oldValid := revision.ValidProofOutputs
	revision.ValidProofOutputs = make([]types.SiacoinOutput, len(outputValues))
	for i := range outputValues {
		revision.ValidProofOutputs[i].Address = oldValid[i].Address
		revision.ValidProofOutputs[i].Value = outputValues[i]
	}
	revision.MissedProofOutputs = revision.ValidProofOutputs
	revision.RevisionNumber = math.MaxUint64
	revision.Filesize = 0
	revision.FileMerkleRoot = types.Hash256{}
	return revision, nil
}

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// hashFinalRevision returns the hash of the final revision during contract renewal
func hashFinalRevision(clearing types.FileContractRevision, renewal types.FileContract) types.Hash256 {
	h := types.NewHasher()
	renewal.EncodeTo(h.E)
	clearing.EncodeTo(h.E)
	return h.Sum()
}

// HashRevision returns the hash of rev.
func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func validateHostRevisionSignature(sig types.TransactionSignature, fcID types.FileContractID, sigHash types.Hash256, hostKey types.PublicKey) error {
	switch {
	case sig.ParentID != types.Hash256(fcID):
		return errors.New("revision signature has invalid parent ID")
	case sig.PublicKeyIndex != 1:
		return errors.New("revision signature has invalid public key index")
	case len(sig.Signature) != ed25519.SignatureSize:
		return errors.New("revision signature has invalid length")
	case len(sig.CoveredFields.SiacoinInputs) != 0:
		return errors.New("signature should not cover siacoin inputs")
	case len(sig.CoveredFields.SiacoinOutputs) != 0:
		return errors.New("signature should not cover siacoin outputs")
	case len(sig.CoveredFields.FileContracts) != 0:
		return errors.New("signature should not cover file contract")
	case len(sig.CoveredFields.StorageProofs) != 0:
		return errors.New("signature should not cover storage proofs")
	case len(sig.CoveredFields.SiafundInputs) != 0:
		return errors.New("signature should not cover siafund inputs")
	case len(sig.CoveredFields.SiafundOutputs) != 0:
		return errors.New("signature should not cover siafund outputs")
	case len(sig.CoveredFields.MinerFees) != 0:
		return errors.New("signature should not cover miner fees")
	case len(sig.CoveredFields.ArbitraryData) != 0:
		return errors.New("signature should not cover arbitrary data")
	case len(sig.CoveredFields.Signatures) != 0:
		return errors.New("signature should not cover signatures")
	case len(sig.CoveredFields.FileContractRevisions) != 1:
		return errors.New("signature should cover one file contract revision")
	case sig.CoveredFields.FileContractRevisions[0] != 0:
		return errors.New("signature should cover the first file contract revision")
	case !hostKey.VerifyHash(sigHash, *(*types.Signature)(sig.Signature)):
		return errors.New("revision signature is invalid")
	}
	return nil
}

// InitialRevision returns the first revision of a file contract formation
// transaction.
func initialRevision(formationTxn *types.Transaction, hostPubKey, renterPubKey types.UnlockKey) types.FileContractRevision {
	fc := formationTxn.FileContracts[0]
	return types.FileContractRevision{
		ParentID:         formationTxn.FileContractID(0),
		UnlockConditions: contractUnlockConditions(hostPubKey, renterPubKey),
		FileContract: types.FileContract{
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
			RevisionNumber:     1,
		},
	}
}

// calculateRenewalPayouts calculates the contract payouts for the host.
func calculateRenewalPayouts(fc types.FileContract, newCollateral types.Currency, pt rhpv3.HostPriceTable, endHeight uint64) (types.Currency, types.Currency, types.Currency, types.Currency) {
	// The host gets their contract fee, plus the cost of the data already in the
	// contract, plus their collateral. In the event of a missed payout, the cost
	// and collateral of the data already in the contract is subtracted from the
	// host, and sent to the void instead.
	//
	// However, it is possible for this subtraction to underflow: this can happen if
	// baseCollateral is large and MaxCollateral is small. We cannot simply replace
	// the underflow with a zero, because the host performs the same subtraction and
	// returns an error on underflow. Nor can we increase the valid payout, because
	// the host calculates its collateral contribution by subtracting the contract
	// price and base price from this payout, and we're already at MaxCollateral.
	// Thus the host has conflicting requirements, and renewing the contract is
	// impossible until they change their settings.

	// calculate base price and collateral
	// if the contract height did not increase both prices are zero
	basePrice := pt.RenewContractCost
	var baseCollateral types.Currency
	if contractEnd := uint64(endHeight + pt.WindowSize); contractEnd > fc.WindowEnd {
		timeExtension := uint64(contractEnd - fc.WindowEnd)
		basePrice = basePrice.Add(pt.WriteStoreCost.Mul64(fc.Filesize).Mul64(timeExtension))
		baseCollateral = pt.CollateralCost.Mul64(fc.Filesize).Mul64(timeExtension)
	}

	// calculate payouts
	hostValidPayout := pt.ContractPrice.Add(basePrice).Add(baseCollateral).Add(newCollateral)
	voidMissedPayout := basePrice.Add(baseCollateral)
	if hostValidPayout.Cmp(voidMissedPayout) < 0 {
		// TODO: detect this elsewhere
		panic("host's settings are unsatisfiable")
	}
	hostMissedPayout := hostValidPayout.Sub(voidMissedPayout)
	return hostValidPayout, hostMissedPayout, voidMissedPayout, basePrice
}

// NOTE: due to a bug in the transaction validation code, calculating payouts
// is way harder than it needs to be. Tax is calculated on the post-tax
// contract payout (instead of the sum of the renter and host payouts). So the
// equation for the payout is:
//
//	   payout = renterPayout + hostPayout + payout*tax
//	∴  payout = (renterPayout + hostPayout) / (1 - tax)
//
// This would work if 'tax' were a simple fraction, but because the tax must
// be evenly distributed among siafund holders, 'tax' is actually a function
// that multiplies by a fraction and then rounds down to the nearest multiple
// of the siafund count. Thus, when inverting the function, we have to make an
// initial guess and then fix the rounding error.
func taxAdjustedPayout(target types.Currency) types.Currency {
	// compute initial guess as target * (1 / 1-tax); since this does not take
	// the siafund rounding into account, the guess will be up to
	// types.SiafundCount greater than the actual payout value.
	guess := target.Mul64(1000).Div64(961)

	// now, adjust the guess to remove the rounding error. We know that:
	//
	//   (target % types.SiafundCount) == (payout % types.SiafundCount)
	//
	// therefore, we can simply adjust the guess to have this remainder as
	// well. The only wrinkle is that, since we know guess >= payout, if the
	// guess remainder is smaller than the target remainder, we must subtract
	// an extra types.SiafundCount.
	//
	// for example, if target = 87654321 and types.SiafundCount = 10000, then:
	//
	//   initial_guess  = 87654321 * (1 / (1 - tax))
	//                  = 91211572
	//   target % 10000 =     4321
	//   adjusted_guess = 91204321

	mod64 := func(c types.Currency, v uint64) types.Currency {
		var r uint64
		if c.Hi < v {
			_, r = bits.Div64(c.Hi, c.Lo, v)
		} else {
			_, r = bits.Div64(0, c.Hi, v)
			_, r = bits.Div64(r, c.Lo, v)
		}
		return types.NewCurrency64(r)
	}
	sfc := (consensus.State{}).SiafundCount()
	tm := mod64(target, sfc)
	gm := mod64(guess, sfc)
	if gm.Cmp(tm) < 0 {
		guess = guess.Sub(types.NewCurrency64(sfc))
	}
	return guess.Add(tm).Sub(gm)
}

func prepareContractRenewal(currentRevision types.FileContractRevision, renterAddress types.Address, renterKey types.PrivateKey, renterPayout, newCollateral types.Currency, hostKey types.PublicKey, hostAddr types.Address, host rhpv3.HostPriceTable, endHeight uint64) (types.FileContract, types.Currency) {
	hostValidPayout, hostMissedPayout, voidMissedPayout, basePrice := calculateRenewalPayouts(currentRevision.FileContract, newCollateral, host, endHeight)
	renterPub := renterKey.PublicKey()
	return types.FileContract{
		Filesize:       currentRevision.Filesize,
		FileMerkleRoot: currentRevision.FileMerkleRoot,
		WindowStart:    uint64(endHeight),
		WindowEnd:      uint64(endHeight + host.WindowSize),
		Payout:         taxAdjustedPayout(renterPayout.Add(hostValidPayout)),
		UnlockHash: types.Hash256(types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				{Algorithm: types.SpecifierEd25519, Key: renterPub[:]},
				{Algorithm: types.SpecifierEd25519, Key: hostKey[:]},
			},
		}.UnlockHash()),
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, Address: renterAddress},
			{Value: hostValidPayout, Address: hostAddr},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, Address: renterAddress},
			{Value: hostMissedPayout, Address: hostAddr},
			{Value: voidMissedPayout, Address: types.Address{}},
		},
	}, basePrice
}

// NewSession creates a new session with a host
func NewSession(ctx context.Context, hostKey types.PublicKey, hostAddr string, cm ChainManager, w *wallet.SingleAddressWallet) (*Session, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial host: %w", err)
	}
	t, err := rhpv3.NewRenterTransport(conn, hostKey)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &Session{
		hostKey: hostKey,
		t:       t,
		w:       w,
		cm:      cm,
	}, nil
}
