package rhp

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/rhp"
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

	sigHash := rhp.HashRevision(revised)
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

// NewSession creates a new session with a host
func NewSession(ctx context.Context, hostKey types.PublicKey, hostAddr string, cm ChainManager) (*Session, error) {
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
		cm:      cm,
	}, nil
}
