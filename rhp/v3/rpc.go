package rhp

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
)

var (
	// ErrTxnMissingContract is returned if the transaction set does not contain
	// any transactions or if the transaction does not contain exactly one
	// contract.
	ErrTxnMissingContract = errors.New("transaction set does not contain a file contract")
	// ErrHostInternalError is returned if the host encountered an error during
	// an RPC that doesn't need to be broadcast to the renter (e.g. insufficient
	// funds).
	ErrHostInternalError = errors.New("unable to form contract")
	// ErrInvalidRenterSignature is returned when a contract's renter signature
	// is invalid.
	ErrInvalidRenterSignature = errors.New("invalid renter signature")
)

func readRequest(s *rhpv3.Stream, req rhpv3.ProtocolObject, maxLen uint64, timeout time.Duration) error {
	s.SetDeadline(time.Now().Add(30 * time.Second))
	if err := s.ReadRequest(req, maxLen); err != nil {
		return err
	}
	s.SetDeadline(time.Time{})
	return nil
}

// handleRPCPriceTable sends the host's price table to the renter.
func (sh *SessionHandler) handleRPCPriceTable(s *rhpv3.Stream) error {
	pt, err := sh.PriceTable()
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get price table: %w", err)
	}
	buf, err := json.Marshal(pt)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to marshal price table: %w", err)
	}

	resp := &rhpv3.RPCUpdatePriceTableResponse{
		PriceTableJSON: buf,
	}
	if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send price table: %w", err)
	}

	// process the payment, catch connection closed errors since the renter
	// likely did not intend to pay
	budget, err := sh.processPayment(s)
	if errors.Is(err, mux.ErrPeerClosedStream) || errors.Is(err, mux.ErrPeerClosedConn) {
		return nil
	} else if err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.UpdatePriceTableCost); err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to pay %v for price table: %w", pt.UpdatePriceTableCost, err))
	}

	// register the price table for future use
	sh.priceTables.Register(pt)
	if err := budget.Commit(); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	} else if err := s.WriteResponse(&rhpv3.RPCPriceTableResponse{}); err != nil {
		return fmt.Errorf("failed to send tracking response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCFundAccount(s *rhpv3.Stream) error {
	// read the price table ID from the stream
	pt, err := sh.readPriceTable(s)
	if err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to read price table: %w", err))
	}

	// read the fund request from the stream
	var fundReq rhpv3.RPCFundAccountRequest
	if err := readRequest(s, &fundReq, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read fund account request: %w", err)
	}

	// process the payment for funding the account
	budget, err := sh.processPayment(s)
	if err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	// subtract the cost of funding the account
	if err := budget.Spend(pt.FundAccountCost); err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to pay %v for fund account: %w", pt.FundAccountCost, err))
	}

	// commit the budget and fund the account with the remaining balance
	// note: may need to add an atomic transfer?
	fundAmount := budget.Empty()
	if err := budget.Commit(); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	balance, err := sh.accounts.Credit(fundReq.Account, fundAmount)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to credit account: %w", err)
	}

	fundResp := &rhpv3.RPCFundAccountResponse{
		Balance: balance,
		Receipt: rhpv3.FundAccountReceipt{
			Host:      sh.HostKey(),
			Account:   fundReq.Account,
			Amount:    fundAmount,
			Timestamp: time.Now(),
		},
	}
	h := types.NewHasher()
	fundResp.Receipt.EncodeTo(h.E)
	fundResp.Signature = sh.privateKey.SignHash(h.Sum())

	// send the response
	if err := s.WriteResponse(fundResp); err != nil {
		return fmt.Errorf("failed to send fund account response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCAccountBalance(s *rhpv3.Stream) error {
	// get the price table to use for payment
	pt, err := sh.readPriceTable(s)
	if err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to read price table: %w", err))
	}

	// read the payment from the stream
	budget, err := sh.processPayment(s)
	if err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	// subtract the cost of the RPC
	if err := budget.Spend(pt.AccountBalanceCost); err != nil {
		return s.WriteResponseErr(fmt.Errorf("failed to pay %v for account balance: %w", pt.AccountBalanceCost, err))
	}

	// read the account balance request from the stream
	var req rhpv3.RPCAccountBalanceRequest
	if err := readRequest(s, &req, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read account balance request: %w", err)
	}

	// get the account balance
	balance, err := sh.accounts.Balance(req.Account)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get account balance: %w", err)
	}

	resp := &rhpv3.RPCAccountBalanceResponse{
		Balance: balance,
	}
	if err := budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit payment: %w", err)
	} else if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send account balance response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCLatestRevision(s *rhpv3.Stream) error {
	return nil
}
