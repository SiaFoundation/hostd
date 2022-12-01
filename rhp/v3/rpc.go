package rhp

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/hostd/internal/mux"
)

var (
	// RPC IDs
	rpcAccountBalance       = newSpecifier("AccountBalance")
	rpcUpdatePriceTable     = newSpecifier("UpdatePriceTable")
	rpcExecuteProgram       = newSpecifier("ExecuteProgram")
	rpcFundAccount          = newSpecifier("FundAccount")
	rpcLatestRevision       = newSpecifier("LatestRevision")
	rpcRegistrySubscription = newSpecifier("Subscription")
	rpcFormContract         = newSpecifier("FormContract")
	rpcRenewContract        = newSpecifier("RenewContract")

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

// handleRPCPriceTable sends the host's price table to the renter.
func (sh *SessionHandler) handleRPCPriceTable(s *rpcSession) error {
	pt, err := sh.PriceTable()
	if err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to get price table: %w", err)
	}
	buf, err := json.Marshal(pt)
	if err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to marshal price table: %w", err)
	}

	resp := &rpcUpdatePriceTableResponse{
		PriceTableJSON: buf,
	}
	if err := s.WriteObjects(resp); err != nil {
		return fmt.Errorf("failed to send price table: %w", err)
	}

	// process the payment, catch connection closed errors since the renter
	// likely did not intend to pay
	budget, err := sh.processPayment(s)
	if errors.Is(err, mux.ErrPeerClosedStream) || errors.Is(err, mux.ErrPeerClosedConn) {
		return nil
	} else if err != nil {
		return s.WriteError(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.UpdatePriceTableCost); err != nil {
		return s.WriteError(fmt.Errorf("failed to pay %v for price table: %w", pt.UpdatePriceTableCost, err))
	}

	// register the price table for future use
	sh.priceTables.Register(pt)
	if err := budget.Commit(); err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	} else if err := s.WriteObjects(&rpcTrackingResponse{}); err != nil {
		return fmt.Errorf("failed to send tracking response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCFundAccount(s *rpcSession) error {
	// read the price table ID from the stream
	pt, err := sh.readPriceTable(s)
	if err != nil {
		return s.WriteError(fmt.Errorf("failed to read price table: %w", err))
	}

	// read the fund request from the stream
	var fundReq rpcFundAccountRequest
	if err := s.ReadObject(&fundReq, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read fund account request: %w", err)
	}

	// process the payment for funding the account
	budget, err := sh.processPayment(s)
	if err != nil {
		return s.WriteError(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	// subtract the cost of funding the account
	if err := budget.Spend(pt.FundAccountCost); err != nil {
		return s.WriteError(fmt.Errorf("failed to pay %v for fund account: %w", pt.FundAccountCost, err))
	}

	// commit the budget and fund the account with the remaining balance
	// note: may need to add an atomic transfer?
	fundAmount := budget.Empty()
	if err := budget.Commit(); err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	balance, err := sh.accounts.Credit(fundReq.Account, fundAmount)
	if err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to credit account: %w", err)
	}

	fundResp := &rpcFundAccountResponse{
		Balance: balance,
		Receipt: fundReceipt{
			Host:      sh.HostKey(),
			Account:   fundReq.Account,
			Amount:    fundAmount,
			Timestamp: time.Now(),
		},
	}
	sigHash := hashFundReceipt(fundResp.Receipt)
	copy(fundResp.Signature[:], ed25519.Sign(sh.privateKey, sigHash[:]))

	// send the response
	if err := s.WriteObjects(fundResp); err != nil {
		return fmt.Errorf("failed to send fund account response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCAccountBalance(s *rpcSession) error {
	// get the price table to use for payment
	pt, err := sh.readPriceTable(s)
	if err != nil {
		return s.WriteError(fmt.Errorf("failed to read price table: %w", err))
	}

	// read the payment from the stream
	budget, err := sh.processPayment(s)
	if err != nil {
		return s.WriteError(fmt.Errorf("failed to process payment: %w", err))
	}
	defer budget.Rollback()

	// subtract the cost of the RPC
	if err := budget.Spend(pt.AccountBalanceCost); err != nil {
		return s.WriteError(fmt.Errorf("failed to pay %v for account balance: %w", pt.AccountBalanceCost, err))
	}

	// read the account balance request from the stream
	var req rpcAccountBalanceRequest
	if err := s.ReadObject(&req, 32, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read account balance request: %w", err)
	}

	// get the account balance
	balance, err := sh.accounts.Balance(req.AccountID)
	if err != nil {
		s.WriteError(ErrHostInternalError)
		return fmt.Errorf("failed to get account balance: %w", err)
	}

	resp := &rpcAccountBalanceResponse{
		Balance: balance,
	}
	if err := budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit payment: %w", err)
	} else if err := s.WriteObjects(resp); err != nil {
		return fmt.Errorf("failed to send account balance response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCLatestRevision(s *rpcSession) error {
	return nil
}
