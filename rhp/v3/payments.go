package rhp

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/accounts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/rhp"
)

// processContractPayment initializes an RPC budget using funds from a contract.
func (sh *SessionHandler) processContractPayment(s *rhp3.Stream, _ uint64) (rhp3.Account, types.Currency, error) {
	var req rhp3.PayByContractRequest
	if err := s.ReadRequest(&req, maxRequestSize); err != nil {
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		s.WriteResponseErr(err)
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to lock contract %v: %w", req.ContractID, err)
	}
	defer sh.contracts.Unlock(req.ContractID)

	current := contract.Revision
	revision, err := rhp.Revise(current, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err = fmt.Errorf("failed to revise contract: %w", err)
		s.WriteResponseErr(err)
		return rhp3.ZeroAccount, types.ZeroCurrency, err
	}

	// calculate the funding amount
	fundAmount, underflow := current.ValidRenterPayout().SubWithUnderflow(revision.ValidRenterPayout())
	if underflow {
		err = errors.New("invalid payment revision: new revision has more funds than current revision")
		s.WriteResponseErr(err)
		return rhp3.ZeroAccount, types.ZeroCurrency, err
	}

	// validate that new revision
	if err := rhp.ValidatePaymentRevision(current, revision, fundAmount); err != nil {
		err = fmt.Errorf("invalid payment revision: %w", err)
		s.WriteResponseErr(err)
		return rhp3.ZeroAccount, types.ZeroCurrency, err
	}

	// verify the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		return rhp3.ZeroAccount, types.ZeroCurrency, ErrInvalidRenterSignature
	}

	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.WriteResponseErr(err)
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to get host settings: %w", err)
	}
	hostSig := sh.privateKey.SignHash(sigHash)
	fundReq := accounts.FundAccountWithContract{
		Account: req.RefundAccount,
		Revision: contracts.SignedRevision{
			Revision:        revision,
			HostSignature:   hostSig,
			RenterSignature: req.Signature,
		},
		Amount:     fundAmount,
		Expiration: time.Now().Add(settings.EphemeralAccountExpiry),
	}
	// credit the account with the deposit
	_, err = sh.accounts.Credit(fundReq, true)
	if err != nil {
		if errors.Is(err, accounts.ErrBalanceExceeded) {
			s.WriteResponseErr(accounts.ErrBalanceExceeded)
		} else {
			s.WriteResponseErr(err)
		}
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to credit refund account: %w", err)
	}

	// send the updated host signature to the renter
	err = s.WriteResponse(&rhp3.PaymentResponse{
		Signature: hostSig,
	})
	if err != nil {
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to send host signature response: %w", err)
	}
	return req.RefundAccount, fundAmount, nil
}

// processAccountPayment initializes an RPC budget using an ephemeral
// account.
func (sh *SessionHandler) processAccountPayment(s *rhp3.Stream, height uint64) (rhp3.Account, types.Currency, error) {
	var req rhp3.PayByEphemeralAccountRequest
	if err := s.ReadRequest(&req, maxRequestSize); err != nil {
		return rhp3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to read ephemeral account payment request: %w", err)
	}

	switch {
	case req.Expiry < height:
		return rhp3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request expired")
	case req.Expiry > height+20:
		return rhp3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request too far in the future")
	case req.Amount.IsZero():
		return rhp3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request has zero amount")
	case req.Account == rhp3.ZeroAccount:
		return rhp3.ZeroAccount, types.ZeroCurrency, errors.New("cannot withdraw from zero account")
	case !types.PublicKey(req.Account).VerifyHash(req.SigHash(), req.Signature):
		return rhp3.ZeroAccount, types.ZeroCurrency, ErrInvalidRenterSignature
	}
	return req.Account, req.Amount, nil
}

// processPayment initializes an RPC budget using funds from a contract or an
// ephemeral account.
func (sh *SessionHandler) processPayment(s *rhp3.Stream, pt *rhp3.HostPriceTable) (*accounts.Budget, error) {
	var paymentType types.Specifier
	if err := s.ReadRequest(&paymentType, 16); err != nil {
		return nil, fmt.Errorf("failed to read payment type: %w", err)
	}
	var account rhp3.Account
	var amount types.Currency
	var err error
	currentHeight := pt.HostBlockHeight
	switch paymentType {
	case rhp3.PaymentTypeContract:
		account, amount, err = sh.processContractPayment(s, currentHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to process contract payment: %w", err)
		}
	case rhp3.PaymentTypeEphemeralAccount:
		account, amount, err = sh.processAccountPayment(s, currentHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to process account payment: %w", err)
		}
	default:
		return nil, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}

	// create a budget for the payment
	return sh.accounts.Budget(account, amount)
}

// processFundAccountPayment processes a contract payment to fund an account for
// RPCFundAccount returning the fund amount and the current balance of the
// account. Accounts can only be funded by a contract.
func (sh *SessionHandler) processFundAccountPayment(pt rhp3.HostPriceTable, s *rhp3.Stream, accountID rhp3.Account) (fundAmount, balance types.Currency, _ error) {
	var paymentType types.Specifier
	if err := s.ReadRequest(&paymentType, 16); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to read payment type: %w", err)
	} else if paymentType != rhp3.PaymentTypeContract {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}
	var req rhp3.PayByContractRequest
	if err := s.ReadRequest(&req, maxRequestSize); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to lock contract %v: %w", req.ContractID, err)
	}
	defer sh.contracts.Unlock(req.ContractID)

	current := contract.Revision
	revision, err := rhp.Revise(current, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	// calculate the funding amount
	totalAmount, underflow := current.ValidRenterPayout().SubWithUnderflow(revision.ValidRenterPayout())
	if underflow {
		err = errors.New("invalid payment revision: new revision has more funds than current revision")
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	// validate that new revision
	if err := rhp.ValidatePaymentRevision(current, revision, totalAmount); err != nil {
		err = fmt.Errorf("invalid payment revision: %w", err)
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	// verify the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		return types.ZeroCurrency, types.ZeroCurrency, ErrInvalidRenterSignature
	}

	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to get host settings: %w", err)
	}
	// credit the account with the deposit
	hostSig := sh.privateKey.SignHash(sigHash)
	fundReq := accounts.FundAccountWithContract{
		Account: accountID,
		Revision: contracts.SignedRevision{
			Revision:        revision,
			HostSignature:   hostSig,
			RenterSignature: req.Signature,
		},
		Cost:       pt.FundAccountCost,
		Amount:     totalAmount.Sub(pt.FundAccountCost),
		Expiration: time.Now().Add(settings.EphemeralAccountExpiry),
	}
	// credit the account with the deposit
	balance, err = sh.accounts.Credit(fundReq, false)
	if err != nil {
		if errors.Is(err, accounts.ErrBalanceExceeded) {
			s.WriteResponseErr(accounts.ErrBalanceExceeded)
		} else {
			s.WriteResponseErr(err)
		}
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to credit account: %w", err)
	}

	// send the updated host signature to the renter
	err = s.WriteResponse(&rhp3.PaymentResponse{
		Signature: hostSig,
	})
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to send host signature response: %w", err)
	}
	return fundReq.Amount, balance, nil
}
