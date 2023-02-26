package rhp

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/rhp"
)

// processContractPayment initializes an RPC budget using funds from a contract.
func (sh *SessionHandler) processContractPayment(s *rhpv3.Stream, height uint64) (rhpv3.Account, types.Currency, error) {
	var req rhpv3.PayByContractRequest
	if err := readRequest(s, &req, 4096, 30*time.Second); err != nil {
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to lock contract: %w", err)
	}
	defer sh.contracts.Unlock(req.ContractID)

	current := contract.Revision
	revision, err := rhp.Revise(current, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err = fmt.Errorf("failed to revise contract: %w", err)
		s.WriteResponseErr(err)
		return rhpv3.ZeroAccount, types.ZeroCurrency, err
	}

	// calculate the funding amount
	if current.ValidProofOutputs[0].Value.Cmp(revision.ValidProofOutputs[0].Value) < 0 {
		err = errors.New("invalid payment revision: new revision has more funds than current revision")
		s.WriteResponseErr(err)
		return rhpv3.ZeroAccount, types.ZeroCurrency, err
	}
	fundAmount := current.ValidProofOutputs[0].Value.Sub(revision.ValidProofOutputs[0].Value)

	// validate that new revision
	if err := rhp.ValidatePaymentRevision(current, revision, fundAmount); err != nil {
		err = fmt.Errorf("invalid payment revision: %w", err)
		s.WriteResponseErr(err)
		return rhpv3.ZeroAccount, types.ZeroCurrency, err
	}

	// verify the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		return rhpv3.ZeroAccount, types.ZeroCurrency, ErrInvalidRenterSignature
	}

	settings := sh.settings.Settings()
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to get host settings: %w", err)
	}

	// credit the account with the deposit
	if _, err := sh.accounts.Credit(req.RefundAccount, fundAmount, time.Now().Add(settings.AccountExpiry)); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to credit refund account: %w", err)
	}

	// update the host signature and the contract
	hostSig := sh.privateKey.SignHash(sigHash)
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostSig,
		RenterSignature: req.Signature,
	}
	updater, err := sh.contracts.ReviseContract(req.ContractID)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to create contract revision updater: %w", err)
	}
	defer updater.Close()
	if err := updater.Commit(signedRevision); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to update stored contract revision: %w", err)
	}

	// send the updated host signature to the renter
	err = s.WriteResponse(&rhpv3.PaymentResponse{
		Signature: hostSig,
	})
	if err != nil {
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to send host signature response: %w", err)
	}
	return req.RefundAccount, fundAmount, nil
}

// processAccountPayment initializes an RPC budget using an ephemeral
// account.
func (sh *SessionHandler) processAccountPayment(s *rhpv3.Stream, height uint64) (rhpv3.Account, types.Currency, error) {
	var req rhpv3.PayByEphemeralAccountRequest
	if err := readRequest(s, &req, 1024, 15*time.Second); err != nil {
		return rhpv3.ZeroAccount, types.ZeroCurrency, fmt.Errorf("failed to read ephemeral account payment request: %w", err)
	}

	switch {
	case req.Expiry < height:
		return rhpv3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request expired")
	case req.Expiry > height+20:
		return rhpv3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request too far in the future")
	case req.Amount.IsZero():
		return rhpv3.ZeroAccount, types.ZeroCurrency, errors.New("withdrawal request has zero amount")
	case req.Account == rhpv3.ZeroAccount:
		return rhpv3.ZeroAccount, types.ZeroCurrency, errors.New("cannot withdraw from zero account")
	case !types.PublicKey(req.Account).VerifyHash(req.SigHash(), req.Signature):
		return rhpv3.ZeroAccount, types.ZeroCurrency, ErrInvalidRenterSignature
	}
	return req.Account, req.Amount, nil
}

// processPayment initializes an RPC budget using funds from a contract or an
// ephemeral account.
func (sh *SessionHandler) processPayment(s *rhpv3.Stream) (*accounts.Budget, error) {
	var paymentType types.Specifier
	if err := readRequest(s, &paymentType, 16, 30*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read payment type: %w", err)
	}
	var account rhpv3.Account
	var amount types.Currency
	var err error
	currentHeight := sh.chain.TipState().Index.Height
	switch paymentType {
	case rhpv3.PaymentTypeContract:
		account, amount, err = sh.processContractPayment(s, currentHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to process contract payment: %w", err)
		}
	case rhpv3.PaymentTypeEphemeralAccount:
		account, amount, err = sh.processAccountPayment(s, currentHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to process account payment: %w", err)
		}
	default:
		return nil, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}

	// create a budget for the payment
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return sh.accounts.Budget(ctx, account, amount)
}

// processFundAccountPayment processes a contract payment to fund an account for
// RPCFundAccount returning the fund amount and the current balance of the
// account. Accounts can only be funded by a contract.
func (sh *SessionHandler) processFundAccountPayment(pt rhpv3.HostPriceTable, s *rhpv3.Stream, accountID rhpv3.Account) (fundAmount, balance types.Currency, _ error) {
	var paymentType types.Specifier
	if err := readRequest(s, &paymentType, 16, 30*time.Second); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to read payment type: %w", err)
	} else if paymentType != rhpv3.PaymentTypeContract {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}
	var req rhpv3.PayByContractRequest
	if err := readRequest(s, &req, 4096, 30*time.Second); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to lock contract: %w", err)
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
	if current.ValidProofOutputs[0].Value.Cmp(revision.ValidProofOutputs[0].Value) < 0 {
		err = errors.New("invalid payment revision: new revision has more funds than current revision")
		s.WriteResponseErr(err)
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
	totalAmount := current.ValidProofOutputs[0].Value.Sub(revision.ValidProofOutputs[0].Value)

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

	settings := sh.settings.Settings()
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to get host settings: %w", err)
	}

	// subtract the cost of the RPC from the total amount
	fundAmount = totalAmount.Sub(pt.FundAccountCost)
	// credit the account with the deposit
	balance, err = sh.accounts.Credit(accountID, fundAmount, time.Now().Add(settings.AccountExpiry))
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to credit refund account: %w", err)
	}

	// update the host signature and the contract
	hostSig := sh.privateKey.SignHash(sigHash)
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostSig,
		RenterSignature: req.Signature,
	}
	updater, err := sh.contracts.ReviseContract(req.ContractID)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to create contract revision updater: %w", err)
	}
	defer updater.Close()
	if err := updater.Commit(signedRevision); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to update stored contract revision: %w", err)
	}

	// send the updated host signature to the renter
	err = s.WriteResponse(&rhpv3.PaymentResponse{
		Signature: hostSig,
	})
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to send host signature response: %w", err)
	}
	return fundAmount, balance, nil
}
