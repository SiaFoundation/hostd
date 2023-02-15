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
func (sh *SessionHandler) processContractPayment(s *rhpv3.Stream, height uint64) (accounts.Budget, error) {
	var req rhpv3.PayByContractRequest
	if err := readRequest(s, &req, 4096, 30*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	contract, err := sh.contracts.Lock(req.ContractID, 30*time.Second)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return nil, fmt.Errorf("failed to lock contract: %w", err)
	}
	defer sh.contracts.Unlock(req.ContractID)

	current := contract.Revision
	revision, err := rhp.Revise(current, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		return nil, s.WriteResponseErr(fmt.Errorf("failed to revise contract: %w", err))
	}

	// calculate the funding amount
	if revision.ValidProofOutputs[0].Value.Cmp(current.ValidProofOutputs[0].Value) < 0 {
		return nil, s.WriteResponseErr(errors.New("invalid payment revision: new valid proof output is less than current valid proof output"))
	}
	fundAmount := revision.ValidProofOutputs[0].Value.Sub(current.ValidProofOutputs[0].Value)

	// validate that new revision
	if err := rhp.ValidatePaymentRevision(current, revision, fundAmount); err != nil {
		return nil, s.WriteResponseErr(fmt.Errorf("invalid payment revision: %w", err))
	}

	// verify the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		return nil, ErrInvalidRenterSignature
	}

	// credit the refund account with the deposit
	if _, err := sh.accounts.Credit(req.RefundAccount, fundAmount); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return nil, fmt.Errorf("failed to credit refund account: %w", err)
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
		return nil, fmt.Errorf("failed to create contract revision updater: %w", err)
	} else if err := updater.Commit(signedRevision); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return nil, fmt.Errorf("failed to update stored contract revision: %w", err)
	}

	// send the updated host signature to the renter
	err = s.WriteResponse(&rhpv3.PaymentResponse{
		Signature: hostSig,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send host signature response: %w", err)
	}
	// create a budget for the payment
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return sh.accounts.Budget(ctx, req.RefundAccount, fundAmount)
}

// processEphemeralAccountPayment initializes an RPC budget using an ephemeral
// account.
func (sh *SessionHandler) processEphemeralAccountPayment(s *rhpv3.Stream, height uint64) (accounts.Budget, error) {
	var req rhpv3.PayByEphemeralAccountRequest
	if err := readRequest(s, &req, 1024, 15*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read ephemeral account payment request: %w", err)
	}

	switch {
	case req.Expiry < height:
		return nil, errors.New("withdrawal request expired")
	case req.Expiry > height+20:
		return nil, errors.New("withdrawal request too far in the future")
	case req.Amount.IsZero():
		return nil, errors.New("withdrawal request has zero amount")
	case req.Account == rhpv3.ZeroAccount:
		return nil, errors.New("cannot withdraw from zero account")
	case !types.PublicKey(req.Account).VerifyHash(req.SigHash(), req.Signature):
		return nil, ErrInvalidRenterSignature
	}

	// create a budget for the payment
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return sh.accounts.Budget(ctx, req.Account, req.Amount)
}

// processPayment initializes an RPC budget using funds from a contract or an
// ephemeral account.
func (sh *SessionHandler) processPayment(s *rhpv3.Stream) (accounts.Budget, error) {
	var paymentType types.Specifier
	if err := readRequest(s, &paymentType, 16, 30*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read payment type: %w", err)
	}
	currentHeight := sh.chain.Tip().Index.Height
	switch paymentType {
	case rhpv3.PaymentTypeContract:
		return sh.processContractPayment(s, currentHeight)
	case rhpv3.PaymentTypeEphemeralAccount:
		return sh.processEphemeralAccountPayment(s, currentHeight)
	default:
		return nil, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}
}
