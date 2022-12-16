package rhp

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

var (
	payByContract         = newSpecifier("PayByContract")
	payByEphemeralAccount = newSpecifier("PayByEphemAcc")
)

func hashWithdrawalMessage(wm withdrawalMessage) (hash crypto.Hash) {
	h, _ := blake2b.New256(nil)
	enc := newEncoder(h)
	wm.encodeTo(enc)
	enc.Flush()
	h.Sum(hash[:0])
	return
}

func hashFundReceipt(fr fundReceipt) (hash crypto.Hash) {
	h, _ := blake2b.New256(nil)
	enc := newEncoder(h)
	fr.encodeTo(enc)
	enc.Flush()
	h.Sum(hash[:0])
	return
}

// processContractPayment initializes an RPC budget using funds from a contract.
func (sh *SessionHandler) processContractPayment(s *rpcSession, height uint64) (accounts.Budget, error) {
	var req rpcPayByContractRequest
	if err := s.ReadObject(&req, 4096, 30*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read contract payment request: %w", err)
	}

	contract, err := sh.contracts.Lock(req.ContractID, 30*time.Second)
	if err != nil {
		s.WriteError(ErrHostInternalError)
		return nil, fmt.Errorf("failed to lock contract: %w", err)
	}
	defer sh.contracts.Unlock(req.ContractID)

	current := contract.Revision
	revision, err := revise(current, req.NewRevisionNumber, req.NewValidProofValues, req.NewMissedProofValues)
	if err != nil {
		return nil, s.WriteError(fmt.Errorf("failed to revise contract: %w", err))
	}
	// validate that new revision
	fundAmount, err := validatePaymentRevision(current, revision)
	if err != nil {
		return nil, s.WriteError(fmt.Errorf("invalid payment revision: %w", err))
	}

	// verify the renter's signature
	sigHash := hashRevision(revision)
	renterKey := contract.RenterKey()
	if !ed25519.Verify(renterKey, sigHash[:], req.Signature) {
		return nil, ErrInvalidRenterSignature
	}

	// credit the refund account with the deposit
	if _, err := sh.accounts.Credit(req.RefundAccount, fundAmount); err != nil {
		s.WriteError(ErrHostInternalError)
		return nil, fmt.Errorf("failed to credit refund account: %w", err)
	}

	// update the host signature and the contract
	hostSig := ed25519.Sign(sh.privateKey, sigHash[:])
	if err := sh.contracts.ReviseContract(revision, req.Signature, hostSig); err != nil {
		s.WriteError(ErrHostInternalError)
		return nil, fmt.Errorf("failed to update stored contract revision: %w", err)
	}

	// send the updated host signature to the renter
	var sig crypto.Signature
	copy(sig[:], hostSig)
	err = s.WriteObjects(&rpcPayByContractResponse{
		Signature: sig,
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
func (sh *SessionHandler) processEphemeralAccountPayment(s *rpcSession, height uint64) (accounts.Budget, error) {
	var req rpcPayByEphemeralAccountRequest
	if err := s.ReadObject(&req, 1024, 15*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read ephemeral account payment request: %w", err)
	}

	// validate that the request is valid.
	switch {
	case req.Message.Expiry < types.BlockHeight(height):
		return nil, errors.New("withdrawal request expired")
	case req.Message.Expiry > types.BlockHeight(height+20):
		return nil, errors.New("withdrawal request too far in the future")
	case req.Message.Amount.IsZero():
		return nil, errors.New("withdrawal request has zero amount")
	case req.Message.AccountID == accounts.AccountID{}:
		return nil, errors.New("cannot withdraw from zero account")
	}

	// verify the signature
	sigHash := hashWithdrawalMessage(req.Message)
	if !ed25519.Verify(req.Message.AccountID[:], sigHash[:], req.Signature[:]) {
		return nil, ErrInvalidRenterSignature
	}

	// create a budget for the payment
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return sh.accounts.Budget(ctx, req.Message.AccountID, req.Message.Amount)
}

// processPayment initializes an RPC budget using funds from a contract or an
// ephemeral account.
func (sh *SessionHandler) processPayment(s *rpcSession) (accounts.Budget, error) {
	var paymentType Specifier
	if err := s.ReadObject(&paymentType, 16, 30*time.Second); err != nil {
		return nil, fmt.Errorf("failed to read payment type: %w", err)
	}
	currentHeight := sh.chain.Tip().Index.Height
	switch paymentType {
	case payByContract:
		return sh.processContractPayment(s, currentHeight)
	case payByEphemeralAccount:
		return sh.processEphemeralAccountPayment(s, currentHeight)
	default:
		return nil, fmt.Errorf("unrecognized payment type: %q", paymentType)
	}
}
