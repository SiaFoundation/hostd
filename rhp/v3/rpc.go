package rhp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/rhp"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	maxRequestSize        = 4096     // 4 KiB
	maxProgramRequestSize = 20 << 20 // 20 MiB
)

var (
	// ErrTxnMissingContract is returned if the transaction set does not contain
	// any transactions or if the transaction does not contain exactly one
	// contract.
	ErrTxnMissingContract = errors.New("transaction set does not contain a file contract")
	// ErrHostInternalError is returned if the host encountered an error during
	// an RPC that doesn't need to be broadcast to the renter (e.g. insufficient
	// funds).
	ErrHostInternalError = errors.New("internal error")
	// ErrInvalidRenterSignature is returned when a contract's renter signature
	// is invalid.
	ErrInvalidRenterSignature = errors.New("invalid renter signature")
	// ErrNotAcceptingContracts is returned when the host is not accepting
	// contracts.
	ErrNotAcceptingContracts = errors.New("host is not accepting contracts")
)

// handleRPCPriceTable sends the host's price table to the renter.
func (sh *SessionHandler) handleRPCPriceTable(s *rhpv3.Stream, log *zap.Logger) error {
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
	budget, err := sh.processPayment(s, &pt)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.UpdatePriceTableCost); err != nil {
		err = fmt.Errorf("failed to pay %v for price table: %w", pt.UpdatePriceTableCost, err)
		s.WriteResponseErr(err)
		return err
	}

	if err := budget.Commit(); err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	// register the price table for future use
	sh.priceTables.Register(pt)
	if err := s.WriteResponse(&rhpv3.RPCPriceTableResponse{}); err != nil {
		return fmt.Errorf("failed to send tracking response: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCFundAccount(s *rhpv3.Stream, log *zap.Logger) error {
	s.SetDeadline(time.Now().Add(time.Minute))
	// read the price table ID from the stream
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// read the fund request from the stream
	var fundReq rhpv3.RPCFundAccountRequest
	if err := s.ReadRequest(&fundReq, 32); err != nil {
		return fmt.Errorf("failed to read fund account request: %w", err)
	}

	// process the payment for funding the account
	fundAmount, balance, err := sh.processFundAccountPayment(pt, s, fundReq.Account)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
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

func (sh *SessionHandler) handleRPCAccountBalance(s *rhpv3.Stream, log *zap.Logger) error {
	s.SetDeadline(time.Now().Add(time.Minute))
	// get the price table to use for payment
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// read the payment from the stream
	budget, err := sh.processPayment(s, &pt)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	// subtract the cost of the RPC
	if err := budget.Spend(pt.AccountBalanceCost); err != nil {
		err = fmt.Errorf("failed to pay %v for account balance: %w", pt.AccountBalanceCost, err)
		s.WriteResponseErr(err)
		return err
	}

	// read the account balance request from the stream
	var req rhpv3.RPCAccountBalanceRequest
	if err := s.ReadRequest(&req, 32); err != nil {
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

func (sh *SessionHandler) handleRPCLatestRevision(s *rhpv3.Stream, log *zap.Logger) error {
	s.SetDeadline(time.Now().Add(time.Minute))
	var req rhpv3.RPCLatestRevisionRequest
	if err := s.ReadRequest(&req, maxRequestSize); err != nil {
		return fmt.Errorf("failed to read latest revision request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		err := fmt.Errorf("failed to lock contract: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	sh.contracts.Unlock(contract.Revision.ParentID)

	resp := &rhpv3.RPCLatestRevisionResponse{
		Revision: contract.Revision,
	}
	if err := s.WriteResponse(resp); err != nil {
		return fmt.Errorf("failed to send latest revision response: %w", err)
	}

	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	budget, err := sh.processPayment(s, &pt)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer budget.Rollback()

	if err := budget.Spend(pt.LatestRevisionCost); err != nil {
		err = fmt.Errorf("failed to pay %v for latest revision: %w", pt.LatestRevisionCost, err)
		s.WriteResponseErr(err)
		return err
	} else if err := budget.Commit(); err != nil {
		return fmt.Errorf("failed to commit payment: %w", err)
	}
	return nil
}

func (sh *SessionHandler) handleRPCRenew(s *rhpv3.Stream, log *zap.Logger) error {
	s.SetDeadline(time.Now().Add(2 * time.Minute))
	if !sh.settings.Settings().AcceptingContracts {
		s.WriteResponseErr(ErrNotAcceptingContracts)
		return ErrNotAcceptingContracts
	}
	pt, err := sh.readPriceTable(s)
	if errors.Is(err, ErrNoPriceTable) {
		// no price table, send the renter a default one
		pt, err = sh.PriceTable()
		if err != nil {
			s.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to get price table: %w", err)
		}
		buf, err := json.Marshal(pt)
		if err != nil {
			s.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to marshal price table: %w", err)
		}
		ptResp := &rhpv3.RPCUpdatePriceTableResponse{
			PriceTableJSON: buf,
		}
		if err := s.WriteResponse(ptResp); err != nil {
			return fmt.Errorf("failed to send price table response: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to read price table: %w", err)
	}

	var req rhpv3.RPCRenewContractRequest
	if err := s.ReadRequest(&req, 10*maxRequestSize); err != nil {
		return fmt.Errorf("failed to read renew contract request: %w", err)
	} else if err := validRenewalTxnSet(req.TransactionSet); err != nil {
		err = fmt.Errorf("invalid renewal transaction set: %w", err)
		s.WriteResponseErr(err)
		return err
	} else if req.RenterKey.Algorithm != types.SpecifierEd25519 || len(req.RenterKey.Key) != ed25519.PublicKeySize {
		err = errors.New("renter key must be an ed25519 public key")
		s.WriteResponseErr(err)
		return err
	}

	renterKey := *(*types.PublicKey)(req.RenterKey.Key)
	hostUnlockKey := sh.privateKey.PublicKey().UnlockKey()
	parents := req.TransactionSet[:len(req.TransactionSet)-1]
	renewalTxn := req.TransactionSet[len(req.TransactionSet)-1]
	clearingRevision := renewalTxn.FileContractRevisions[0]
	renewal := renewalTxn.FileContracts[0]

	// lock the existing contract
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing, err := sh.contracts.Lock(ctx, clearingRevision.ParentID)
	if err != nil {
		err := fmt.Errorf("failed to lock contract: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	defer sh.contracts.Unlock(clearingRevision.ParentID)

	// validate the final revision and renter signature
	finalPayment, err := rhp.ValidateClearingRevision(existing.Revision, clearingRevision, types.ZeroCurrency)
	if err != nil {
		err := fmt.Errorf("failed to validate clearing revision: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	finalRevisionSigHash := hashFinalRevision(clearingRevision, renewal)
	if !existing.RenterKey().VerifyHash(finalRevisionSigHash, req.FinalRevisionSignature) { // important to verify using the existing contract's renter key
		err := fmt.Errorf("failed to verify final revision signature: %w", ErrInvalidRenterSignature)
		s.WriteResponseErr(err)
		return err
	}
	// sign the clearing revision
	signedClearingRevision := contracts.SignedRevision{
		Revision:        clearingRevision,
		RenterSignature: req.FinalRevisionSignature,
		HostSignature:   sh.privateKey.SignHash(finalRevisionSigHash),
	}

	// calculate the "base" storage cost to the renter and risked collateral for
	// the host for the data already in the contract. If the contract height did
	// not increase, base costs are zero since the storage is already paid for.
	baseRevenue := pt.RenewContractCost
	var baseCollateral types.Currency
	if renewal.WindowStart > existing.Revision.WindowStart {
		extension := uint64(renewal.WindowStart - existing.Revision.WindowStart)
		baseRevenue = baseRevenue.Add(pt.WriteStoreCost.Mul64(renewal.Filesize).Mul64(extension))
		baseCollateral = pt.CollateralCost.Mul64(renewal.Filesize).Mul64(extension)
	}

	riskedCollateral, lockedCollateral, err := validateContractRenewal(existing.Revision, renewal, hostUnlockKey, req.RenterKey, sh.wallet.Address(), baseRevenue, baseCollateral, pt)
	if err != nil {
		err := fmt.Errorf("failed to validate renewal: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	renterInputs, renterOutputs := len(renewalTxn.SiacoinInputs), len(renewalTxn.SiacoinOutputs)
	toSign, release, err := sh.wallet.FundTransaction(&renewalTxn, lockedCollateral)
	if err != nil {
		s.WriteResponseErr(fmt.Errorf("failed to fund renewal transaction: %w", ErrHostInternalError))
		return fmt.Errorf("failed to fund renewal transaction: %w", err)
	}
	defer release()

	hostAdditions := &rhpv3.RPCRenewContractHostAdditions{
		SiacoinInputs:          renewalTxn.SiacoinInputs[renterInputs:],
		SiacoinOutputs:         renewalTxn.SiacoinOutputs[renterOutputs:],
		FinalRevisionSignature: signedClearingRevision.HostSignature,
	}
	if err := s.WriteResponse(hostAdditions); err != nil {
		return fmt.Errorf("failed to write host additions: %w", err)
	}

	var renterSigsResp rhpv3.RPCRenewSignatures
	if err := s.ReadRequest(&renterSigsResp, 10*maxRequestSize); err != nil {
		return fmt.Errorf("failed to read renter signatures: %w", err)
	}

	// create the initial revision and verify the renter's signature
	renewalRevision := rhp.InitialRevision(&renewalTxn, hostUnlockKey, req.RenterKey)
	renewalSigHash := rhp.HashRevision(renewalRevision)
	if validateRenterRevisionSignature(renterSigsResp.RevisionSignature, renewalRevision.ParentID, renewalSigHash, renterKey) != nil {
		err := fmt.Errorf("failed to verify renter revision signature: %w", ErrInvalidRenterSignature)
		s.WriteResponseErr(err)
		return err
	}
	signedRenewal := contracts.SignedRevision{
		Revision:        renewalRevision,
		HostSignature:   sh.privateKey.SignHash(renewalSigHash),
		RenterSignature: *(*types.Signature)(renterSigsResp.RevisionSignature.Signature),
	}

	// add the final revision signatures to the transaction
	renewalTxn.Signatures = append(renewalTxn.Signatures, types.TransactionSignature{
		ParentID:       types.Hash256(existing.Revision.ParentID),
		PublicKeyIndex: 0,
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: req.FinalRevisionSignature[:],
	}, types.TransactionSignature{
		ParentID:       types.Hash256(existing.Revision.ParentID),
		PublicKeyIndex: 1,
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: signedClearingRevision.HostSignature[:],
	})
	// add the renter's signatures to the transaction
	renewalTxn.Signatures = append(renewalTxn.Signatures, renterSigsResp.TransactionSignatures...)
	renterSigs := len(renewalTxn.Signatures)

	// sign and broadcast the transaction
	if err := sh.wallet.SignTransaction(sh.chain.TipState(), &renewalTxn, toSign, wallet.ExplicitCoveredFields(renewalTxn)); err != nil {
		s.WriteResponseErr(fmt.Errorf("failed to sign renewal transaction: %w", ErrHostInternalError))
		return fmt.Errorf("failed to sign renewal transaction: %w", err)
	}
	renewalTxnSet := append(parents, renewalTxn)
	if err := sh.tpool.AcceptTransactionSet(renewalTxnSet); err != nil {
		err = fmt.Errorf("failed to broadcast renewal transaction: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// calculate the usage
	finalRevisionUsage := contracts.Usage{
		RPCRevenue: finalPayment,
	}
	renewalUsage := contracts.Usage{
		RPCRevenue:       pt.ContractPrice,
		StorageRevenue:   baseRevenue,
		RiskedCollateral: riskedCollateral,
	}
	// renew the contract in the manager
	err = sh.contracts.RenewContract(signedRenewal, signedClearingRevision, renewalTxnSet, lockedCollateral, finalRevisionUsage, renewalUsage)
	if err != nil {
		s.WriteResponseErr(fmt.Errorf("failed to renew contract: %w", ErrHostInternalError))
		return fmt.Errorf("failed to renew contract: %w", err)
	}

	// send the signatures to the renter
	hostSigs := &rhpv3.RPCRenewSignatures{
		TransactionSignatures: renewalTxn.Signatures[renterSigs:],
		RevisionSignature: types.TransactionSignature{
			ParentID:       types.Hash256(signedRenewal.Revision.ParentID),
			PublicKeyIndex: 1,
			CoveredFields: types.CoveredFields{
				FileContractRevisions: []uint64{0},
			},
			Signature: signedRenewal.HostSignature[:],
		},
	}
	if err := s.WriteResponse(hostSigs); err != nil {
		return fmt.Errorf("failed to write host signatures: %w", err)
	}
	return nil
}

// handleRPCExecute handles an RPCExecuteProgram request.
func (sh *SessionHandler) handleRPCExecute(s *rhpv3.Stream, log *zap.Logger) error {
	s.SetDeadline(time.Now().Add(5 * time.Minute))
	// read the price table
	pt, err := sh.readPriceTable(s)
	if err != nil {
		err = fmt.Errorf("failed to read price table: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	// create the program budget
	budget, err := sh.processPayment(s, &pt)
	if err != nil {
		err = fmt.Errorf("failed to process payment: %w", err)
		s.WriteResponseErr(err)
		return err
	}
	// note: the budget is committed by the executor, no need to commit it in the handler.
	defer budget.Rollback()

	// read the program request
	var executeReq rhpv3.RPCExecuteProgramRequest
	if err := s.ReadRequest(&executeReq, maxProgramRequestSize); err != nil {
		return fmt.Errorf("failed to read execute request: %w", err)
	}
	instructions := executeReq.Program

	// pay for the execution
	if err := budget.Spend(pt.InitBaseCost); err != nil {
		err = fmt.Errorf("failed to pay program init cost: %w", err)
		s.WriteResponseErr(err)
		return err
	}

	var requiresContract, requiresFinalization bool
	for _, instr := range instructions {
		requiresContract = requiresContract || instr.RequiresContract()
		requiresFinalization = requiresFinalization || instr.RequiresFinalization()
	}
	log = log.Named("mdm")
	// if the program requires a contract, lock it
	var revision *contracts.SignedRevision
	if requiresContract || requiresFinalization {
		if executeReq.FileContractID == (types.FileContractID{}) {
			err = ErrContractRequired
			s.WriteResponseErr(err)
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		contract, err := sh.contracts.Lock(ctx, executeReq.FileContractID)
		if err != nil {
			err = fmt.Errorf("failed to lock contract: %w", err)
			s.WriteResponseErr(err)
			return err
		}
		defer sh.contracts.Unlock(contract.Revision.ParentID)
		revision = &contract
		log = log.With(zap.String("contractID", contract.Revision.ParentID.String())) // attach the contract ID to the logger
	}

	// generate a cancellation token and write it to the stream. Currently just
	// a placeholder.
	cancelToken := types.Specifier(frand.Entropy128())
	if err := s.WriteResponse(&cancelToken); err != nil {
		return fmt.Errorf("failed to write cancel token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Debug("executing program", zap.Int("instructions", len(instructions)), zap.String("budget", budget.Remaining().ExactString()), zap.Bool("requiresFinalization", requiresFinalization))
	// create the program executor
	// note: the budget is committed by the executor, no need to commit it in the handler.
	executor, err := sh.newExecutor(instructions, executeReq.ProgramData, pt, budget, revision, requiresFinalization, log)
	if err != nil {
		s.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to create program executor: %w", err)
	} else if err := executor.Execute(ctx, s); err != nil {
		return fmt.Errorf("failed to execute program: %w", err)
	}

	return nil
}

func validateRenterRevisionSignature(sig types.TransactionSignature, fcID types.FileContractID, sigHash types.Hash256, renterKey types.PublicKey) error {
	switch {
	case sig.ParentID != types.Hash256(fcID):
		return errors.New("revision signature has invalid parent ID")
	case sig.PublicKeyIndex != 0:
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
	case !renterKey.VerifyHash(sigHash, *(*types.Signature)(sig.Signature)):
		return errors.New("revision signature is invalid")
	}
	return nil
}

func validRenewalTxnSet(txnset []types.Transaction) error {
	switch {
	case len(txnset) == 0:
		return errors.New("transaction set does not contain any transactions")
	case len(txnset[len(txnset)-1].FileContracts) != 1:
		return errors.New("transaction set must contain exactly one file contract")
	case len(txnset[len(txnset)-1].FileContractRevisions) != 1:
		return errors.New("transaction set must contain exactly one file contract revision")
	}
	for _, txn := range txnset[:len(txnset)-1] {
		if len(txn.FileContracts) != 0 || len(txn.FileContractRevisions) != 0 {
			return errors.New("transaction set contains non-renewal transactions")
		}
	}
	return nil
}
