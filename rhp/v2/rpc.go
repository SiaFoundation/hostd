package rhp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/rhp"
	"go.uber.org/zap"
)

var (
	// ErrTxnMissingContract is returned if the transaction set does not contain
	// any transactions or if the transaction does not contain exactly one
	// contract.
	ErrTxnMissingContract = errors.New("transaction set does not contain a file contract")
	// ErrInvalidRenterSignature is returned when a contract's renter signature
	// is invalid.
	ErrInvalidRenterSignature = errors.New("invalid renter signature")
	// ErrContractAlreadyLocked is returned when a renter tries to lock
	// a contract before unlocking the previous one.
	ErrContractAlreadyLocked = errors.New("contract already locked")
	// ErrNotAcceptingContracts is returned when the host is not accepting
	// contracts.
	ErrNotAcceptingContracts = errors.New("host is not accepting contracts")

	// ErrV2Hardfork is returned when a renter tries to form or renew a contract
	// after the v2 hardfork has been activated.
	ErrV2Hardfork = errors.New("hardfork v2 is active")

	// ErrAfterV2Hardfork is returned when a renter tries to form or renew a
	// contract that ends after the v2 hardfork has been activated.
	ErrAfterV2Hardfork = errors.New("proof window after hardfork v2 activation")
)

func (sh *SessionHandler) rpcSettings(s *session, log *zap.Logger) (contracts.Usage, error) {
	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get host settings: %w", err)
	}
	js, err := json.Marshal(settings)
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to marshal settings: %v", err)
	}
	return contracts.Usage{}, s.writeResponse(&rhp2.RPCSettingsResponse{
		Settings: js,
	}, 30*time.Second)
}

func (sh *SessionHandler) rpcLock(s *session, log *zap.Logger) (contracts.Usage, error) {
	var req rhp2.RPCLockRequest
	if err := s.readRequest(&req, minMessageSize, 30*time.Second); err != nil {
		return contracts.Usage{}, err
	}

	// Check if a contract is already locked.
	if s.contract.Revision.ParentID != (types.FileContractID{}) {
		err := ErrContractAlreadyLocked
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		err := fmt.Errorf("failed to lock contract: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// verify the renter's challenge signature
	newChallenge, ok := s.t.VerifyChallenge(req.Signature, contract.RenterKey())
	if !ok {
		sh.contracts.Unlock(contract.Revision.ParentID)
		err := fmt.Errorf("challenge failed: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	s.contract = contract
	lockResp := &rhp2.RPCLockResponse{
		Acquired:     true,
		NewChallenge: newChallenge,
		Revision:     contract.Revision,
		Signatures:   contract.Signatures(),
	}
	if err := s.writeResponse(lockResp, 30*time.Second); err != nil {
		sh.contracts.Unlock(contract.Revision.ParentID)
		s.contract = contracts.SignedRevision{}
		return contracts.Usage{}, fmt.Errorf("failed to write lock response: %w", err)
	}
	return contracts.Usage{}, nil
}

// rpcUnlock unlocks the contract associated with the session.
func (sh *SessionHandler) rpcUnlock(s *session, log *zap.Logger) (contracts.Usage, error) {
	// check if a contract is locked
	if s.contract.Revision.ParentID == (types.FileContractID{}) {
		return contracts.Usage{}, ErrNoContractLocked
	}
	sh.contracts.Unlock(s.contract.Revision.ParentID)
	s.contract = contracts.SignedRevision{}
	return contracts.Usage{}, nil
}

// rpcFormContract is an RPC that forms a contract between a renter and the
// host.
func (sh *SessionHandler) rpcFormContract(s *session, log *zap.Logger) (contracts.Usage, error) {
	cs := sh.chain.TipState()

	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get host settings: %w", err)
	}

	if !settings.AcceptingContracts {
		s.t.WriteResponseErr(ErrNotAcceptingContracts)
		return contracts.Usage{}, ErrNotAcceptingContracts
	}
	var req rhp2.RPCFormContractRequest
	if err := s.readRequest(&req, 10*minMessageSize, time.Minute); err != nil {
		return contracts.Usage{}, err
	}
	formationTxnSet := req.Transactions
	// if the transaction set does not contain any transaction or if the
	// transaction does not contain exactly one file contract, return an error
	if len(formationTxnSet) == 0 || len(formationTxnSet[len(formationTxnSet)-1].FileContracts) != 1 {
		err := ErrTxnMissingContract
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	} else if req.RenterKey.Algorithm != types.SpecifierEd25519 {
		err := errors.New("unsupported renter key algorithm")
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	renterPub := *(*types.PublicKey)(req.RenterKey.Key)
	// get the host's public key, current block height, and settings
	hostPub := sh.privateKey.PublicKey()

	// get the contract from the transaction set
	formationTxn, formationTxnSet := formationTxnSet[len(formationTxnSet)-1], formationTxnSet[:len(formationTxnSet)-1]
	fc := formationTxn.FileContracts[0]

	// prevent forming contracts that end after the v2 hardfork
	if fc.WindowStart >= cs.Network.HardforkV2.RequireHeight {
		err := ErrAfterV2Hardfork
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// validate the contract formation fields. note: the v1 contract type
	// does not contain the public keys or signatures.
	hostCollateral, err := validateContractFormation(formationTxn.FileContracts[0], hostPub.UnlockKey(), renterPub.UnlockKey(), sh.chain.Tip().Height, settings)
	if err != nil {
		err := fmt.Errorf("contract rejected: validation failed: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// calculate the host's collateral and add the inputs to the transaction
	renterInputs, renterOutputs := len(formationTxn.SiacoinInputs), len(formationTxn.SiacoinOutputs)
	toSign, err := sh.wallet.FundTransaction(&formationTxn, hostCollateral, false)
	if err != nil {
		s.t.WriteResponseErr(fmt.Errorf("failed to fund formation transaction: %w", err))
		return contracts.Usage{}, fmt.Errorf("failed to fund formation transaction: %w", err)
	}

	// create an initial revision for the contract
	initialRevision := rhp.InitialRevision(formationTxn, hostPub.UnlockKey(), renterPub.UnlockKey())
	sigHash := rhp.HashRevision(initialRevision)
	hostSig := sh.privateKey.SignHash(sigHash)

	// send the host's transaction funding additions to the renter
	hostAdditionsResp := &rhp2.RPCFormContractAdditions{
		Inputs:  formationTxn.SiacoinInputs[renterInputs:],
		Outputs: formationTxn.SiacoinOutputs[renterOutputs:],
	}
	if err := s.writeResponse(hostAdditionsResp, 30*time.Second); err != nil {
		sh.wallet.ReleaseInputs(append(formationTxnSet, formationTxn), nil)
		return contracts.Usage{}, fmt.Errorf("failed to write host additions: %w", err)
	}

	// read and validate the renter's signatures
	var renterSignaturesResp rhp2.RPCFormContractSignatures
	if err := s.readResponse(&renterSignaturesResp, 10*minMessageSize, 30*time.Second); err != nil {
		sh.wallet.ReleaseInputs(append(formationTxnSet, formationTxn), nil)
		return contracts.Usage{}, fmt.Errorf("failed to read renter signatures: %w", err)
	} else if err := validateRenterRevisionSignature(renterSignaturesResp.RevisionSignature, initialRevision.ParentID, sigHash, renterPub); err != nil {
		sh.wallet.ReleaseInputs(append(formationTxnSet, formationTxn), nil)
		err := fmt.Errorf("contract rejected: validation failed: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	// add the renter's signatures to the transaction and contract revision
	renterTxnSigs := len(renterSignaturesResp.ContractSignatures)
	formationTxn.Signatures = renterSignaturesResp.ContractSignatures

	// sign and broadcast the formation transaction
	sh.wallet.SignTransaction(&formationTxn, toSign, types.CoveredFields{WholeTransaction: true})

	formationTxnSet = append(formationTxnSet, formationTxn)
	if _, err := sh.chain.AddPoolTransactions(formationTxnSet); err != nil {
		sh.wallet.ReleaseInputs(formationTxnSet, nil)
		err = fmt.Errorf("failed to broadcast formation transaction: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	} else if err := sh.syncer.BroadcastTransactionSet(formationTxnSet); err != nil {
		sh.wallet.ReleaseInputs(formationTxnSet, nil)
		err = fmt.Errorf("failed to broadcast formation transaction: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	signedRevision := contracts.SignedRevision{
		Revision:        initialRevision,
		RenterSignature: *(*types.Signature)(renterSignaturesResp.RevisionSignature.Signature),
		HostSignature:   hostSig,
	}
	usage := contracts.Usage{
		RPCRevenue: settings.ContractPrice,
	}
	if err := sh.contracts.AddContract(signedRevision, formationTxnSet, hostCollateral, usage); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to add contract to store: %w", err)
	}

	// send the host signatures to the renter
	hostSignaturesResp := &rhp2.RPCFormContractSignatures{
		ContractSignatures: formationTxn.Signatures[renterTxnSigs:],
		RevisionSignature: types.TransactionSignature{
			ParentID:      types.Hash256(formationTxn.FileContractID(0)),
			Signature:     hostSig[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
	}
	return usage, s.writeResponse(hostSignaturesResp, 30*time.Second)
}

// rpcRenewAndClearContract is an RPC that renews a contract and clears the
// existing contract
func (sh *SessionHandler) rpcRenewAndClearContract(s *session, log *zap.Logger) (contracts.Usage, error) {
	cs := sh.chain.TipState()

	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get host settings: %w", err)
	} else if !settings.AcceptingContracts {
		s.t.WriteResponseErr(ErrNotAcceptingContracts)
		return contracts.Usage{}, ErrNotAcceptingContracts
	}

	hostUnlockKey := sh.privateKey.PublicKey().UnlockKey()

	// make sure the current contract is revisable
	if err := s.ContractRevisable(); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	var req rhp2.RPCRenewAndClearContractRequest
	if err := s.readRequest(&req, 10*minMessageSize, time.Minute); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to read renew request: %w", err)
	}

	renterKey, err := convertToPublicKey(req.RenterKey)
	if err != nil {
		err = fmt.Errorf("failed to convert renter key: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	renewalTxnSet := req.Transactions
	if len(renewalTxnSet) == 0 || len(renewalTxnSet[len(renewalTxnSet)-1].FileContracts) != 1 {
		err := ErrTxnMissingContract
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	renewalTxn, renewalParents := renewalTxnSet[len(renewalTxnSet)-1], renewalTxnSet[:len(renewalTxnSet)-1]
	renewedContract := renewalTxn.FileContracts[0]

	// prevent forming contracts that end after the v2 hardfork
	if renewedContract.WindowStart >= cs.Network.HardforkV2.RequireHeight {
		err := ErrAfterV2Hardfork
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	existingRevision := s.contract.Revision
	clearingRevision, err := rhp.ClearingRevision(existingRevision, req.FinalValidProofValues)
	if err != nil {
		err = fmt.Errorf("failed to create clearing revision: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	expectedExchange := settings.BaseRPCPrice
	// if the contract has less than the host's current base RPC price, cap
	// the exchange at the contract's remaining value
	if expectedExchange.Cmp(existingRevision.ValidRenterPayout()) > 0 {
		expectedExchange = existingRevision.ValidRenterPayout()
	}
	finalPayment, err := rhp.ValidateClearingRevision(existingRevision, clearingRevision, expectedExchange)
	if err != nil {
		err = fmt.Errorf("invalid clearing revision: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	clearingUsage := contracts.Usage{
		RPCRevenue: finalPayment,
	}

	// calculate the "base" storage cost to the renter and risked collateral for
	// the host for the data already in the contract. If the contract height did
	// not increase, base costs are zero since the storage is already paid for.
	baseRevenue := settings.ContractPrice
	var baseCollateral types.Currency
	if renewedContract.WindowEnd > existingRevision.WindowEnd {
		extension := uint64(renewedContract.WindowEnd - existingRevision.WindowEnd)
		baseRevenue = baseRevenue.Add(settings.StoragePrice.Mul64(renewedContract.Filesize).Mul64(extension))
		baseCollateral = settings.Collateral.Mul64(renewedContract.Filesize).Mul64(extension)
	}

	// validate the renewal
	baseRevenue, riskedCollateral, lockedCollateral, err := validateContractRenewal(existingRevision, renewedContract, hostUnlockKey, req.RenterKey, baseRevenue, baseCollateral, sh.chain.Tip().Height, settings)
	if err != nil {
		err = fmt.Errorf("invalid contract renewal: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	renewalUsage := contracts.Usage{
		RPCRevenue:       settings.ContractPrice,
		RiskedCollateral: riskedCollateral,
		StorageRevenue:   baseRevenue.Sub(settings.ContractPrice),
	}

	renterInputs, renterOutputs := len(renewalTxn.SiacoinInputs), len(renewalTxn.SiacoinOutputs)
	toSign, err := sh.wallet.FundTransaction(&renewalTxn, lockedCollateral, false)
	if err != nil {
		s.t.WriteResponseErr(fmt.Errorf("failed to fund renewal transaction: %w", err))
		return contracts.Usage{}, fmt.Errorf("failed to fund renewal transaction: %w", err)
	}

	// send the renter the host additions to the renewal txn
	hostAdditionsResp := &rhp2.RPCFormContractAdditions{
		Inputs:  renewalTxn.SiacoinInputs[renterInputs:],
		Outputs: renewalTxn.SiacoinOutputs[renterOutputs:],
	}
	if err = s.writeResponse(hostAdditionsResp, 30*time.Second); err != nil {
		sh.wallet.ReleaseInputs(append(renewalParents, renewalTxn), nil)
		return contracts.Usage{}, fmt.Errorf("failed to write host additions: %w", err)
	}

	// read the renter's signatures for the renewal
	var renterSigsResp rhp2.RPCRenewAndClearContractSignatures
	if err = s.readResponse(&renterSigsResp, minMessageSize, 30*time.Second); err != nil {
		sh.wallet.ReleaseInputs(append(renewalParents, renewalTxn), nil)
		return contracts.Usage{}, fmt.Errorf("failed to read renter signatures: %w", err)
	} else if len(renterSigsResp.RevisionSignature.Signature) != 64 {
		sh.wallet.ReleaseInputs(append(renewalParents, renewalTxn), nil)
		return contracts.Usage{}, fmt.Errorf("invalid renter signature length: %w", ErrInvalidRenterSignature)
	}

	// add the renter's signatures to the formation transaction
	renewalTxn.Signatures = append(renewalTxn.Signatures, renterSigsResp.ContractSignatures...)
	// sign the transaction
	sh.wallet.SignTransaction(&renewalTxn, toSign, types.CoveredFields{WholeTransaction: true})
	// create the initial revision
	initialRevision := rhp.InitialRevision(renewalTxn, hostUnlockKey, req.RenterKey)

	// verify the clearing revision signature
	clearingRevSigHash := rhp.HashRevision(clearingRevision)
	// important: verify using the existing contract's renter key
	if !s.contract.RenterKey().VerifyHash(clearingRevSigHash, renterSigsResp.FinalRevisionSignature) {
		sh.wallet.ReleaseInputs(append(renewalParents, renewalTxn), nil)
		err := fmt.Errorf("failed to verify clearing revision signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// verify the renewal revision signature
	renewalSigHash := rhp.HashRevision(initialRevision)
	renterRenewalSig := *(*types.Signature)(renterSigsResp.RevisionSignature.Signature)
	if !renterKey.VerifyHash(renewalSigHash, renterRenewalSig) {
		sh.wallet.ReleaseInputs(append(renewalParents, renewalTxn), nil)
		err := fmt.Errorf("failed to verify renewal revision signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	signedClearing := contracts.SignedRevision{
		Revision:        clearingRevision,
		RenterSignature: renterSigsResp.FinalRevisionSignature,
		HostSignature:   sh.privateKey.SignHash(clearingRevSigHash),
	}
	signedRenewal := contracts.SignedRevision{
		Revision:        initialRevision,
		RenterSignature: renterRenewalSig,
		HostSignature:   sh.privateKey.SignHash(renewalSigHash),
	}

	// validate & broadcast the transaction
	renewalTxnSet = append(renewalParents, renewalTxn)
	if _, err = sh.chain.AddPoolTransactions(renewalTxnSet); err != nil {
		sh.wallet.ReleaseInputs(renewalTxnSet, nil)
		err = fmt.Errorf("failed to broadcast renewal transaction: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	} else if err := sh.syncer.BroadcastTransactionSet(renewalTxnSet); err != nil {
		sh.wallet.ReleaseInputs(renewalTxnSet, nil)
		err = fmt.Errorf("failed to broadcast renewal transaction: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// update the existing contract and add the renewed contract to the store
	if err := sh.contracts.RenewContract(signedRenewal, signedClearing, renewalTxnSet, lockedCollateral, clearingUsage, renewalUsage); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to renew contract: %w", err)
	}

	// send the host signatures to the renter
	hostSigsResp := &rhp2.RPCRenewAndClearContractSignatures{
		ContractSignatures:     renewalTxn.Signatures[len(renterSigsResp.ContractSignatures):],
		RevisionSignature:      signedRenewal.Signatures()[1],
		FinalRevisionSignature: signedClearing.HostSignature,
	}
	return clearingUsage.Add(renewalUsage), s.writeResponse(hostSigsResp, 30*time.Second)
}

// rpcSectorRoots returns the Merkle roots of the sectors in a contract
func (sh *SessionHandler) rpcSectorRoots(s *session, log *zap.Logger) (contracts.Usage, error) {
	if err := s.ContractRevisable(); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	contractSectors := s.contract.Revision.Filesize / rhp2.SectorSize

	var req rhp2.RPCSectorRootsRequest
	if err := s.readRequest(&req, minMessageSize, 30*time.Second); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to read sector roots request: %w", err)
	}

	start := req.RootOffset
	end := req.RootOffset + req.NumRoots

	if end > contractSectors {
		err := fmt.Errorf("invalid sector range: %d-%d, contract has %d sectors", start, end, contractSectors)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get host settings: %w", err)
	}

	costs := settings.RPCSectorRootsCost(req.RootOffset, req.NumRoots)
	cost, _ := costs.Total()

	// revise the contract
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	payment, _, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// validate the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		err := fmt.Errorf("failed to validate revision: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	hostSig := sh.privateKey.SignHash(sigHash)

	if req.NumRoots > math.MaxInt {
		err := errors.New("too many requested sector roots")
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	} else if req.RootOffset > math.MaxInt {
		err := errors.New("sector root offset is too large")
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	roots := sh.contracts.SectorRoots(s.contract.Revision.ParentID)
	if uint64(len(roots)) != contractSectors {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("inconsistent sector roots: expected %v, got %v", contractSectors, len(roots))
	}

	// commit the revision
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		RenterSignature: req.Signature,
		HostSignature:   hostSig,
	}
	updater, err := sh.contracts.ReviseContract(revision.ParentID)
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to revise contract: %w", err)
	}
	defer updater.Close()

	// adjust the revenue to account for the full transfer by the renter
	excess, underflow := payment.SubWithUnderflow(cost)
	if !underflow {
		costs.Egress = costs.Egress.Add(excess)
	}

	usage := newUsageFromRPCCost(costs)
	if err := updater.Commit(signedRevision, usage); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to commit contract revision: %w", err)
	}
	s.contract = signedRevision
	sectorRootsResp := &rhp2.RPCSectorRootsResponse{
		SectorRoots: roots[start:end],
		MerkleProof: rhp2.BuildSectorRangeProof(roots, start, end),
		Signature:   hostSig,
	}
	return usage, s.writeResponse(sectorRootsResp, 2*time.Minute)
}

func (sh *SessionHandler) rpcWrite(s *session, log *zap.Logger) (contracts.Usage, error) {
	currentHeight := sh.chain.Tip().Height
	// get the locked contract and check that it is revisable
	if err := s.ContractRevisable(); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get settings: %w", err)
	}

	var req rhp2.RPCWriteRequest
	if err := s.readRequest(&req, 5*rhp2.SectorSize, 5*time.Minute); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to read write request: %w", err)
	}

	remainingDuration := uint64(s.contract.Revision.WindowEnd) - currentHeight
	// validate the requested actions
	oldSectors := s.contract.Revision.Filesize / rhp2.SectorSize
	costs, err := settings.RPCWriteCost(req.Actions, oldSectors, remainingDuration, req.MerkleProof)
	if err != nil {
		err := fmt.Errorf("failed to validate write actions: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	cost, collateral := costs.Total()

	// revise and validate the new revision
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	payment, risked, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, collateral)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	contractUpdater, err := sh.contracts.ReviseContract(revision.ParentID)
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to revise contract: %w", err)
	}
	defer contractUpdater.Close()

	oldRoots := contractUpdater.SectorRoots()
	for _, action := range req.Actions {
		switch action.Type {
		case rhp2.RPCWriteActionAppend:
			if len(action.Data) != rhp2.SectorSize {
				err := fmt.Errorf("append action: invalid sector size: %v", len(action.Data))
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}
			sector := (*[rhp2.SectorSize]byte)(action.Data)
			root := rhp2.SectorRoot(sector)
			err := sh.sectors.Write(root, sector)
			if err != nil {
				err := fmt.Errorf("append action: failed to write sector: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}
			contractUpdater.AppendSector(root)
		case rhp2.RPCWriteActionTrim:
			if err := contractUpdater.TrimSectors(action.A); err != nil {
				err := fmt.Errorf("trim action: failed to trim sectors: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}
		case rhp2.RPCWriteActionSwap:
			if err := contractUpdater.SwapSectors(action.A, action.B); err != nil {
				err := fmt.Errorf("swap action: failed to swap sectors: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}
		case rhp2.RPCWriteActionUpdate:
			root, err := contractUpdater.SectorRoot(action.A)
			if err != nil {
				err := fmt.Errorf("update action: failed to get sector root: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}

			sector, err := sh.sectors.ReadSector(root)
			if err != nil {
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, fmt.Errorf("failed to read sector %v: %w", root, err)
			}

			i, offset := action.A, action.B
			if offset > rhp2.SectorSize {
				err := fmt.Errorf("update action: invalid offset %v bytes", offset)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			} else if offset+uint64(len(action.Data)) > rhp2.SectorSize {
				err := errors.New("update action: offset + data exceeds sector size")
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}

			copy(sector[offset:], action.Data)
			newRoot := rhp2.SectorRoot(sector)

			if err := contractUpdater.UpdateSector(newRoot, i); err != nil {
				err := fmt.Errorf("update action: failed to update sector: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}

			if err = sh.sectors.Write(root, sector); err != nil {
				err := fmt.Errorf("append action: failed to write sector: %w", err)
				s.t.WriteResponseErr(err)
				return contracts.Usage{}, err
			}
		}
	}

	// build the merkle proof response
	writeResp := &rhp2.RPCWriteMerkleProof{
		NewMerkleRoot: contractUpdater.MerkleRoot(),
	}
	if req.MerkleProof {
		writeResp.OldSubtreeHashes, writeResp.OldLeafHashes = rhp2.BuildDiffProof(req.Actions, oldRoots)
	}
	if err := s.writeResponse(writeResp, 5*time.Minute); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to write merkle proof: %w", err)
	}

	// apply the new merkle root and file size to the revision
	revision.FileMerkleRoot = writeResp.NewMerkleRoot
	revision.Filesize = contractUpdater.SectorCount() * rhp2.SectorSize

	// read the renter's signature
	var renterSigResponse rhp2.RPCWriteResponse
	if err := s.readResponse(&renterSigResponse, minMessageSize, 30*time.Second); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to read renter signature: %w", err)
	}

	// validate the contract signature
	renterSig := renterSigResponse.Signature
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, renterSigResponse.Signature) {
		err := fmt.Errorf("failed to verify renter signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}
	hostSig := sh.privateKey.SignHash(sigHash)
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostSig,
		RenterSignature: renterSig,
	}

	// sync the storage manager
	if err := sh.sectors.Sync(); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to sync storage manager: %w", err)
	}

	// adjust the revenue to account for the full transfer by the renter
	excess, underflow := payment.SubWithUnderflow(cost)
	if !underflow {
		costs.Storage = costs.Storage.Add(excess)
	}
	// adjust the collateral to account for the transfer by the host -- the host
	// can risk less than the expected amount
	costs.Collateral = risked

	// commit the contract modifications
	usage := newUsageFromRPCCost(costs)
	if err := contractUpdater.Commit(signedRevision, usage); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to commit contract modifications: %w", err)
	}
	// update the session contract
	s.contract = signedRevision

	// send the host signature
	hostSigResp := &rhp2.RPCWriteResponse{Signature: hostSig}
	return usage, s.writeResponse(hostSigResp, 30*time.Second)
}

func (sh *SessionHandler) rpcRead(s *session, log *zap.Logger) (contracts.Usage, error) {
	// get the locked contract and check that it is revisable
	if err := s.ContractRevisable(); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// get the host's current settings
	settings, err := sh.settings.RHP2Settings()
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to get host settings: %w", err)
	}

	// read the read request
	var req rhp2.RPCReadRequest
	if err := s.readRequest(&req, 4*minMessageSize, time.Minute); err != nil {
		return contracts.Usage{}, fmt.Errorf("failed to read read request: %w", err)
	}

	// validate the request sections and calculate the cost
	costs, err := settings.RPCReadCost(req.Sections, req.MerkleProof)
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to validate read request: %w", err)
	}
	cost, _ := costs.Total()

	// revise the contract with the values sent by the renter
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// validate the renter's signature and transfer
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		err := fmt.Errorf("failed to validate revision: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	payment, _, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, err
	}

	// sign and commit the new revision
	hostSig := sh.privateKey.SignHash(sigHash)
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostSig,
		RenterSignature: req.Signature,
	}

	updater, err := sh.contracts.ReviseContract(revision.ParentID)
	if err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to revise contract: %w", err)
	}
	defer updater.Close()

	// adjust the revenue to account for the full transfer by the renter
	excess, underflow := payment.SubWithUnderflow(cost)
	if !underflow {
		costs.Egress = costs.Egress.Add(excess)
	}
	// commit the contract revision
	usage := newUsageFromRPCCost(costs)
	if err := updater.Commit(signedRevision, usage); err != nil {
		s.t.WriteResponseErr(err)
		return contracts.Usage{}, fmt.Errorf("failed to commit contract revision: %w", err)
	}
	// update the session contract
	s.contract = signedRevision

	// listen for RPCLoopReadStop
	stopSignal := make(chan error, 1)
	go func() {
		var id types.Specifier
		// long timeout because the renter may be slow to send the stop signal
		err := s.readResponse(&id, minMessageSize, 5*time.Minute)
		if err != nil {
			stopSignal <- err
		} else if id != rhp2.RPCReadStop {
			stopSignal <- errors.New("expected 'stop' from renter, got " + id.String())
		} else {
			stopSignal <- nil
		}
	}()

	// enter response loop
	for i, sec := range req.Sections {
		sector, err := sh.sectors.ReadSector(sec.MerkleRoot)
		if err != nil {
			err := fmt.Errorf("failed to get sector: %w", err)
			s.t.WriteResponseErr(err)
			return usage, err
		}

		resp := &rhp2.RPCReadResponse{
			Data: sector[sec.Offset : sec.Offset+sec.Length],
		}
		if req.MerkleProof {
			start := sec.Offset / rhp2.LeafSize
			end := (sec.Offset + sec.Length) / rhp2.LeafSize
			resp.MerkleProof = rhp2.BuildProof(sector, start, end, nil)
		}

		// check for the stop signal and send the response
		select {
		case err := <-stopSignal:
			if err != nil {
				return usage, err
			}
			resp.Signature = hostSig
			return usage, s.writeResponse(resp, 30*time.Second)
		default:
		}

		if i == len(req.Sections)-1 {
			resp.Signature = hostSig
		}
		if err := s.writeResponse(resp, 30*time.Second); err != nil {
			return usage, fmt.Errorf("failed to write read response: %w", err)
		}
	}
	return usage, <-stopSignal
}

// newUsageFromRPCCost returns a new Usage from the given RPCCost.
func newUsageFromRPCCost(c rhp2.RPCCost) contracts.Usage {
	return contracts.Usage{
		RPCRevenue:       c.Base,
		StorageRevenue:   c.Storage,
		EgressRevenue:    c.Egress,
		IngressRevenue:   c.Ingress,
		RiskedCollateral: c.Collateral,
	}
}

func convertToPublicKey(uc types.UnlockKey) (types.PublicKey, error) {
	if uc.Algorithm != types.SpecifierEd25519 {
		return types.PublicKey{}, errors.New("unsupported signature algorithm")
	} else if len(uc.Key) != ed25519.PublicKeySize {
		return types.PublicKey{}, errors.New("invalid public key length")
	}
	return *(*types.PublicKey)(uc.Key), nil
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
