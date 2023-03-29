package rhp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
)

var (
	// ErrTxnMissingContract is returned if the transaction set does not contain
	// any transactions or if the transaction does not contain exactly one
	// contract.
	ErrTxnMissingContract = errors.New("transaction set does not contain a file contract")
	// ErrHostInternalError is returned if the host encountered an error during
	// an RPC that doesn't need to be broadcast to the renter (e.g. insufficient
	// funds).
	ErrHostInternalError = errors.New("host internal error")
	// ErrInvalidRenterSignature is returned when a contract's renter signature
	// is invalid.
	ErrInvalidRenterSignature = errors.New("invalid renter signature")

	// ErrContractAlreadyLocked is returned when a renter tries to lock
	// a contract before unlocking the previous one.
	ErrContractAlreadyLocked = errors.New("contract already locked")
)

func (sh *SessionHandler) rpcSettings(s *session) error {
	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get host settings: %w", err)
	}
	js, err := json.Marshal(settings)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to marshal settings: %v", err)
	}
	return s.writeResponse(&rhpv2.RPCSettingsResponse{
		Settings: js,
	}, 30*time.Second)
}

func (sh *SessionHandler) rpcLock(s *session) error {
	var req rhpv2.RPCLockRequest
	if err := s.readRequest(&req, minMessageSize, 30*time.Second); err != nil {
		return err
	}

	// Check if a contract is already locked.
	if s.contract.Revision.ParentID != (types.FileContractID{}) {
		err := ErrContractAlreadyLocked
		s.t.WriteResponseErr(err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contract, err := sh.contracts.Lock(ctx, req.ContractID)
	if err != nil {
		err := fmt.Errorf("failed to lock contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// verify the renter's challenge signature
	newChallenge, ok := s.t.VerifyChallenge(req.Signature, contract.RenterKey())
	if !ok {
		sh.contracts.Unlock(contract.Revision.ParentID)
		err := fmt.Errorf("challenge failed: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	}

	// set the contract
	s.contract = contract
	lockResp := &rhpv2.RPCLockResponse{
		Acquired:     true,
		NewChallenge: newChallenge,
		Revision:     contract.Revision,
		Signatures:   contract.Signatures(),
	}
	// avoid holding lock during network round trip
	if err := s.writeResponse(lockResp, 30*time.Second); err != nil {
		sh.contracts.Unlock(contract.Revision.ParentID)
		return fmt.Errorf("failed to write lock response: %w", err)
	}
	return nil
}

// rpcUnlock unlocks the contract associated with the session.
func (sh *SessionHandler) rpcUnlock(s *session) error {
	// check if a contract is locked
	if s.contract.Revision.ParentID == (types.FileContractID{}) {
		return ErrNoContractLocked
	}
	sh.contracts.Unlock(s.contract.Revision.ParentID)
	s.contract = contracts.SignedRevision{}
	return nil
}

// rpcFormContract is an RPC that forms a contract between a renter and the
// host.
func (sh *SessionHandler) rpcFormContract(s *session) error {
	var req rhpv2.RPCFormContractRequest
	if err := s.readRequest(&req, 10*minMessageSize, time.Minute); err != nil {
		return err
	}
	formationTxnSet := req.Transactions
	// if the transaction set does not contain any transaction or if the
	// transaction does not contain exactly one file contract, return an error
	if len(formationTxnSet) == 0 || len(formationTxnSet[len(formationTxnSet)-1].FileContracts) != 1 {
		err := ErrTxnMissingContract
		s.t.WriteResponseErr(err)
		return err
	} else if req.RenterKey.Algorithm != types.SpecifierEd25519 {
		err := errors.New("unsupported renter key algorithm")
		s.t.WriteResponseErr(err)
		return err
	}
	renterPub := *(*types.PublicKey)(req.RenterKey.Key)
	// get the host's public key, current block height, and settings
	hostPub := sh.privateKey.PublicKey()
	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get host settings: %w", err)
	}
	currentHeight := sh.cm.TipState().Index.Height
	// get the contract from the transaction set
	formationTxn := &formationTxnSet[len(formationTxnSet)-1]

	// validate the contract formation fields. note: the v1 contract type
	// does not contain the public keys or signatures.
	hostCollateral, err := validateContractFormation(formationTxn.FileContracts[0], hostPub.UnlockKey(), renterPub.UnlockKey(), currentHeight, settings)
	if err != nil {
		err := fmt.Errorf("contract rejected: validation failed: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// calculate the host's collateral and add the inputs to the transaction
	renterInputs, renterOutputs := len(formationTxn.SiacoinInputs), len(formationTxn.SiacoinOutputs)
	toSign, discard, err := sh.wallet.FundTransaction(formationTxn, hostCollateral)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to fund formation transaction: %w", err)
	}
	defer discard()

	// create an initial revision for the contract
	initialRevision := rhp.InitialRevision(formationTxn, hostPub.UnlockKey(), renterPub.UnlockKey())
	sigHash := rhp.HashRevision(initialRevision)
	hostSig := sh.privateKey.SignHash(sigHash)

	// send the host's transaction funding additions to the renter
	hostAdditionsResp := &rhpv2.RPCFormContractAdditions{
		Inputs:  formationTxn.SiacoinInputs[renterInputs:],
		Outputs: formationTxn.SiacoinOutputs[renterOutputs:],
	}
	if err := s.writeResponse(hostAdditionsResp, 30*time.Second); err != nil {
		return fmt.Errorf("failed to write host additions: %w", err)
	}

	// read the renter's signatures
	var renterSignaturesResp rhpv2.RPCFormContractSignatures
	if err := s.readResponse(&renterSignaturesResp, 10*minMessageSize, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read renter signatures: %w", err)
	}
	// validate the renter's initial revision signature
	if len(renterSignaturesResp.RevisionSignature.Signature) != 64 {
		s.t.WriteResponseErr(ErrInvalidRenterSignature)
		return ErrInvalidRenterSignature
	}
	renterSig := *(*types.Signature)(renterSignaturesResp.RevisionSignature.Signature)
	if !renterPub.VerifyHash(sigHash, renterSig) {
		s.t.WriteResponseErr(ErrInvalidRenterSignature)
		return ErrInvalidRenterSignature
	}
	// add the renter's signatures to the transaction and contract revision
	renterTxnSigs := len(renterSignaturesResp.ContractSignatures)
	formationTxn.Signatures = renterSignaturesResp.ContractSignatures

	// sign and broadcast the formation transaction
	if err = sh.wallet.SignTransaction(sh.cm.TipState(), formationTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to sign formation transaction: %w", err)
	} else if err = sh.tpool.AcceptTransactionSet(formationTxnSet); err != nil {
		err = fmt.Errorf("failed to broadcast formation transaction: %w", err)
		buf, _ := json.Marshal(formationTxnSet)
		sh.log.Error("failed to broadcast formation transaction", zap.Error(err), zap.String("txnset", string(buf)))
		s.t.WriteResponseErr(err)
		return err
	}

	signedRevision := contracts.SignedRevision{
		Revision:        initialRevision,
		RenterSignature: renterSig,
		HostSignature:   hostSig,
	}
	usage := contracts.Usage{
		RPCRevenue: settings.ContractPrice,
	}
	if err := sh.contracts.AddContract(signedRevision, formationTxnSet, hostCollateral, usage); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to add contract to store: %w", err)
	}

	// add the contract fee to the amount spent by the renter
	s.Spend(settings.ContractPrice)
	// log the formation event
	sh.metrics.Report(EventContractFormed{
		SessionUID: s.uid,
		ContractID: formationTxn.FileContractID(0),
		Contract:   initialRevision,
	})

	// send the host signatures to the renter
	hostSignaturesResp := &rhpv2.RPCFormContractSignatures{
		ContractSignatures: formationTxn.Signatures[renterTxnSigs:],
		RevisionSignature: types.TransactionSignature{
			ParentID:      types.Hash256(formationTxn.FileContractID(0)),
			Signature:     hostSig[:],
			CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
		},
	}
	if err := s.writeResponse(hostSignaturesResp, 30*time.Second); err != nil {
		return fmt.Errorf("failed to write host signatures: %w", err)
	}
	return nil
}

// rpcRenewAndClearContract is an RPC that renews a contract and clears the
// existing contract
func (sh *SessionHandler) rpcRenewAndClearContract(s *session) error {
	state := sh.cm.TipState()
	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get host settings: %w", err)
	}

	hostUnlockKey := sh.privateKey.PublicKey().UnlockKey()

	// make sure the current contract is revisable
	if err := s.ContractRevisable(state.Index.Height); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return err
	} else if !settings.AcceptingContracts {
		err := fmt.Errorf("host is not accepting contracts")
		s.t.WriteResponseErr(err)
		return err
	}

	var req rhpv2.RPCRenewAndClearContractRequest
	if err := s.readRequest(&req, 10*minMessageSize, time.Minute); err != nil {
		return fmt.Errorf("failed to read renew request: %w", err)
	}

	renterKey, err := convertToPublicKey(req.RenterKey)
	if err != nil {
		err = fmt.Errorf("failed to convert renter key: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	renewalTxnSet := req.Transactions
	if len(renewalTxnSet) == 0 || len(renewalTxnSet[len(renewalTxnSet)-1].FileContracts) != 1 {
		err := ErrTxnMissingContract
		s.t.WriteResponseErr(err)
		return err
	}
	renewalParents := renewalTxnSet[:len(renewalTxnSet)-1]
	renewalTxn := renewalTxnSet[len(renewalTxnSet)-1]
	renewedContract := renewalTxn.FileContracts[0]

	existingRevision := s.contract.Revision
	clearingRevision, err := rhp.ClearingRevision(existingRevision, req.FinalValidProofValues)
	if err != nil {
		err = fmt.Errorf("failed to create clearing revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	finalPayment, err := rhp.ValidateClearingRevision(existingRevision, clearingRevision, types.ZeroCurrency)
	if err != nil {
		err = fmt.Errorf("invalid clearing revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	clearingUsage := contracts.Usage{
		RPCRevenue: finalPayment,
	}

	// calculate the "base" storage cost to the renter and risked collateral for
	// the host for the data already in the contract. If the contract height did
	// not increase, base costs are zero since the storage is already paid for.
	baseRevenue := settings.ContractPrice
	var baseCollateral types.Currency
	if renewedContract.WindowStart > existingRevision.WindowStart {
		extension := uint64(renewedContract.WindowStart - existingRevision.WindowStart)
		baseRevenue = baseRevenue.Add(settings.StoragePrice.Mul64(renewedContract.Filesize).Mul64(extension))
		baseCollateral = settings.Collateral.Mul64(renewedContract.Filesize).Mul64(extension)
	}

	// validate the renewal
	baseRevenue, riskedCollateral, lockedCollateral, err := validateContractRenewal(existingRevision, renewedContract, hostUnlockKey, req.RenterKey, baseRevenue, baseCollateral, state.Index.Height, settings)
	if err != nil {
		err = fmt.Errorf("invalid contract renewal: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	renewalUsage := contracts.Usage{
		RPCRevenue:       settings.ContractPrice,
		RiskedCollateral: riskedCollateral,
		StorageRevenue:   baseRevenue.Sub(settings.ContractPrice),
	}

	renterInputs, renterOutputs := len(renewalTxn.SiacoinInputs), len(renewalTxn.SiacoinOutputs)
	toSign, discard, err := sh.wallet.FundTransaction(&renewalTxn, lockedCollateral)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to fund renewal transaction: %w", err)
	}
	defer discard()

	// send the renter the host additions to the renewal txn
	hostAdditionsResp := &rhpv2.RPCFormContractAdditions{
		Inputs:  renewalTxn.SiacoinInputs[renterInputs:],
		Outputs: renewalTxn.SiacoinOutputs[renterOutputs:],
	}
	if err = s.writeResponse(hostAdditionsResp, 30*time.Second); err != nil {
		return fmt.Errorf("failed to write host additions: %w", err)
	}

	// read the renter's signatures for the renewal
	var renterSigsResp rhpv2.RPCRenewAndClearContractSignatures
	if err = s.readResponse(&renterSigsResp, 4096, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read renter signatures: %w", err)
	} else if len(renterSigsResp.RevisionSignature.Signature) != 64 {
		return fmt.Errorf("invalid renter signature length: %w", ErrInvalidRenterSignature)
	}

	// add the renter's signatures to the formation transaction
	renewalTxn.Signatures = append(renewalTxn.Signatures, renterSigsResp.ContractSignatures...)
	// sign the transaction
	if err = sh.wallet.SignTransaction(state, &renewalTxn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to sign renewal transaction: %w", err)
	}

	// create the initial revision
	initialRevision := rhp.InitialRevision(&renewalTxn, hostUnlockKey, req.RenterKey)

	// verify the clearing revision signature
	clearingRevSigHash := rhp.HashRevision(clearingRevision)
	// important: verify using the existing contract's renter key
	if !s.contract.RenterKey().VerifyHash(clearingRevSigHash, renterSigsResp.FinalRevisionSignature) {
		err := fmt.Errorf("failed to verify clearing revision signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	}

	// verify the renewal revision signature
	renewalSigHash := rhp.HashRevision(initialRevision)
	renterRenewalSig := *(*types.Signature)(renterSigsResp.RevisionSignature.Signature)
	if !renterKey.VerifyHash(renewalSigHash, renterRenewalSig) {
		err := fmt.Errorf("failed to verify renewal revision signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
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

	// broadcast the transaction
	renewalTxnSet = append(renewalParents, renewalTxn)
	if err = sh.tpool.AcceptTransactionSet(renewalTxnSet); err != nil {
		err = fmt.Errorf("failed to broadcast renewal transaction: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	// update the existing contract and add the renewed contract to the store
	if err := sh.contracts.RenewContract(signedRenewal, signedClearing, renewalTxnSet, lockedCollateral, clearingUsage, renewalUsage); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to renew contract: %w", err)
	}

	// send the host signatures to the renter
	hostSigsResp := &rhpv2.RPCRenewAndClearContractSignatures{
		ContractSignatures:     renewalTxn.Signatures[len(renterSigsResp.ContractSignatures):],
		RevisionSignature:      signedRenewal.Signatures()[0],
		FinalRevisionSignature: signedClearing.HostSignature,
	}
	if err := s.writeResponse(hostSigsResp, 30*time.Second); err != nil {
		return fmt.Errorf("failed to write host signatures: %w", err)
	}
	return nil
}

// rpcSectorRoots returns the Merkle roots of the sectors in a contract
func (sh *SessionHandler) rpcSectorRoots(s *session) error {
	currentHeight := sh.cm.TipState().Index.Height
	if err := s.ContractRevisable(currentHeight); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	var req rhpv2.RPCSectorRootsRequest
	if err := s.readRequest(&req, minMessageSize, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read sector roots request: %w", err)
	}

	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get host settings: %w", err)
	}

	costs := rpcSectorRootsCost(req.NumRoots, req.RootOffset, settings)
	cost, _ := costs.Total()

	// revise the contract
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	payment, _, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// validate the renter's signature
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		err := fmt.Errorf("failed to validate revision: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	}
	hostSig := sh.privateKey.SignHash(sigHash)

	roots, err := sh.contracts.SectorRoots(s.contract.Revision.ParentID, req.NumRoots, req.RootOffset)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get sector roots: %w", err)
	}

	// commit the revision
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		RenterSignature: req.Signature,
		HostSignature:   hostSig,
	}
	updater, err := sh.contracts.ReviseContract(revision.ParentID)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	defer updater.Close()

	// adjust the revenue to account for the full transfer by the renter
	excess, underflow := payment.SubWithUnderflow(cost)
	if !underflow {
		costs.Egress = costs.Egress.Add(excess)
	}

	if err := updater.Commit(signedRevision, costs.ToUsage()); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit contract revision: %w", err)
	}
	s.contract = signedRevision

	sectorRootsResp := &rhpv2.RPCSectorRootsResponse{
		SectorRoots: roots,
		MerkleProof: rhpv2.BuildSectorRangeProof(roots, req.RootOffset, req.RootOffset+req.NumRoots),
		Signature:   hostSig,
	}
	if err := s.writeResponse(sectorRootsResp, 2*time.Minute); err != nil {
		return fmt.Errorf("failed to write sector roots response: %w", err)
	}

	s.Spend(cost)
	return nil
}

func (sh *SessionHandler) rpcWrite(s *session) error {
	currentHeight := sh.cm.TipState().Index.Height
	// get the locked contract and check that it is revisable
	if err := s.ContractRevisable(currentHeight); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get settings: %w", err)
	}

	var req rhpv2.RPCWriteRequest
	if err := s.readRequest(&req, 5*rhpv2.SectorSize, 5*time.Minute); err != nil {
		return fmt.Errorf("failed to read write request: %w", err)
	}

	remainingDuration := uint64(s.contract.Revision.WindowStart) - currentHeight
	// validate the requested actions
	oldSectors := s.contract.Revision.Filesize / rhpv2.SectorSize
	costs, err := validateWriteActions(req.Actions, oldSectors, req.MerkleProof, remainingDuration, settings)
	if err != nil {
		err := fmt.Errorf("failed to validate write actions: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}
	cost, collateral := costs.Total()

	// revise and validate the new revision
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	payment, risked, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, collateral)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	contractUpdater, err := sh.contracts.ReviseContract(revision.ParentID)
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	defer contractUpdater.Close()

	oldRoots := contractUpdater.SectorRoots()
	for _, action := range req.Actions {
		switch action.Type {
		case rhpv2.RPCWriteActionAppend:
			if len(action.Data) != rhpv2.SectorSize {
				err := fmt.Errorf("append action: invalid sector size: %v", len(action.Data))
				s.t.WriteResponseErr(err)
				return err
			}
			sector := (*[rhpv2.SectorSize]byte)(action.Data)
			root := rhpv2.SectorRoot(sector)
			release, err := sh.storage.Write(root, sector)
			if err != nil {
				err := fmt.Errorf("append action: failed to write sector: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}
			defer release()
			contractUpdater.AppendSector(root)
		case rhpv2.RPCWriteActionTrim:
			if err := contractUpdater.TrimSectors(action.A); err != nil {
				err := fmt.Errorf("trim action: failed to trim sectors: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}
		case rhpv2.RPCWriteActionSwap:
			if err := contractUpdater.SwapSectors(action.A, action.B); err != nil {
				err := fmt.Errorf("swap action: failed to swap sectors: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}
		case rhpv2.RPCWriteActionUpdate:
			root, err := contractUpdater.SectorRoot(action.A)
			if err != nil {
				err := fmt.Errorf("update action: failed to get sector root: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}

			sector, err := sh.storage.Read(root)
			if err != nil {
				s.t.WriteResponseErr(ErrHostInternalError)
				return fmt.Errorf("failed to read sector %v: %w", root, err)
			}

			i, offset := action.A, action.B
			if offset > rhpv2.SectorSize {
				err := fmt.Errorf("update action: invalid offset %v bytes", offset)
				s.t.WriteResponseErr(err)
				return err
			} else if offset+uint64(len(action.Data)) > rhpv2.SectorSize {
				err := errors.New("update action: offset + data exceeds sector size")
				s.t.WriteResponseErr(err)
				return err
			}

			copy(sector[offset:], action.Data)
			newRoot := rhpv2.SectorRoot(sector)

			if err := contractUpdater.UpdateSector(newRoot, i); err != nil {
				err := fmt.Errorf("update action: failed to update sector: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}
			release, err := sh.storage.Write(root, sector)
			if err != nil {
				err := fmt.Errorf("append action: failed to write sector: %w", err)
				s.t.WriteResponseErr(err)
				return err
			}
			defer release()
		}
	}

	// build the merkle proof response
	writeResp := &rhpv2.RPCWriteMerkleProof{
		NewMerkleRoot: contractUpdater.MerkleRoot(),
	}
	if req.MerkleProof {
		writeResp.OldSubtreeHashes, writeResp.OldLeafHashes = rhpv2.BuildDiffProof(req.Actions, oldRoots)
	}
	if err := s.writeResponse(writeResp, time.Minute); err != nil {
		return fmt.Errorf("failed to write merkle proof: %w", err)
	}

	// apply the new merkle root and file size to the revision
	revision.FileMerkleRoot = writeResp.NewMerkleRoot
	revision.Filesize = contractUpdater.SectorCount() * rhpv2.SectorSize

	// read the renter's signature
	var renterSigResponse rhpv2.RPCWriteResponse
	if err := s.readResponse(&renterSigResponse, minMessageSize, 30*time.Second); err != nil {
		return fmt.Errorf("failed to read renter signature: %w", err)
	}

	// validate the contract signature
	renterSig := renterSigResponse.Signature
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, renterSigResponse.Signature) {
		err := fmt.Errorf("failed to verify renter signature: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	}
	hostSig := sh.privateKey.SignHash(sigHash)
	signedRevision := contracts.SignedRevision{
		Revision:        revision,
		HostSignature:   hostSig,
		RenterSignature: renterSig,
	}

	// sync the storage manager
	if err := sh.storage.Sync(); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to sync storage manager: %w", err)
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
	if err := contractUpdater.Commit(signedRevision, costs.ToUsage()); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit contract modifications: %w", err)
	}
	// update the session contract
	s.contract = signedRevision

	// send the host signature
	hostSigResp := &rhpv2.RPCWriteResponse{Signature: hostSig}
	if err := s.writeResponse(hostSigResp, 30*time.Second); err != nil {
		return fmt.Errorf("failed to write host signature: %w", err)
	}
	return nil
}

func (sh *SessionHandler) rpcRead(s *session) error {
	currentHeight := sh.cm.TipState().Index.Height
	// get the locked contract and check that it is revisable
	if err := s.ContractRevisable(currentHeight); err != nil {
		err := fmt.Errorf("contract not revisable: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// get the host's current settings
	settings, err := sh.Settings()
	if err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to get host settings: %w", err)
	}

	// read the read request
	var req rhpv2.RPCReadRequest
	if err := s.readRequest(&req, 4*minMessageSize, time.Minute); err != nil {
		return fmt.Errorf("failed to read read request: %w", err)
	}

	// validate the request sections and calculate the cost
	costs, err := validateReadActions(req.Sections, req.MerkleProof, settings)
	if err != nil {
		s.t.WriteResponseErr(err)
		return fmt.Errorf("failed to validate read request: %w", err)
	}
	cost, _ := costs.Total()

	// revise the contract with the values sent by the renter
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// validate the renter's signature and transfer
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		err := fmt.Errorf("failed to validate revision: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	}

	payment, _, err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency)
	if err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
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
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	defer updater.Close()

	// adjust the revenue to account for the full transfer by the renter
	excess, underflow := payment.SubWithUnderflow(cost)
	if !underflow {
		costs.Egress = costs.Egress.Add(excess)
	}
	// commit the contract revision
	if err := updater.Commit(signedRevision, costs.ToUsage()); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit contract revision: %w", err)
	}
	// update the session contract
	s.contract = signedRevision
	// add the cost to the amount spent
	s.Spend(cost)

	// listen for RPCLoopReadStop
	stopSignal := make(chan error, 1)
	go func() {
		var id types.Specifier
		// long timeout because the renter may be slow to send the stop signal
		err := s.readResponse(&id, minMessageSize, 5*time.Minute)
		if err != nil {
			stopSignal <- err
		} else if id != rhpv2.RPCReadStop {
			stopSignal <- errors.New("expected 'stop' from renter, got " + id.String())
		} else {
			stopSignal <- nil
		}
	}()

	// enter response loop
	for i, sec := range req.Sections {
		sector, err := sh.storage.Read(sec.MerkleRoot)
		if err != nil {
			err := fmt.Errorf("failed to get sector: %w", err)
			s.t.WriteResponseErr(err)
			return err
		}

		resp := &rhpv2.RPCReadResponse{
			Data: sector[sec.Offset : sec.Offset+sec.Length],
		}
		if req.MerkleProof {
			start := sec.Offset / rhpv2.LeafSize
			end := (sec.Offset + sec.Length) / rhpv2.LeafSize
			resp.MerkleProof = rhpv2.BuildProof(sector, start, end, nil)
		}

		// check for the stop signal and send the response
		select {
		case err := <-stopSignal:
			if err != nil {
				return err
			}
			resp.Signature = hostSig
			return s.writeResponse(resp, 30*time.Second)
		default:
		}

		if i == len(req.Sections)-1 {
			resp.Signature = hostSig
		}
		if err := s.writeResponse(resp, 30*time.Second); err != nil {
			return fmt.Errorf("failed to write read response: %w", err)
		}
	}
	return <-stopSignal
}

func convertToPublicKey(uc types.UnlockKey) (types.PublicKey, error) {
	if uc.Algorithm != types.SpecifierEd25519 {
		return types.PublicKey{}, errors.New("unsupported signature algorithm")
	} else if len(uc.Key) != ed25519.PublicKeySize {
		return types.PublicKey{}, errors.New("invalid public key length")
	}
	return *(*types.PublicKey)(uc.Key), nil
}
