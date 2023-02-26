package rhp

import (
	"context"
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
	ErrHostInternalError = errors.New("unable to form contract")
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
		return nil
	}

	s.contract = contracts.SignedRevision{}
	sh.contracts.Unlock(s.contract.Revision.ParentID)
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
	if err := rhp.ValidateContractFormation(formationTxn.FileContracts[0], hostPub.UnlockKey(), renterPub.UnlockKey(), currentHeight, settings); err != nil {
		err := fmt.Errorf("contract rejected: validation failed: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// calculate the host's collateral and add the inputs to the transaction
	hostCollateral := formationTxn.FileContracts[0].ValidProofOutputs[1].Value.Sub(settings.ContractPrice)
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
	if err := s.readResponse(&renterSignaturesResp, minMessageSize, 30*time.Second); err != nil {
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
	if err := sh.contracts.AddContract(signedRevision, formationTxnSet, hostCollateral, currentHeight); err != nil {
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
	/*	currentHeight := sh.cm.TipState().Index.Height
		if err := s.ContractRevisable(currentHeight); err != nil {
			err := fmt.Errorf("contract not revisable: %w", err)
			s.t.WriteResponseErr(err)
			return err
		}

		var req rpcRenewAndClearContractRequest
		if err := s.ReadRequest(&req, 10*minMessageSize, time.Minute); err != nil {
			return fmt.Errorf("failed to read renew request: %w", err)
		}

		renewalTxnSet := req.Transactions
		if len(renewalTxnSet) == 0 || len(renewalTxnSet[len(renewalTxnSet)-1].FileContracts) != 1 || len(renewalTxnSet[len(renewalTxnSet)-1].FileContractRevisions) != 1 {
			err := ErrTxnMissingContract
			s.t.WriteResponseErr(err)
			return err
		}

		renterPub := req.RenterKey
		// get the host's public key, current block height, and settings
		existingContract := s.contract.Revision
		clearingRevision := renewalTxn.FileContractRevisions[0]
		hostPub := types.SiaPublicKey{
			Algorithm: types.SignatureEd25519,
			Key:       sh.privateKey.Public().(ed25519.PublicKey),
		}
		settings, err := sh.Settings()
		if err != nil {
			s.t.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to get host settings: %w", err)
		}
		// get the contract from the transaction set
		renewalTxn := &renewalTxnSet[len(renewalTxnSet)-1]

		// create an initial revision for the renewed contract
		initialRevision := initialRevision(renewalTxn, hostPub, renterPub)
		renewalSigHash := hashRevision(initialRevision)
		clearingSigHash := hashRevision(clearingRevision)
		renewalHostSig := ed25519.Sign(sh.privateKey, renewalSigHash[:])
		clearingHostSig := ed25519.Sign(sh.privateKey, clearingSigHash[:])

		// calculate the "base" storage cost to the renter and risked collateral for
		// the host for the data already in the contract. If the contract height did
		// not increase, base costs are zero since the storage is already paid for.
		baseRenterCost := settings.ContractPrice
		var baseCollateral types.Currency
		if initialRevision.NewWindowEnd > existingContract.NewWindowEnd {
			extension := uint64(initialRevision.NewWindowEnd - existingContract.NewWindowEnd)
			baseRenterCost = baseRenterCost.Add(settings.StoragePrice.Mul64(initialRevision.NewFileSize).Mul64(extension))
			baseCollateral = settings.Collateral.Mul64(initialRevision.NewFileSize).Mul64(extension)
		} else if initialRevision.NewValidProofOutputs[1].Value.Cmp(baseCollateral.Add(baseRenterCost)) < 0 {
			err := errors.New("renewal rejected: insufficient host payout for storage and collateral")
			s.t.WriteResponseErr(err)
			return err
		}

		// validate fields of the clearing revision and renewal. note: the v1
		// contract type does not contain the public keys or signatures.
		if err := validateClearingRevision(existingContract, clearingRevision); err != nil {
			err := fmt.Errorf("renewal rejected: clearing revision validation failed: %w", err)
			s.t.WriteResponseErr(err)
			return err
		} else if err := validateContractRenewal(existingContract, initialRevision, hostPub, renterPub, baseRenterCost, baseCollateral, currentHeight, settings); err != nil {
			err := fmt.Errorf("renewal rejected: renewal validation failed: %w", err)
			s.t.WriteResponseErr(err)
			return err
		}

		renterInputs, renterOutputs := len(renewalTxn.SiacoinInputs), len(renewalTxn.SiacoinOutputs)
		fundAmount := initialRevision.NewValidProofOutputs[1].Value.Sub(baseRenterCost)
		toSign, discard, err := sh.wallet.FundTransaction(renewalTxn, fundAmount, nil)
		if err != nil {
			s.t.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to fund renewal transaction: %w", err)
		}
		defer discard()

		// send the renter the host additions to the renewal txn
		hostAdditionsResp := &rpcFormContractAdditions{
			Inputs:  renewalTxn.SiacoinInputs[renterInputs:],
			Outputs: renewalTxn.SiacoinOutputs[renterOutputs:],
		}
		if err = s.WriteResponse(hostAdditionsResp, 30*time.Second); err != nil {
			return fmt.Errorf("failed to write host additions: %w", err)
		}

		// read the renter's signatures for the renewal
		var renterSigsResp rpcRenewAndClearContractSignatures
		if err = s.ReadResponse(&renterSigsResp, 4096, 30*time.Second); err != nil {
			return fmt.Errorf("failed to read renter signatures: %w", err)
		}

		// validate the renter's initial revision signature
		renterRenewalSig := renterSigsResp.
		if !ed25519.Verify(renterPub.Key, renewalSigHash[:], renterRenewalSig) {
			err := ErrInvalidRenterSignature
			s.t.WriteResponseErr(err)
			return err
		}
		renterClearingSig := renterSigsResp.FinalRevisionSignature
		// add the renter's signatures to the transaction and contract revision
		renewalTxn.TransactionSignatures = renterSigsResp.ContractSignatures

		// sign and broadcast the formation transaction
		if err = sh.wallet.SignTransaction(renewalTxn, toSign, types.FullCoveredFields); err != nil {
			s.t.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to sign formation transaction: %w", err)
		} else if err = sh.tpool.AcceptTransactionSet(renewalTxnSet); err != nil {
			err := fmt.Errorf("failed to broadcast formation transaction: %w", err)
			s.t.WriteResponseErr(err)
			return err
		}

		clearingSignedRevision := contracts.SignedRevision{
			Revision:        existingContract,
			RenterSignature: renterSigsResp.FinalRevisionSignature,
			HostSignature:   clearingHostSig,
		}

		renewalSignedRevision := contracts.SignedRevision{
			Revision:        initialRevision,
			RenterSignature: renterRenewalSig,
			HostSignature:   hostRenewalSig,
		}
		if err := sh.contracts.RenewContract(renewalSignedRevision, clearingSignedRevision, renewalTxnSet, hostCollateral, currentHeight); err != nil {
			s.t.WriteResponseErr(ErrHostInternalError)
			return fmt.Errorf("failed to renew contract: %w", err)
		}

		// add renter spending to the amount spent
		s.Spend(baseRenterCost)
		// log the formation event
		sh.metrics.Report(EventContractRenewed{
			SessionUID: s.uid,
			ContractID: renewalTxn.FileContractID(0),
			Contract:   initialRevision,
		})

		// send the host signatures to the renter
		renterTxnSigs := len(renterSigsResp.ContractSignatures)
		hostSignaturesResp := &rpcFormContractSignatures{
			ContractSignatures: renewalTxn.TransactionSignatures[renterTxnSigs:],
			RevisionSignature: types.TransactionSignature{
				ParentID:      crypto.Hash(renewalTxn.FileContractID(0)),
				Signature:     hostSig,
				CoveredFields: types.CoveredFields{FileContractRevisions: []uint64{0}},
			},
		}
		if err := s.WriteResponse(hostSignaturesResp, 30*time.Second); err != nil {
			return fmt.Errorf("failed to write host signatures: %w", err)
		}*/
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

	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
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

	proofSize := rhpv2.RangeProofSize(rhpv2.LeavesPerSector, req.RootOffset, req.RootOffset+req.NumRoots)
	cost := settings.DownloadBandwidthPrice.Mul64((req.NumRoots + proofSize) * 32)
	if err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency); err != nil {
		err := fmt.Errorf("failed to validate revision: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

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

	if err := updater.Commit(signedRevision); err != nil {
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
	cost, collateral, err := validateWriteActions(req.Actions, oldSectors, req.MerkleProof, remainingDuration, settings)
	if err != nil {
		err := fmt.Errorf("failed to validate write actions: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// revise and validate the new revision
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	} else if err := rhp.ValidateRevision(s.contract.Revision, revision, cost, collateral); err != nil {
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
	// commit the contract modifications
	if err := contractUpdater.Commit(signedRevision); err != nil {
		s.t.WriteResponseErr(ErrHostInternalError)
		return fmt.Errorf("failed to commit contract modifications: %w", err)
	}
	// update the session contract
	s.contract = signedRevision

	// add the amount spent
	s.Spend(cost)

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
	var bandwidth uint64
	for _, sec := range req.Sections {
		switch {
		case uint64(sec.Offset)+uint64(sec.Length) > rhpv2.SectorSize:
			err := errors.New("request is out-of-bounds")
			s.t.WriteResponseErr(err)
			return err
		case sec.Length == 0:
			err := errors.New("length cannot be zero")
			s.t.WriteResponseErr(err)
			return err
		case req.MerkleProof && (sec.Offset%rhpv2.LeafSize != 0 || sec.Length%rhpv2.LeafSize != 0):
			err := errors.New("offset and length must be multiples of SegmentSize when requesting a Merkle proof")
			s.t.WriteResponseErr(err)
			return err
		}

		bandwidth += uint64(sec.Length)
		if req.MerkleProof {
			start := sec.Offset / rhpv2.LeafSize
			end := (sec.Offset + sec.Length) / rhpv2.LeafSize
			proofSize := rhpv2.RangeProofSize(rhpv2.LeavesPerSector, start, end)
			bandwidth += proofSize * 32
		}
	}
	// revise the contract with the values sent by the renter
	revision, err := rhp.Revise(s.contract.Revision, req.RevisionNumber, req.ValidProofValues, req.MissedProofValues)
	if err != nil {
		err := fmt.Errorf("failed to revise contract: %w", err)
		s.t.WriteResponseErr(err)
		return err
	}

	// calculate the cost of the read
	cost := settings.DownloadBandwidthPrice.Mul64(bandwidth).Add(settings.SectorAccessPrice.Mul64(uint64(len(req.Sections))))
	// validate the renter's signature and transfer
	sigHash := rhp.HashRevision(revision)
	if !s.contract.RenterKey().VerifyHash(sigHash, req.Signature) {
		err := fmt.Errorf("failed to validate revision: %w", ErrInvalidRenterSignature)
		s.t.WriteResponseErr(err)
		return err
	} else if err := rhp.ValidateRevision(s.contract.Revision, revision, cost, types.ZeroCurrency); err != nil {
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

	if err := updater.Commit(signedRevision); err != nil {
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
