package rhp

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

const (
	// minMessageSize is the minimum size of an RPC message
	minMessageSize = 4096
)

var (
	errContractLocked         = errors.New("contract is locked by another party")
	errContractFinalized      = errors.New("contract cannot be revised further")
	errInsufficientCollateral = errors.New("insufficient collateral")
	errInsufficientFunds      = errors.New("insufficient funds")
	errInvalidMerkleProof     = errors.New("host supplied invalid Merkle proof")
)

// RPCSettings calls the Settings RPC, returning the host's reported settings.
func RPCSettings(ctx context.Context, t *rhp2.Transport) (settings rhp2.HostSettings, err error) {
	var resp rhp2.RPCSettingsResponse
	if err := t.Call(rhp2.RPCSettingsID, nil, &resp); err != nil {
		return rhp2.HostSettings{}, err
	} else if err := json.Unmarshal(resp.Settings, &settings); err != nil {
		return rhp2.HostSettings{}, fmt.Errorf("couldn't unmarshal json: %w", err)
	}

	return settings, nil
}

// RPCFormContract forms a contract with a host.
func RPCFormContract(ctx context.Context, t *rhp2.Transport, renterKey types.PrivateKey, txnSet []types.Transaction) (_ rhp2.ContractRevision, _ []types.Transaction, err error) {
	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	// create request
	renterPubkey := renterKey.PublicKey()
	req := &rhp2.RPCFormContractRequest{
		Transactions: txnSet,
		RenterKey:    renterPubkey.UnlockKey(),
	}
	if err := t.WriteRequest(rhp2.RPCFormContractID, req); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	// execute form contract RPC
	var resp rhp2.RPCFormContractAdditions
	if err := t.ReadResponse(&resp, 65536); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID: txn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				renterPubkey.UnlockKey(),
				t.HostKey().UnlockKey(),
			},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			RevisionNumber:     1,
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
		},
	}
	revSig := renterKey.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      revSig[:],
	}

	// write our signatures
	renterSigs := &rhp2.RPCFormContractSignatures{
		ContractSignatures: renterContractSignatures,
		RevisionSignature:  renterRevisionSig,
	}
	if err := t.WriteResponse(renterSigs); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	// read the host's signatures and merge them with our own
	var hostSigs rhp2.RPCFormContractSignatures
	if err := t.ReadResponse(&hostSigs, minMessageSize); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)
	return rhp2.ContractRevision{
		Revision: initRevision,
		Signatures: [2]types.TransactionSignature{
			renterRevisionSig,
			hostSigs.RevisionSignature,
		},
	}, signedTxnSet, nil
}

// RHP2Session represents a session with a host
type RHP2Session struct {
	transport   *rhp2.Transport
	revision    rhp2.ContractRevision
	key         types.PrivateKey
	appendRoots []types.Hash256
	settings    rhp2.HostSettings
	lastSeen    time.Time
}

// NewRHP2Session returns a new rhp2 session
func NewRHP2Session(t *rhp2.Transport, key types.PrivateKey, rev rhp2.ContractRevision, settings rhp2.HostSettings) *RHP2Session {
	return &RHP2Session{
		transport: t,
		key:       key,
		revision:  rev,
		settings:  settings,
	}
}

// Append appends the sector to the contract
func (s *RHP2Session) Append(ctx context.Context, sector *[rhp2.SectorSize]byte, price, collateral types.Currency) (types.Hash256, error) {
	err := s.Write(ctx, []rhp2.RPCWriteAction{{
		Type: rhp2.RPCWriteActionAppend,
		Data: sector[:],
	}}, price, collateral)
	if err != nil {
		return types.Hash256{}, err
	}
	return s.appendRoots[0], nil
}

// Close closes the underlying transport
func (s *RHP2Session) Close() (err error) {
	return s.closeTransport()
}

// Delete deletes the sectors at the given indices from the contract
func (s *RHP2Session) Delete(ctx context.Context, sectorIndices []uint64, price types.Currency) error {
	if len(sectorIndices) == 0 {
		return nil
	}

	// sort in descending order so that we can use 'range'
	sort.Slice(sectorIndices, func(i, j int) bool {
		return sectorIndices[i] > sectorIndices[j]
	})

	// iterate backwards from the end of the contract, swapping each "good"
	// sector with one of the "bad" sectors.
	var actions []rhp2.RPCWriteAction
	cIndex := s.revision.NumSectors() - 1
	for _, rIndex := range sectorIndices {
		if cIndex != rIndex {
			// swap a "good" sector for a "bad" sector
			actions = append(actions, rhp2.RPCWriteAction{
				Type: rhp2.RPCWriteActionSwap,
				A:    uint64(cIndex),
				B:    uint64(rIndex),
			})
		}
		cIndex--
	}
	// trim all "bad" sectors
	actions = append(actions, rhp2.RPCWriteAction{
		Type: rhp2.RPCWriteActionTrim,
		A:    uint64(len(sectorIndices)),
	})

	// request the swap+delete operation
	//
	// NOTE: siad hosts will accept up to 20 MiB of data in the request,
	// which should be sufficient to delete up to 2.5 TiB of sector data
	// at a time.
	return s.Write(ctx, actions, price, types.ZeroCurrency)
}

// HostKey returns the host's public key
func (s *RHP2Session) HostKey() types.PublicKey { return s.revision.HostKey() }

// Read reads the given sections
func (s *RHP2Session) Read(ctx context.Context, w io.Writer, sections []rhp2.RPCReadRequestSection, price types.Currency) (err error) {
	empty := true
	for _, s := range sections {
		empty = empty && s.Length == 0
	}
	if empty || len(sections) == 0 {
		return nil
	}

	if !s.isRevisable() {
		return errContractFinalized
	} else if !s.sufficientFunds(price) {
		return errInsufficientFunds
	}

	// construct new revision
	rev := s.revision.Revision
	rev.RevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)
	renterSig := s.key.SignHash(revisionHash)

	// construct the request
	req := &rhp2.RPCReadRequest{
		Sections:    sections,
		MerkleProof: true,

		RevisionNumber:    rev.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         renterSig,
	}

	var hostSig *types.Signature
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.WriteRequest(rhp2.RPCReadID, req); err != nil {
			return err
		}

		// ensure we send RPCLoopReadStop before returning
		defer transport.WriteResponse(&rhp2.RPCReadStop)

		// read all sections
		for _, sec := range sections {
			hostSig, err = s.readSection(w, transport, sec)
			if err != nil {
				return err
			}
			if hostSig != nil {
				break // exit the loop; they won't be sending any more data
			}
		}

		// the host is required to send a signature; if they haven't sent one
		// yet, they should send an empty ReadResponse containing just the
		// signature.
		if hostSig == nil {
			var resp rhp2.RPCReadResponse
			if err := transport.ReadResponse(&resp, 4096); err != nil {
				return wrapResponseErr(err, "couldn't read signature", "host rejected Read request")
			}
			hostSig = &resp.Signature
		}
		return nil
	}); err != nil {
		return err
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, *hostSig) {
		return errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = renterSig[:]
	s.revision.Signatures[1].Signature = hostSig[:]

	return nil
}

// Reconnect reconnects to the host
func (s *RHP2Session) Reconnect(ctx context.Context, hostIP string, hostKey types.PublicKey, renterKey types.PrivateKey, contractID types.FileContractID) (err error) {
	s.closeTransport()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	if err != nil {
		return err
	}
	s.transport, err = rhp2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}

	s.key = renterKey
	if err = s.lock(ctx, contractID, renterKey, 10*time.Second); err != nil {
		s.closeTransport()
		return err
	}

	if err := s.updateSettings(ctx); err != nil {
		s.closeTransport()
		return err
	}

	s.lastSeen = time.Now()
	return nil
}

// Refresh refreshes the session
func (s *RHP2Session) Refresh(ctx context.Context, sessionTTL time.Duration, renterKey types.PrivateKey, contractID types.FileContractID) error {
	if s.transport == nil {
		return errors.New("no transport")
	}

	if time.Since(s.lastSeen) >= sessionTTL {
		// use RPCSettings as a generic "ping"
		if err := s.updateSettings(ctx); err != nil {
			return err
		}
	}

	if s.revision.ID() != contractID {
		// connected, but not locking the correct contract
		if s.revision.ID() != (types.FileContractID{}) {
			if err := s.unlock(ctx); err != nil {
				return err
			}
		}
		if err := s.lock(ctx, contractID, renterKey, 10*time.Second); err != nil {
			return err
		}

		s.key = renterKey
		if err := s.updateSettings(ctx); err != nil {
			return err
		}
	}
	s.lastSeen = time.Now()
	return nil
}

// RenewContract renews the contract
func (s *RHP2Session) RenewContract(ctx context.Context, txnSet []types.Transaction, finalPayment types.Currency) (_ rhp2.ContractRevision, _ []types.Transaction, err error) {
	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	// construct the final revision of the old contract
	finalOldRevision := s.revision.Revision
	newValid, _ := updateRevisionOutputs(&finalOldRevision, finalPayment, types.ZeroCurrency)
	finalOldRevision.MissedProofOutputs = finalOldRevision.ValidProofOutputs
	finalOldRevision.Filesize = 0
	finalOldRevision.FileMerkleRoot = types.Hash256{}
	finalOldRevision.RevisionNumber = math.MaxUint64

	// construct the renew request
	req := &rhp2.RPCRenewAndClearContractRequest{
		Transactions:           txnSet,
		RenterKey:              s.revision.Revision.UnlockConditions.PublicKeys[0],
		FinalValidProofValues:  newValid,
		FinalMissedProofValues: newValid,
	}

	// send the request
	var resp rhp2.RPCFormContractAdditions
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.WriteRequest(rhp2.RPCRenewClearContractID, req); err != nil {
			return err
		}
		return transport.ReadResponse(&resp, 65536)
	}); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID:         txn.FileContractID(0),
		UnlockConditions: s.revision.Revision.UnlockConditions,
		FileContract: types.FileContract{
			RevisionNumber:     1,
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
		},
	}
	revSig := s.key.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      revSig[:],
	}

	// create  signatures
	finalRevSig := s.key.SignHash(hashRevision(finalOldRevision))
	renterSigs := &rhp2.RPCRenewAndClearContractSignatures{
		ContractSignatures:     renterContractSignatures,
		RevisionSignature:      renterRevisionSig,
		FinalRevisionSignature: finalRevSig,
	}

	// send the signatures and read the host's signatures
	var hostSigs rhp2.RPCRenewAndClearContractSignatures
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.WriteResponse(renterSigs); err != nil {
			return err
		}
		return transport.ReadResponse(&hostSigs, 4096)
	}); err != nil {
		return rhp2.ContractRevision{}, nil, err
	}

	// merge host signatures with our own
	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)
	return rhp2.ContractRevision{
		Revision:   initRevision,
		Signatures: [2]types.TransactionSignature{renterRevisionSig, hostSigs.RevisionSignature},
	}, signedTxnSet, nil
}

// Revision returns the contract revision
func (s *RHP2Session) Revision() (rev rhp2.ContractRevision) {
	b, _ := json.Marshal(s.revision) // deep copy
	if err := json.Unmarshal(b, &rev); err != nil {
		panic(err) // should never happen
	}
	return rev
}

// RPCAppendCost returns the cost of a single append
func (s *RHP2Session) RPCAppendCost(remainingDuration uint64) (types.Currency, types.Currency, error) {
	var sector [rhp2.SectorSize]byte
	actions := []rhp2.RPCWriteAction{{Type: rhp2.RPCWriteActionAppend, Data: sector[:]}}
	cost, err := s.settings.RPCWriteCost(actions, 0, remainingDuration, true)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
	price, collateral := cost.Total()
	return price, collateral, nil
}

// SectorRoots returns n roots at offset.
func (s *RHP2Session) SectorRoots(ctx context.Context, offset, n uint64, price types.Currency) (roots []types.Hash256, err error) {
	if !s.isRevisable() {
		return nil, errContractFinalized
	} else if offset+n > s.revision.NumSectors() {
		return nil, errors.New("requested range is out-of-bounds")
	} else if n == 0 {
		return nil, nil
	} else if !s.sufficientFunds(price) {
		return nil, errInsufficientFunds
	}

	// construct new revision
	rev := s.revision.Revision
	rev.RevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)

	req := &rhp2.RPCSectorRootsRequest{
		RootOffset: uint64(offset),
		NumRoots:   uint64(n),

		RevisionNumber:    rev.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         s.key.SignHash(revisionHash),
	}

	// execute the sector roots RPC
	var resp rhp2.RPCSectorRootsResponse
	if err = s.withTransport(ctx, func(t *rhp2.Transport) error {
		if err := t.WriteRequest(rhp2.RPCSectorRootsID, req); err != nil {
			return err
		} else if err := t.ReadResponse(&resp, uint64(4096+32*n)); err != nil {
			readCtx := fmt.Sprintf("couldn't read %v response", rhp2.RPCSectorRootsID)
			rejectCtx := fmt.Sprintf("host rejected %v request", rhp2.RPCSectorRootsID)
			return wrapResponseErr(err, readCtx, rejectCtx)
		} else {
			return nil
		}
	}); err != nil {
		return nil, err
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, resp.Signature) {
		return nil, errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = req.Signature[:]
	s.revision.Signatures[1].Signature = resp.Signature[:]

	// verify the proof
	if !rhp2.VerifySectorRangeProof(resp.MerkleProof, resp.SectorRoots, offset, offset+n, s.revision.NumSectors(), rev.FileMerkleRoot) {
		return nil, errInvalidMerkleProof
	}
	return resp.SectorRoots, nil
}

// Settings returns the host settings
func (s *RHP2Session) Settings() *rhp2.HostSettings { return &s.settings }

// Write performs the given write actions
func (s *RHP2Session) Write(ctx context.Context, actions []rhp2.RPCWriteAction, price, collateral types.Currency) (err error) {
	if !s.isRevisable() {
		return errContractFinalized
	} else if len(actions) == 0 {
		return nil
	} else if !s.sufficientFunds(price) {
		return errInsufficientFunds
	} else if !s.sufficientCollateral(collateral) {
		return errInsufficientCollateral
	}

	rev := s.revision.Revision
	newFilesize := rev.Filesize
	for _, action := range actions {
		switch action.Type {
		case rhp2.RPCWriteActionAppend:
			newFilesize += rhp2.SectorSize
		case rhp2.RPCWriteActionTrim:
			newFilesize -= rhp2.SectorSize * action.A
		}
	}

	// calculate new revision outputs
	newValid, newMissed := updateRevisionOutputs(&rev, price, collateral)

	// compute appended roots in parallel with I/O
	precompChan := make(chan struct{})
	go func() {
		s.appendRoots = s.appendRoots[:0]
		for _, action := range actions {
			if action.Type == rhp2.RPCWriteActionAppend {
				s.appendRoots = append(s.appendRoots, rhp2.SectorRoot((*[rhp2.SectorSize]byte)(action.Data)))
			}
		}
		close(precompChan)
	}()
	// ensure that the goroutine has exited before we return
	defer func() { <-precompChan }()

	// create request
	req := &rhp2.RPCWriteRequest{
		Actions:     actions,
		MerkleProof: true,

		RevisionNumber:    rev.RevisionNumber + 1,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
	}

	// send request and read merkle proof
	var merkleResp rhp2.RPCWriteMerkleProof
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.WriteRequest(rhp2.RPCWriteID, req); err != nil {
			return err
		} else if err := transport.ReadResponse(&merkleResp, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read Merkle proof response", "host rejected Write request")
		} else {
			return nil
		}
	}); err != nil {
		return err
	}

	// verify proof
	proofHashes := merkleResp.OldSubtreeHashes
	leafHashes := merkleResp.OldLeafHashes
	oldRoot, newRoot := types.Hash256(rev.FileMerkleRoot), merkleResp.NewMerkleRoot
	<-precompChan
	if newFilesize > 0 && !rhp2.VerifyDiffProof(actions, s.revision.NumSectors(), proofHashes, leafHashes, oldRoot, newRoot, s.appendRoots) {
		err := errInvalidMerkleProof
		s.withTransport(ctx, func(transport *rhp2.Transport) error { return transport.WriteResponseErr(err) })
		return err
	}

	// update revision
	rev.RevisionNumber++
	rev.Filesize = newFilesize
	copy(rev.FileMerkleRoot[:], newRoot[:])
	revisionHash := hashRevision(rev)
	renterSig := &rhp2.RPCWriteResponse{
		Signature: s.key.SignHash(revisionHash),
	}

	// exchange signatures
	var hostSig rhp2.RPCWriteResponse
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.WriteResponse(renterSig); err != nil {
			return fmt.Errorf("couldn't write signature response: %w", err)
		} else if err := transport.ReadResponse(&hostSig, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read signature response", "host rejected Write signature")
		} else {
			return nil
		}
	}); err != nil {
		return err
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, hostSig.Signature) {
		return errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = renterSig.Signature[:]
	s.revision.Signatures[1].Signature = hostSig.Signature[:]
	return nil
}

func (s *RHP2Session) closeTransport() error {
	if s.transport != nil {
		return s.transport.Close()
	}
	return nil
}

func (s *RHP2Session) isRevisable() bool {
	return s.revision.Revision.RevisionNumber < math.MaxUint64
}

func (s *RHP2Session) lock(ctx context.Context, id types.FileContractID, key types.PrivateKey, timeout time.Duration) (err error) {
	req := &rhp2.RPCLockRequest{
		ContractID: id,
		Signature:  s.transport.SignChallenge(key),
		Timeout:    uint64(timeout.Milliseconds()),
	}

	// execute lock RPC
	var resp rhp2.RPCLockResponse
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		if err := transport.Call(rhp2.RPCLockID, req, &resp); err != nil {
			return err
		}
		transport.SetChallenge(resp.NewChallenge)
		return nil
	}); err != nil {
		return err
	}

	// verify claimed revision
	if len(resp.Signatures) != 2 {
		return fmt.Errorf("host returned wrong number of signatures (expected 2, got %v)", len(resp.Signatures))
	} else if len(resp.Signatures[0].Signature) != 64 || len(resp.Signatures[1].Signature) != 64 {
		return errors.New("signatures on claimed revision have wrong length")
	}
	revHash := hashRevision(resp.Revision)
	if !key.PublicKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[0].Signature)) {
		return errors.New("renter's signature on claimed revision is invalid")
	} else if !s.transport.HostKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[1].Signature)) {
		return errors.New("host's signature on claimed revision is invalid")
	} else if !resp.Acquired {
		return errContractLocked
	} else if resp.Revision.RevisionNumber == math.MaxUint64 {
		return errContractFinalized
	}
	s.revision = rhp2.ContractRevision{
		Revision:   resp.Revision,
		Signatures: [2]types.TransactionSignature{resp.Signatures[0], resp.Signatures[1]},
	}
	return nil
}

func (s *RHP2Session) readSection(w io.Writer, t *rhp2.Transport, sec rhp2.RPCReadRequestSection) (hostSig *types.Signature, _ error) {
	// NOTE: normally, we would call ReadResponse here to read an AEAD RPC
	// message, verify the tag and decrypt, and then pass the data to
	// VerifyProof. As an optimization, we instead stream the message
	// through a Merkle proof verifier before verifying the AEAD tag.
	// Security therefore depends on the caller of Read discarding any data
	// written to w in the event that verification fails.
	msgReader, err := t.RawResponse(4096 + uint64(sec.Length))
	if err != nil {
		return nil, wrapResponseErr(err, "couldn't read sector data", "host rejected Read request")
	}
	// Read the signature, which may or may not be present.
	lenbuf := make([]byte, 8)
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read signature len: %w", err)
	}
	if n := binary.LittleEndian.Uint64(lenbuf); n > 0 {
		hostSig = new(types.Signature)
		if _, err := io.ReadFull(msgReader, hostSig[:]); err != nil {
			return nil, fmt.Errorf("couldn't read signature: %w", err)
		}
	}
	// stream the sector data into w and the proof verifier
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read data len: %w", err)
	} else if binary.LittleEndian.Uint64(lenbuf) != uint64(sec.Length) {
		return nil, errors.New("host sent wrong amount of sector data")
	}
	proofStart := sec.Offset / rhp2.LeafSize
	proofEnd := proofStart + sec.Length/rhp2.LeafSize
	rpv := rhp2.NewRangeProofVerifier(proofStart, proofEnd)
	tee := io.TeeReader(io.LimitReader(msgReader, int64(sec.Length)), &segWriter{w: w})
	// the proof verifier Reads one segment at a time, so bufio is crucial
	// for performance here
	if _, err := rpv.ReadFrom(bufio.NewReaderSize(tee, 1<<16)); err != nil {
		return nil, fmt.Errorf("couldn't stream sector data: %w", err)
	}
	// read the Merkle proof
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read proof len: %w", err)
	}
	if binary.LittleEndian.Uint64(lenbuf) != uint64(rhp2.RangeProofSize(rhp2.LeavesPerSector, proofStart, proofEnd)) {
		return nil, errors.New("invalid proof size")
	}
	proof := make([]types.Hash256, binary.LittleEndian.Uint64(lenbuf))
	for i := range proof {
		if _, err := io.ReadFull(msgReader, proof[i][:]); err != nil {
			return nil, fmt.Errorf("couldn't read Merkle proof: %w", err)
		}
	}
	// verify the message tag and the Merkle proof
	if err := msgReader.VerifyTag(); err != nil {
		return nil, err
	}
	if !rpv.Verify(proof, sec.MerkleRoot) {
		return nil, errInvalidMerkleProof
	}
	return
}

func (s *RHP2Session) sufficientFunds(price types.Currency) bool {
	return s.revision.RenterFunds().Cmp(price) >= 0
}

func (s *RHP2Session) sufficientCollateral(collateral types.Currency) bool {
	return s.revision.Revision.MissedProofOutputs[1].Value.Cmp(collateral) >= 0
}

func (s *RHP2Session) updateSettings(ctx context.Context) (err error) {
	var resp rhp2.RPCSettingsResponse
	if err := s.withTransport(ctx, func(transport *rhp2.Transport) error {
		return transport.Call(rhp2.RPCSettingsID, nil, &resp)
	}); err != nil {
		return err
	}

	if err := json.Unmarshal(resp.Settings, &s.settings); err != nil {
		return fmt.Errorf("couldn't unmarshal json: %w", err)
	}
	return
}

func (s *RHP2Session) unlock(ctx context.Context) (err error) {
	s.revision = rhp2.ContractRevision{}
	s.key = nil

	return s.withTransport(ctx, func(transport *rhp2.Transport) error {
		return transport.WriteRequest(rhp2.RPCUnlockID, nil)
	})
}

func (s *RHP2Session) withTransport(ctx context.Context, fn func(t *rhp2.Transport) error) (err error) {
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- fn(s.transport)
	}()

	select {
	case err = <-errChan:
		return
	case <-ctx.Done():
		_ = s.transport.ForceClose() // ignore error
		if err = <-errChan; err == nil {
			err = ctx.Err()
		}
		s.transport = nil
	}
	return
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func updateRevisionOutputs(rev *types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.ValidProofOutputs = append([]types.SiacoinOutput(nil), rev.ValidProofOutputs...)
	rev.MissedProofOutputs = append([]types.SiacoinOutput(nil), rev.MissedProofOutputs...)

	// move valid payout from renter to host
	rev.ValidProofOutputs[0].Value = rev.ValidProofOutputs[0].Value.Sub(cost)
	rev.ValidProofOutputs[1].Value = rev.ValidProofOutputs[1].Value.Add(cost)

	// move missed payout from renter to void
	rev.MissedProofOutputs[0].Value = rev.MissedProofOutputs[0].Value.Sub(cost)
	rev.MissedProofOutputs[2].Value = rev.MissedProofOutputs[2].Value.Add(cost)

	// move collateral from host to void
	rev.MissedProofOutputs[1].Value = rev.MissedProofOutputs[1].Value.Sub(collateral)
	rev.MissedProofOutputs[2].Value = rev.MissedProofOutputs[2].Value.Add(collateral)

	return []types.Currency{rev.ValidProofOutputs[0].Value, rev.ValidProofOutputs[1].Value},
		[]types.Currency{rev.MissedProofOutputs[0].Value, rev.MissedProofOutputs[1].Value, rev.MissedProofOutputs[2].Value}
}

func wrapResponseErr(err error, readCtx, rejectCtx string) error {
	if errors.As(err, new(*rhp2.RPCError)) {
		return fmt.Errorf("%s: %w", rejectCtx, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", readCtx, err)
	}
	return nil
}

type segWriter struct {
	w   io.Writer
	buf [rhp2.LeafSize * 64]byte
	len int
}

func (sw *segWriter) Write(p []byte) (int, error) {
	lenp := len(p)
	for len(p) > 0 {
		n := copy(sw.buf[sw.len:], p)
		sw.len += n
		p = p[n:]
		segs := sw.buf[:sw.len-(sw.len%rhp2.LeafSize)]
		if _, err := sw.w.Write(segs); err != nil {
			return 0, err
		}
		sw.len = copy(sw.buf[:], sw.buf[len(segs):sw.len])
	}
	return lenp, nil
}
