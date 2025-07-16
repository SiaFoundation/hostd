package rhp

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

const (
	// minMessageSize is the minimum size of an RPC message
	minMessageSize = 4096
)

var (
	errContractLocked     = errors.New("contract is locked by another party")
	errInvalidMerkleProof = errors.New("host supplied invalid Merkle proof")
)

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

func wrapResponseErr(err error, readCtx, rejectCtx string) error {
	if errors.As(err, new(*rhp2.RPCError)) {
		return fmt.Errorf("%s: %w", rejectCtx, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", readCtx, err)
	}
	return nil
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func revisionOutputValues(rev types.FileContractRevision) (valid, missed []types.Currency) {
	valid = make([]types.Currency, len(rev.ValidProofOutputs))
	missed = make([]types.Currency, len(rev.MissedProofOutputs))

	for i := range rev.ValidProofOutputs {
		valid[i] = rev.ValidProofOutputs[i].Value
	}

	for i := range rev.MissedProofOutputs {
		missed[i] = rev.MissedProofOutputs[i].Value
	}
	return
}

func revisionTransfer(rev types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency) {
	valid, missed = revisionOutputValues(rev)
	if len(valid) != 2 || len(missed) != 3 {
		panic("unexpected number of outputs")
	}

	// move valid payout from renter to host
	valid[0] = rev.ValidProofOutputs[0].Value.Sub(cost)
	valid[1] = rev.ValidProofOutputs[1].Value.Add(cost)

	// move missed payout from renter to void
	missed[0] = rev.MissedProofOutputs[0].Value.Sub(cost)
	missed[2] = rev.MissedProofOutputs[2].Value.Add(cost)

	// move collateral from host to void
	missed[1] = rev.MissedProofOutputs[1].Value.Sub(collateral)
	missed[2] = missed[2].Add(collateral)
	return
}

func readSection(w io.Writer, t *rhp2.Transport, sec rhp2.RPCReadRequestSection) (hostSig *types.Signature, _ error) {
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

// RPCSettings calls the Settings RPC, returning the host's reported settings.
func RPCSettings(t *rhp2.Transport) (settings rhp2.HostSettings, err error) {
	var resp rhp2.RPCSettingsResponse
	if err := t.Call(rhp2.RPCSettingsID, nil, &resp); err != nil {
		return rhp2.HostSettings{}, err
	} else if err := json.Unmarshal(resp.Settings, &settings); err != nil {
		return rhp2.HostSettings{}, fmt.Errorf("couldn't unmarshal json: %w", err)
	}

	return settings, nil
}

// RPCFormContract forms a contract with a host.
func RPCFormContract(t *rhp2.Transport, renterKey types.PrivateKey, txnSet []types.Transaction) (rhp2.ContractRevision, []types.Transaction, error) {
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

// RPCWrite appends data to a contract.
func RPCWrite(t *rhp2.Transport, renterKey types.PrivateKey, rev *rhp2.ContractRevision, actions []rhp2.RPCWriteAction, price, collateral types.Currency) error {
	newValid, newMissed := revisionTransfer(rev.Revision, price, collateral)

	// precompute append roots
	var appendRoots []types.Hash256
	appendRoots = appendRoots[:0]
	contractSize := rev.Revision.Filesize
	for _, action := range actions {
		if action.Type == rhp2.RPCWriteActionAppend {
			appendRoots = append(appendRoots, proto4.SectorRoot((*[proto4.SectorSize]byte)(action.Data)))
		}

		switch action.Type {
		case rhp2.RPCWriteActionAppend:
			contractSize += proto4.SectorSize
		case rhp2.RPCWriteActionTrim:
			d := proto4.SectorSize * action.A
			if contractSize < d {
				return fmt.Errorf("contract size too small to trim %d sectors", action.A)
			}
			contractSize -= d
		}
	}
	req := &rhp2.RPCWriteRequest{
		Actions:     actions,
		MerkleProof: true,

		RevisionNumber:    rev.Revision.RevisionNumber + 1,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
	}
	err := t.WriteRequest(rhp2.RPCWriteID, req)
	if err != nil {
		return fmt.Errorf("failed to write request: %w", err)
	}

	var merkleResp rhp2.RPCWriteMerkleProof
	if err := t.ReadResponse(&merkleResp, 4096); err != nil {
		return fmt.Errorf("failed to read merkle proof response: %w", err)
	}

	// verify proof
	proofHashes := merkleResp.OldSubtreeHashes
	leafHashes := merkleResp.OldLeafHashes
	oldRoot, newRoot := rev.Revision.FileMerkleRoot, merkleResp.NewMerkleRoot
	if contractSize > 0 && !rhp2.VerifyDiffProof(actions, rev.NumSectors(), proofHashes, leafHashes, oldRoot, newRoot, appendRoots) {
		err := errInvalidMerkleProof
		t.WriteResponseErr(err)
		return err
	}

	// create new revision
	newRevision := rev.Revision
	newRevision.RevisionNumber = req.RevisionNumber
	newRevision.Filesize = contractSize
	newRevision.ValidProofOutputs = make([]types.SiacoinOutput, len(newValid))
	newRevision.MissedProofOutputs = make([]types.SiacoinOutput, len(newMissed))
	copy(newRevision.FileMerkleRoot[:], newRoot[:])
	for i := range newValid {
		newRevision.ValidProofOutputs[i].Address = rev.Revision.ValidProofOutputs[i].Address
		newRevision.ValidProofOutputs[i].Value = newValid[i]
	}
	for i := range newMissed {
		newRevision.MissedProofOutputs[i].Address = rev.Revision.MissedProofOutputs[i].Address
		newRevision.MissedProofOutputs[i].Value = newMissed[i]
	}
	revisionHash := hashRevision(newRevision)
	renterSig := &rhp2.RPCWriteResponse{
		Signature: renterKey.SignHash(revisionHash),
	}

	if err := t.WriteResponse(renterSig); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	var hostSig rhp2.RPCWriteResponse
	if err := t.ReadResponse(&hostSig, 4096); err != nil {
		return fmt.Errorf("failed to read host signature: %w", err)
	} else if !rev.HostKey().VerifyHash(revisionHash, hostSig.Signature) {
		return fmt.Errorf("host's signature is invalid")
	}

	rev.Revision = newRevision
	rev.Signatures[0].Signature = renterSig.Signature[:]
	rev.Signatures[1].Signature = hostSig.Signature[:]
	return nil
}

// RPCRead reads data from a contract.
func RPCRead(t *rhp2.Transport, w io.Writer, renterKey types.PrivateKey, rev *rhp2.ContractRevision, sections []rhp2.RPCReadRequestSection, price types.Currency) error {
	empty := true
	for _, s := range sections {
		empty = empty && s.Length == 0
	}
	if empty || len(sections) == 0 {
		return nil
	}

	// create new revision
	newValid, newMissed := revisionTransfer(rev.Revision, price, types.ZeroCurrency)
	newRevision := rev.Revision
	newRevision.RevisionNumber++
	newRevision.ValidProofOutputs = make([]types.SiacoinOutput, len(newValid))
	newRevision.MissedProofOutputs = make([]types.SiacoinOutput, len(newMissed))
	for i := range newValid {
		newRevision.ValidProofOutputs[i].Address = rev.Revision.ValidProofOutputs[i].Address
		newRevision.ValidProofOutputs[i].Value = newValid[i]
	}
	for i := range newMissed {
		newRevision.MissedProofOutputs[i].Address = rev.Revision.MissedProofOutputs[i].Address
		newRevision.MissedProofOutputs[i].Value = newMissed[i]
	}
	revisionHash := hashRevision(newRevision)
	renterSig := renterKey.SignHash(revisionHash)

	// construct the request
	req := &rhp2.RPCReadRequest{
		Sections:    sections,
		MerkleProof: true,

		RevisionNumber:    newRevision.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         renterSig,
	}

	if err := t.WriteRequest(rhp2.RPCReadID, req); err != nil {
		return fmt.Errorf("failed to write request: %w", err)
	}
	defer t.WriteResponse(&rhp2.RPCReadStop)

	var hostSig *types.Signature
	var err error
	// read all sections
	for i, sec := range sections {
		hostSig, err = readSection(w, t, sec)
		if err != nil {
			return fmt.Errorf("failed to read section %d: %w", i, err)
		} else if hostSig != nil {
			break // exit the loop; they won't be sending any more data
		}
	}

	// the host is required to send a signature; if they haven't sent one
	// yet, they should send an empty ReadResponse containing just the
	// signature.
	if hostSig == nil {
		var resp rhp2.RPCReadResponse
		if err := t.ReadResponse(&resp, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read signature", "host rejected Read request")
		}
		hostSig = &resp.Signature
	}

	// verify the host signature
	if !rev.HostKey().VerifyHash(revisionHash, *hostSig) {
		return errors.New("host's signature is invalid")
	}
	rev.Revision = newRevision
	rev.Signatures[0].Signature = renterSig[:]
	rev.Signatures[1].Signature = hostSig[:]
	return nil
}

// RPCRenewContract renews a contract with a host.
func RPCRenewContract(t *rhp2.Transport, renterKey types.PrivateKey, rev *rhp2.ContractRevision, txnSet []types.Transaction, price types.Currency) (rhp2.ContractRevision, []types.Transaction, error) {
	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	finalRevision := rev.Revision
	finalRevision.Filesize = 0
	finalRevision.RevisionNumber = types.MaxRevisionNumber
	finalRevision.FileMerkleRoot = types.Hash256{}
	finalRevision.ValidProofOutputs[0].Value = finalRevision.ValidProofOutputs[0].Value.Sub(price)
	finalRevision.ValidProofOutputs[1].Value = finalRevision.ValidProofOutputs[1].Value.Add(price)
	finalRevision.MissedProofOutputs = finalRevision.ValidProofOutputs

	newValid, newMissed := revisionOutputValues(rev.Revision)
	// construct the renew request
	req := &rhp2.RPCRenewAndClearContractRequest{
		Transactions:           txnSet,
		RenterKey:              renterKey.PublicKey().UnlockKey(),
		FinalValidProofValues:  newValid,
		FinalMissedProofValues: newMissed,
	}

	if err := t.WriteRequest(rhp2.RPCRenewClearContractID, req); err != nil {
		return rhp2.ContractRevision{}, nil, fmt.Errorf("failed to write request: %w", err)
	}

	var hostAdditions rhp2.RPCFormContractAdditions
	if err := t.ReadResponse(&hostAdditions, 65536); err != nil {
		return rhp2.ContractRevision{}, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, hostAdditions.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, hostAdditions.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID:         txn.FileContractID(0),
		UnlockConditions: finalRevision.UnlockConditions,
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
	initialRevRenterSig := renterKey.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      initialRevRenterSig[:],
	}

	// send renter signatures
	finalRevSigHash := hashRevision(finalRevision)
	finalRevSig := renterKey.SignHash(finalRevSigHash)
	err := t.WriteResponse(&rhp2.RPCRenewAndClearContractSignatures{
		ContractSignatures:     renterSignatures,
		RevisionSignature:      renterRevisionSig,
		FinalRevisionSignature: finalRevSig,
	})
	if err != nil {
		return rhp2.ContractRevision{}, nil, fmt.Errorf("failed to write response: %w", err)
	}

	var hostSigs rhp2.RPCRenewAndClearContractSignatures
	if err := t.ReadResponse(&hostSigs, 4096); err != nil {
		return rhp2.ContractRevision{}, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// merge host signatures with our own
	txn.Signatures = append(renterSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(hostAdditions.Parents, append(parents, txn)...)
	return rhp2.ContractRevision{
		Revision:   initRevision,
		Signatures: [2]types.TransactionSignature{renterRevisionSig, hostSigs.RevisionSignature},
	}, signedTxnSet, nil
}

// RPCSectorRoots fetches sector roots from a host.
func RPCSectorRoots(t *rhp2.Transport, renterKey types.PrivateKey, offset, limit uint64, rev *rhp2.ContractRevision, price types.Currency) ([]types.Hash256, error) {
	// create new revision
	newValid, newMissed := revisionTransfer(rev.Revision, price, types.ZeroCurrency)
	newRevision := rev.Revision
	newRevision.RevisionNumber++
	newRevision.ValidProofOutputs = make([]types.SiacoinOutput, len(newValid))
	newRevision.MissedProofOutputs = make([]types.SiacoinOutput, len(newMissed))
	for i := range newValid {
		newRevision.ValidProofOutputs[i].Address = rev.Revision.ValidProofOutputs[i].Address
		newRevision.ValidProofOutputs[i].Value = newValid[i]
	}
	for i := range newMissed {
		newRevision.MissedProofOutputs[i].Address = rev.Revision.MissedProofOutputs[i].Address
		newRevision.MissedProofOutputs[i].Value = newMissed[i]
	}
	revisionHash := hashRevision(newRevision)
	renterSig := renterKey.SignHash(revisionHash)

	// create request
	req := &rhp2.RPCSectorRootsRequest{
		RootOffset: offset,
		NumRoots:   limit,

		RevisionNumber:    newRevision.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         renterSig,
	}

	if err := t.WriteRequest(rhp2.RPCSectorRootsID, req); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp2.RPCSectorRootsResponse
	if err := t.ReadResponse(&resp, 4096); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// verify the host signature
	if !rev.HostKey().VerifyHash(revisionHash, resp.Signature) {
		return nil, errors.New("host's signature is invalid")
	}
	rev.Revision = newRevision
	rev.Signatures[0].Signature = req.Signature[:]
	rev.Signatures[1].Signature = resp.Signature[:]

	// verify the proof
	if !rhp2.VerifySectorRangeProof(resp.MerkleProof, resp.SectorRoots, offset, offset+limit, rev.NumSectors(), rev.Revision.FileMerkleRoot) {
		return nil, errInvalidMerkleProof
	}
	return resp.SectorRoots, nil
}

// RPCLock locks a contract with a host.
func RPCLock(t *rhp2.Transport, renterKey types.PrivateKey, id types.FileContractID) (rhp2.ContractRevision, error) {
	req := &rhp2.RPCLockRequest{
		ContractID: id,
		Signature:  t.SignChallenge(renterKey),
		Timeout:    uint64(30 * time.Second.Milliseconds()),
	}

	// execute lock RPC
	var resp rhp2.RPCLockResponse
	if err := t.Call(rhp2.RPCLockID, req, &resp); err != nil {
		return rhp2.ContractRevision{}, err
	}
	t.SetChallenge(resp.NewChallenge)

	if len(resp.Signatures) != 2 {
		return rhp2.ContractRevision{}, fmt.Errorf("host returned wrong number of signatures (expected 2, got %v)", len(resp.Signatures))
	} else if len(resp.Signatures[0].Signature) != 64 || len(resp.Signatures[1].Signature) != 64 {
		return rhp2.ContractRevision{}, errors.New("signatures on claimed revision have wrong length")
	}

	revHash := hashRevision(resp.Revision)
	if !renterKey.PublicKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[0].Signature)) {
		return rhp2.ContractRevision{}, errors.New("renter's signature on claimed revision is invalid")
	} else if !t.HostKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[1].Signature)) {
		return rhp2.ContractRevision{}, errors.New("host's signature on claimed revision is invalid")
	} else if !resp.Acquired {
		return rhp2.ContractRevision{}, errContractLocked
	}
	return rhp2.ContractRevision{
		Revision:   resp.Revision,
		Signatures: [2]types.TransactionSignature{resp.Signatures[0], resp.Signatures[1]},
	}, nil
}

// RPCUnlock unlocks a contract with a host.
func RPCUnlock(t *rhp2.Transport) error {
	return t.WriteRequest(rhp2.RPCUnlockID, nil)
}
