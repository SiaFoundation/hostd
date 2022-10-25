// Package renterhost implements the handshake and transport for the Sia
// renter-host protocol.
package rhp

import (
	"crypto/cipher"
	"crypto/ed25519"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// minMessageSize is the minimum size of an RPC message. If an encoded message
// would be smaller than minMessageSize, the sender MAY pad it with random data.
// This hinders traffic analysis by obscuring the true sizes of messages.
const minMessageSize = 4096

// A session is an ongoing exchange of RPCs via the renter-host protocol.
type session struct {
	conn   *rpcConn
	aead   cipher.AEAD
	inbuf  objBuffer
	outbuf objBuffer

	contract  contracts.SignedRevision
	challenge [16]byte

	uid     UniqueID
	spent   types.Currency
	metrics MetricReporter

	mu     sync.Mutex // guards the fields below
	err    error      // set when session is prematurely closed
	closed bool
}

func (s *session) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil && s.err == nil {
		s.conn.Close()
		s.err = err
	}
}

// error returns the first error encountered during the session.
func (s *session) error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// isClosed returns whether the session is closed. Check prematureCloseErr to
// determine whether the session was closed gracefully.
func (s *session) isClosed() bool {
	return s.closed || s.err != nil
}

func (s *session) writeMessage(obj rpcObject) error {
	if err := s.error(); err != nil {
		return err
	}
	// generate random nonce
	nonce := make([]byte, 256)[:s.aead.NonceSize()] // avoid heap alloc
	frand.Read(nonce)

	// pad short messages to minMessageSize
	msgSize := 8 + s.aead.NonceSize() + obj.marshalledSize() + s.aead.Overhead()
	if msgSize < minMessageSize {
		msgSize = minMessageSize
	}
	// write length prefix, nonce, and object directly into buffer
	s.outbuf.reset()
	s.outbuf.grow(msgSize)
	s.outbuf.writePrefix(msgSize - 8)
	s.outbuf.write(nonce)
	obj.marshalBuffer(&s.outbuf)

	// encrypt the object in-place
	msg := s.outbuf.bytes()[:msgSize]
	msgNonce := msg[8:][:len(nonce)]
	payload := msg[8+len(nonce) : msgSize-s.aead.Overhead()]
	s.aead.Seal(payload[:0], msgNonce, payload, nil)

	_, err := s.conn.Write(msg)
	s.setErr(err)
	return err
}

func (s *session) readMessage(obj rpcObject, maxLen uint64) error {
	if err := s.error(); err != nil {
		return err
	}
	if maxLen < minMessageSize {
		maxLen = minMessageSize
	}
	s.inbuf.reset()
	if err := s.inbuf.copyN(s.conn, 8); err != nil {
		s.setErr(err)
		return err
	}
	msgSize := s.inbuf.readUint64()
	if msgSize > maxLen {
		return fmt.Errorf("message size (%v bytes) exceeds maxLen of %v bytes", msgSize, maxLen)
	} else if msgSize < uint64(s.aead.NonceSize()+s.aead.Overhead()) {
		return fmt.Errorf("message size (%v bytes) is too small (nonce + MAC is %v bytes)", msgSize, s.aead.NonceSize()+s.aead.Overhead())
	}

	s.inbuf.reset()
	s.inbuf.grow(int(msgSize))
	if err := s.inbuf.copyN(s.conn, msgSize); err != nil {
		s.setErr(err)
		return err
	}

	nonce := s.inbuf.next(s.aead.NonceSize())
	paddedPayload := s.inbuf.bytes()
	_, err := s.aead.Open(paddedPayload[:0], nonce, paddedPayload, nil)
	if err != nil {
		s.setErr(err) // not an I/O error, but still fatal
		return err
	}
	return obj.unmarshalBuffer(&s.inbuf)
}

// SignChallenge signs the current session challenge.
func (s *session) SignChallenge(priv ed25519.PrivateKey) []byte {
	sigHash := hashChallenge(s.challenge)
	return ed25519.Sign(priv, sigHash[:])
}

// VerifyChallenge verifies a signature of the current session challenge.
func (s *session) VerifyChallenge(sig []byte, pub ed25519.PublicKey) bool {
	sigHash := hashChallenge(s.challenge)
	return ed25519.Verify(pub, sigHash[:], sig)
}

// ReadID reads an RPC request ID. If the renter sends the session termination
// signal, ReadID returns ErrRenterClosed.
func (s *session) ReadID() (rpcID Specifier, err error) {
	s.conn.SetReadDeadline(time.Now().Add(time.Hour))
	err = s.readMessage(&rpcID, minMessageSize)
	if rpcID == loopExit {
		err = ErrRenterClosed
	}
	return
}

// ReadRequest reads an RPC request using the new loop protocol.
func (s *session) ReadRequest(req rpcObject) error {
	var maxSize uint64
	var timeout time.Duration

	switch req.(type) {
	case *rpcFormContractRequest, *rpcRenewAndClearContractRequest:
		maxSize, timeout = modules.TransactionSizeLimit, 2*time.Minute
	case *rpcLockRequest, *rpcSectorRootsRequest, *rpcFormContractSignatures:
		maxSize, timeout = minMessageSize, 30*time.Second
	case *rpcReadRequest:
		maxSize, timeout = 4*minMessageSize, 2*time.Minute
	case *rpcWriteRequest:
		maxSize, timeout = 10*merkle.SectorSize, 2*time.Minute
	default:
		panic(fmt.Sprintf("unrecognized request type %T", req))
	}
	s.conn.SetReadDeadline(time.Now().Add(timeout))
	return s.readMessage(req, maxSize)
}

// WriteError writes an RPC error response. If err is an *RPCError, it is sent
// directly; otherwise, a generic RPCError is created from err's Error string.
func (s *session) WriteError(err error) error {
	re, ok := err.(*rpcError)
	if err != nil && !ok {
		re = &rpcError{Description: err.Error()}
	}
	s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	s.writeMessage(&rpcResponse{re, nil})
	return err
}

// WriteResponse writes an RPC response object.
func (s *session) WriteResponse(resp rpcObject, timeout time.Duration) error {
	s.conn.SetWriteDeadline(time.Now().Add(timeout))
	return s.writeMessage(&rpcResponse{nil, resp})
}

// ReadResponse reads an RPC response. If the response is an error, it is
// returned directly.
func (s *session) ReadResponse(resp rpcObject, maxSize uint64, timeout time.Duration) error {
	if maxSize < minMessageSize {
		maxSize = minMessageSize
	}
	rr := rpcResponse{nil, resp}
	s.conn.SetReadDeadline(time.Now().Add(timeout))
	if err := s.readMessage(&rr, maxSize); err != nil {
		return fmt.Errorf("failed to read message: %w", err)
	} else if rr.err != nil {
		return fmt.Errorf("remote error: %w", rr.err)
	}
	return nil
}

// spend increments the session's spent amount
func (s *session) Spend(n types.Currency) {
	s.spent = s.spent.Add(n)
}

func (s *session) ReviseContract(contracts ContractManager, revision types.FileContractRevision, hostSig, renterSig []byte) error {
	s.contract.Revision = revision
	copy(s.contract.HostSignature[:], hostSig)
	copy(s.contract.RenterSignature[:], renterSig)

	return contracts.ReviseContract(revision, renterSig, hostSig)
}

// ContractRevisable returns an error if a contract is not locked or can't be
// revised. A contract is revisable if the revision number is not the max uint64
// value and it is not close to the proof window.
func (s *session) ContractRevisable(height uint64) error {
	switch {
	case s.contract.Revision.ParentID == (types.FileContractID{}):
		return ErrNoContractLocked
	case s.contract.Revision.NewRevisionNumber == maxRevisionNumber:
		return ErrContractRevisionLimit
	case uint64(s.contract.Revision.NewWindowStart) < height:
		return ErrContractProofWindowStarted
	case uint64(s.contract.Revision.NewWindowEnd) < height:
		return ErrContractExpired
	}
	return nil
}

// Close gracefully terminates the RPC loop and closes the connection.
func (s *session) Close() error {
	if s.isClosed() {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return s.conn.Close()
}
