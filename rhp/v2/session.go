package rhp

import (
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
)

// minMessageSize is the minimum size of an RPC message. If an encoded message
// would be smaller than minMessageSize, the sender MAY pad it with random data.
// This hinders traffic analysis by obscuring the true sizes of messages.
const minMessageSize = 4096

// A session is an ongoing exchange of RPCs via the renter-host protocol.
type session struct {
	t *rhp2.Transport

	contract contracts.SignedRevision
}

func (s *session) readRequest(req rhp2.ProtocolObject, maxSize uint64, timeout time.Duration) error {
	s.t.SetReadDeadline(time.Now().Add(timeout))
	return s.t.ReadRequest(req, maxSize)
}

func (s *session) readResponse(req rhp2.ProtocolObject, maxSize uint64, timeout time.Duration) error {
	s.t.SetReadDeadline(time.Now().Add(timeout))
	return s.t.ReadResponse(req, maxSize)
}

func (s *session) writeResponse(resp rhp2.ProtocolObject, timeout time.Duration) error {
	s.t.SetWriteDeadline(time.Now().Add(timeout))
	return s.t.WriteResponse(resp)
}

// ContractRevisable returns an error if a contract is not locked or can't be
// revised. A contract is revisable if the revision number is not the max uint64
// value and it is not close to the proof window.
func (s *session) ContractRevisable() error {
	switch {
	case s.contract.Revision.ParentID == (types.FileContractID{}):
		return ErrNoContractLocked
	case s.contract.Revision.RevisionNumber == types.MaxRevisionNumber:
		return ErrContractRevisionLimit
	}
	return nil
}
