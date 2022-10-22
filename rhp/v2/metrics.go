package rhp

import (
	"encoding/hex"
	"time"

	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// A UniqueID is a unique identifier for an RPC or Session.
type UniqueID [8]byte

// String returns a string representation of the UniqueID.
func (u UniqueID) String() string {
	return hex.EncodeToString(u[:])
}

// MarshalJSON marshals the UniqueID to JSON.
func (u UniqueID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + u.String() + `"`), nil
}

// generateUniqueID returns a random UniqueID.
func generateUniqueID() (id UniqueID) {
	frand.Read(id[:])
	return
}

type (
	// EventSessionStart records the start of a new renter session.
	EventSessionStart struct {
		UID       UniqueID  `json:"uid"`
		RenterIP  string    `json:"renterIP"`
		Timestamp time.Time `json:"timestamp"`
	}

	// EventSessionEnd records the end of a renter session.
	EventSessionEnd struct {
		UID       UniqueID      `json:"uid"`
		Timestamp time.Time     `json:"timestamp"`
		Elapsed   time.Duration `json:"elapsed"`
	}

	// EventRPCStart records the start of an RPC.
	EventRPCStart struct {
		RPC        Specifier `json:"rpc"`
		SessionUID UniqueID  `json:"sessionUID"`
		Timestamp  time.Time `json:"timestamp"`
	}

	// EventRPCEnd records the end of an RPC.
	EventRPCEnd struct {
		RPC        Specifier `json:"rpc"`
		SessionUID UniqueID  `json:"sessionUID"`
		Error      error     `json:"error"`

		Spending   types.Currency `json:"spending"`
		ReadBytes  uint64         `json:"readBytes"`
		WriteBytes uint64         `json:"writeBytes"`

		Elapsed   time.Duration `json:"elapsed"`
		Timestamp time.Time     `json:"timestamp"`
	}

	EventContractFormed struct {
		SessionUID UniqueID                   `json:"sessionUID"`
		ContractID types.FileContractID       `json:"contractID"`
		Contract   types.FileContractRevision `json:"contract"`
	}

	// EventContractRenewed records the renewal of a contract.
	EventContractRenewed struct {
		SessionUID          UniqueID                   `json:"sessionUID"`
		ContractID          types.FileContractID       `json:"contractID"`
		FinalizedContractID types.FileContractID       `json:"finalizedContractID"`
		Contract            types.FileContractRevision `json:"contract"`
		FinalizedContract   types.FileContractRevision `json:"finalizedContract"`
	}
)

func (sh *SessionHandler) recordSessionStart(s *session) func() {
	start := time.Now()
	s.uid = generateUniqueID()
	sh.metrics.Report(EventSessionStart{
		UID:       s.uid,
		RenterIP:  s.conn.RemoteAddr().String(),
		Timestamp: start,
	})
	return func() {
		sh.metrics.Report(EventSessionEnd{
			UID:       s.uid,
			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
		})
	}
}

func (sh *SessionHandler) recordRPC(id Specifier, s *session) func(error) {
	start := time.Now()
	sh.metrics.Report(EventRPCStart{
		RPC:        id,
		SessionUID: s.uid,
		Timestamp:  start,
	})
	rs, ws := s.conn.usage()
	spent := s.spent
	return func(err error) {
		re, we := s.conn.usage()

		sh.metrics.Report(EventRPCEnd{
			RPC:        id,
			SessionUID: s.uid,
			Error:      err,

			Spending:   s.spent.Sub(spent),
			ReadBytes:  re - rs,
			WriteBytes: we - ws,

			Elapsed:   time.Since(start),
			Timestamp: time.Now(),
		})
	}
}
