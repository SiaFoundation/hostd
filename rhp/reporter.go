package rhp

import (
	"encoding/hex"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"lukechampine.com/frand"
)

// SessionEventType is the type of a session event.
const (
	SessionEventTypeStart    = "sessionStart"
	SessionEventTypeEnd      = "sessionEnd"
	SessionEventTypeRPCStart = "rpcStart"
	SessionEventTypeRPCEnd   = "rpcEnd"
)

// SessionProtocol is the protocol used by a session.
const (
	SessionProtocolTCP = "tcp"
	SessionProtocolWS  = "websocket"
)

type (
	// UID is a unique identifier for a session or RPC.
	UID [8]byte

	// A Session is an open connection between a host and a renter.
	Session struct {
		conn *Conn

		ID             UID             `json:"id"`
		Protocol       string          `json:"protocol"`
		RHPVersion     int             `json:"rhpVersion"`
		PeerAddress    string          `json:"peerAddress"`
		Ingress        uint64          `json:"ingress"`
		Egress         uint64          `json:"egress"`
		Usage          contracts.Usage `json:"usage"`
		SuccessfulRPCs uint64          `json:"successfulRPCs"`
		FailedRPCs     uint64          `json:"failedRPCs"`

		Timestamp time.Time `json:"timestamp"`
	}

	// An RPC is an RPC call made by a renter to a host.
	RPC struct {
		ID        UID             `json:"ID"`
		SessionID UID             `json:"sessionID"`
		RPC       types.Specifier `json:"rpc"`
		Usage     contracts.Usage `json:"usage"`
		Error     error           `json:"error,omitempty"`
		Elapsed   time.Duration   `json:"timestamp"`
	}

	// A SessionSubscriber receives session events.
	SessionSubscriber interface {
		ReceiveSessionEvent(SessionEvent)
	}

	// A SessionReporter manages open sessions and reports session events to
	// subscribers.
	SessionReporter struct {
		mu          sync.Mutex
		sessions    map[UID]Session
		subscribers []SessionSubscriber
	}

	// A SessionEvent is an event that occurs during a session.
	SessionEvent struct {
		Type    string  `json:"type"`
		Session Session `json:"session"`
		RPC     any     `json:"rpc,omitempty"`
	}
)

// String returns the hex-encoded string representation of the UID.
func (u UID) String() string {
	return hex.EncodeToString(u[:])
}

func (sr *SessionReporter) updateSubscribers(sessionID UID, eventType string, rpc any) {
	sess, ok := sr.sessions[sessionID]
	if !ok {
		return
	}

	sess.Ingress, sess.Egress = sess.conn.Usage()
	sr.sessions[sessionID] = sess

	for _, sub := range sr.subscribers {
		sub.ReceiveSessionEvent(SessionEvent{
			Type:    eventType,
			Session: sess,
			RPC:     rpc,
		})
	}
}

// Subscribe subscribes to session events.
func (sr *SessionReporter) Subscribe(sub SessionSubscriber) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.subscribers = append(sr.subscribers, sub)
}

// StartSession starts a new session and returns a function that should be
// called when the session ends.
func (sr *SessionReporter) StartSession(conn *Conn, proto string, version int) (sessionID UID, end func()) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	copy(sessionID[:], frand.Bytes(8))
	sr.sessions[sessionID] = Session{
		conn: conn,

		ID:          sessionID,
		RHPVersion:  version,
		Protocol:    proto,
		PeerAddress: conn.RemoteAddr().String(),
		Timestamp:   time.Now(),
	}
	sr.updateSubscribers(sessionID, SessionEventTypeStart, nil)
	return sessionID, func() {
		sr.mu.Lock()
		defer sr.mu.Unlock()

		sr.updateSubscribers(sessionID, SessionEventTypeEnd, nil)
		delete(sr.sessions, sessionID)
	}
}

// StartRPC starts a new RPC and returns a function that should be called when
// the RPC ends.
func (sr *SessionReporter) StartRPC(sessionID UID, rpc types.Specifier) (rpcID UID, end func(contracts.Usage, error)) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	copy(rpcID[:], frand.Bytes(8))
	_, ok := sr.sessions[sessionID]
	if !ok {
		return rpcID, func(contracts.Usage, error) {}
	}

	event := RPC{
		ID:        rpcID,
		SessionID: sessionID,
		RPC:       rpc,
	}
	rpcStart := time.Now()
	sr.updateSubscribers(sessionID, SessionEventTypeRPCStart, event)
	return rpcID, func(usage contracts.Usage, err error) {
		// update event
		event.Error = err
		event.Elapsed = time.Since(rpcStart)
		event.Usage = usage

		sr.mu.Lock()
		defer sr.mu.Unlock()

		sess, ok := sr.sessions[sessionID]
		if !ok {
			return
		}

		// update session
		if err == nil {
			sess.SuccessfulRPCs++
		} else {
			sess.FailedRPCs++
		}
		sess.Usage = sess.Usage.Add(usage)
		sr.sessions[sessionID] = sess
		// update subscribers
		sr.updateSubscribers(sessionID, SessionEventTypeRPCEnd, event)
	}
}

// Active returns a snapshot of the currently active sessions.
func (sr *SessionReporter) Active() []Session {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sessions := make([]Session, 0, len(sr.sessions))
	for _, sess := range sr.sessions {
		// update session usage
		sess.Ingress, sess.Egress = sess.conn.Usage()
		sr.sessions[sess.ID] = sess
		// append to slice
		sessions = append(sessions, sess)
	}
	return sessions
}

// NewSessionReporter returns a new SessionReporter.
func NewSessionReporter() *SessionReporter {
	return &SessionReporter{
		sessions: make(map[UID]Session),
	}
}
