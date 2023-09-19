package rhp

import (
	"encoding/hex"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

const (
	SessionEventTypeStart    = "sessionStart"
	SessionEventTypeEnd      = "sessionEnd"
	SessionEventTypeRPCStart = "rpcStart"
	SessionEventTypeRPCEnd   = "rpcEnd"

	SessionProtocolTCP = "tcp"
	SessionProtocolWS  = "websocket"
)

type (
	UID [8]byte // unique identifier

	Session struct {
		conn *Conn

		ID             UID    `json:"id"`
		Protocol       string `json:"protocol"`
		RHPVersion     int    `json:"rhpVersion"`
		PeerAddress    string `json:"peerAddress"`
		Ingress        uint64 `json:"ingress"`
		Egress         uint64 `json:"egress"`
		SuccessfulRPCs uint64 `json:"successfulRPCs"`
		FailedRPCs     uint64 `json:"failedRPCs"`

		Timestamp time.Time `json:"timestamp"`
	}

	RPC struct {
		ID        UID             `json:"ID"`
		SessionID UID             `json:"sessionID"`
		RPC       types.Specifier `json:"rpc"`
		Error     error           `json:"error,omitempty"`
		Timestamp time.Time       `json:"timestamp"`
	}

	SessionReporter struct {
		mu          sync.Mutex
		sessions    map[UID]Session
		subscribers []SessionSubscriber
	}

	SessionEvent struct {
		Type    string  `json:"type"`
		Session Session `json:"session"`
		RPC     any     `json:"rpc,omitempty"`
	}

	SessionSubscriber interface {
		ReceiveSessionEvent(SessionEvent)
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
func (sr *SessionReporter) StartRPC(sessionID UID, rpc types.Specifier) (rpcID UID, end func(error)) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	copy(rpcID[:], frand.Bytes(8))
	_, ok := sr.sessions[sessionID]
	if !ok {
		return rpcID, func(error) {}
	}

	event := RPC{
		ID:        rpcID,
		SessionID: sessionID,
		RPC:       rpc,
		Timestamp: time.Now(),
	}
	sr.updateSubscribers(sessionID, SessionEventTypeRPCStart, event)
	return rpcID, func(err error) {
		sr.mu.Lock()
		defer sr.mu.Unlock()

		sess, ok := sr.sessions[sessionID]
		if !ok {
			return
		}

		// update event
		event.Error = err

		// update session
		if err == nil {
			sess.SuccessfulRPCs++
		} else {
			sess.FailedRPCs++
		}
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
