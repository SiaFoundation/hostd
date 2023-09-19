package rhp

import (
	"context"
	"net/http"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

// handleWebSockets handles websocket connections to the host.
func (sh *SessionHandler) handleWebSockets(w http.ResponseWriter, r *http.Request) {
	log := sh.log.Named("websockets").With(zap.String("peerAddr", r.RemoteAddr))
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		log.Error("failed to accept websocket connection", zap.Error(err))
		return
	}
	defer wsConn.Close(websocket.StatusNormalClosure, "")

	// wrap the websocket connection
	conn := websocket.NetConn(context.Background(), wsConn, websocket.MessageBinary)
	defer conn.Close()

	// wrap the connection with a rate limiter
	ingress, egress := sh.settings.BandwidthLimiters()
	rhpConn := rhp.NewConn(conn, sh.monitor, ingress, egress)
	defer rhpConn.Close()

	// initiate the session
	sessionID, end := sh.sessions.StartSession(rhpConn, rhp.SessionProtocolWS, 3)
	defer end()

	log = log.With(zap.String("sessionID", sessionID.String()))

	// upgrade the connection
	t, err := rhpv3.NewHostTransport(rhpConn, sh.privateKey)
	if err != nil {
		sh.log.Debug("failed to upgrade conn", zap.Error(err), zap.String("remoteAddress", conn.RemoteAddr().String()))
		return
	}
	defer t.Close()

	for {
		stream, err := t.AcceptStream()
		if err != nil {
			log.Debug("failed to accept stream", zap.Error(err))
			return
		}

		go sh.handleHostStream(stream, sessionID, log)
	}
}

// WebSocketHandler returns an http.Handler that upgrades the connection to a
// WebSocket and then passes the connection to the RHP3 host transport.
func (sh *SessionHandler) WebSocketHandler() http.HandlerFunc {
	return sh.handleWebSockets
}
