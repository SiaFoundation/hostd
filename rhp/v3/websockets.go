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
	log := sh.log.Named("websockets").With(zap.String("remoteAddr", r.RemoteAddr))
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		log.Error("failed to accept websocket connection", zap.Error(err))
		return
	}
	defer wsConn.Close(websocket.StatusNormalClosure, "")

	conn := websocket.NetConn(context.Background(), wsConn, websocket.MessageBinary)
	ingress, egress := sh.settings.BandwidthLimiters()
	t, err := rhpv3.NewHostTransport(rhp.NewConn(conn, sh.monitor, ingress, egress), sh.privateKey)
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
		go sh.handleHostStream(conn.RemoteAddr().String(), stream)
	}
}

// WebSocketHandler returns an http.Handler that upgrades the connection to a
// WebSocket and then passes the connection to the RHP3 host transport.
func (sh *SessionHandler) WebSocketHandler() http.HandlerFunc {
	return sh.handleWebSockets
}
