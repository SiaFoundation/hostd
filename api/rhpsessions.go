package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"go.sia.tech/hostd/rhp"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

type rhpSessionSubscriber struct {
	conn *websocket.Conn
}

func (rs *rhpSessionSubscriber) ReceiveSessionEvent(event rhp.SessionEvent) {
	buf, err := json.Marshal(event)
	if err != nil {
		return
	}
	rs.conn.Write(context.Background(), websocket.MessageText, buf)
}

func (a *api) handleGETSessionsPrometheus(c jape.Context) {
	active_sessions := a.sessions.Active()

	resulttext := ""
	for i, session := range active_sessions {
		text := fmt.Sprintf(`hostd_session_ingress{peer="%s"} %d
hostd_session_egress{peer="%s"} %d
hostd_session_successfulrpcs{peer="%s"} %d
hostd_session_failedrpcs{peer="%s"} %d`,
			session.PeerAddress, session.Ingress,
			session.PeerAddress, session.Egress,
			session.PeerAddress, session.SuccessfulRPCs,
			session.PeerAddress, session.FailedRPCs,
		)
		if i != len(active_sessions)-1 {
			text = text + "\n"
		}
		resulttext = resulttext + text
	}

	var resultbuffer bytes.Buffer
	resultbuffer.WriteString(resulttext)
	c.ResponseWriter.Write(resultbuffer.Bytes())
}

func (a *api) handleGETSessions(c jape.Context) {
	c.Encode(a.sessions.Active())
}

func (a *api) handleGETSessionsSubscribe(c jape.Context) {
	wsc, err := websocket.Accept(c.ResponseWriter, c.Request, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		a.log.Warn("failed to accept websocket connection", zap.Error(err))
		return
	}
	defer wsc.Close(websocket.StatusNormalClosure, "")

	// subscribe the websocket conn
	sub := &rhpSessionSubscriber{
		conn: wsc,
	}
	a.sessions.Subscribe(sub)
	defer a.sessions.Unsubscribe(sub)
}
