package api

import (
	"context"
	"encoding/json"

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

func (a *api) handleGETSessions(c jape.Context) {
	a.writeResponse(c, SessionResp(a.sessions.Active()))
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
	a.sessions.Unsubscribe(sub)
}
