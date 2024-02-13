package rhp_test

import (
	"context"
	"encoding/json"
	"testing"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/hostd/internal/test"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
	"nhooyr.io/websocket"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWebSockets(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	c, _, err := websocket.Dial(context.Background(), "ws://"+host.RHP3WSAddr()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	conn := websocket.NetConn(context.Background(), c, websocket.MessageBinary)
	transport, err := rhp3.NewRenterTransport(conn, host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	stream := transport.DialStream()
	defer stream.Close()

	if err := stream.WriteRequest(rhp3.RPCUpdatePriceTableID, nil); err != nil {
		t.Fatal(err)
	}
	var resp rhp3.RPCUpdatePriceTableResponse
	if err := stream.ReadResponse(&resp, 4096); err != nil {
		t.Fatal(err)
	}
	var pt rhp3.HostPriceTable
	if err := json.Unmarshal(resp.PriceTableJSON, &pt); err != nil {
		t.Fatal(err)
	}
}
