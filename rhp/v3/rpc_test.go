package rhp

import (
	"errors"
	"net"
	"testing"
	"time"

	nlog "gitlab.com/NebulousLabs/log"
	nmux "gitlab.com/NebulousLabs/siamux"
	smux "gitlab.com/NebulousLabs/siamux/mux"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	smods "go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

func startHostTransport(priv types.PrivateKey) (string, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	go func() {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		} else if err != nil {
			panic(err)
		}

		t, err := rhpv3.NewHostTransport(conn, priv)
		if err != nil {
			panic(err)
		}

		stream, err := t.AcceptStream()
		if err != nil {
			panic(err)
		} else if err := stream.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			panic(err)
		}

		id, err := stream.ReadID()
		if err != nil {
			panic(err)
		} else if id != rhpv3.RPCUpdatePriceTableID {
			panic("unexpected id")
		}

		resp := rhpv3.RPCUpdatePriceTableResponse{
			PriceTableJSON: []byte("hello world"),
		}
		if err := stream.WriteResponse(&resp); err != nil {
			panic(err)
		}
	}()
	return l.Addr().String(), nil
}

func TestTransportReadWriteCompat(t *testing.T) {
	privKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	addr, err := startHostTransport(privKey)
	if err != nil {
		t.Fatal(err)
	}

	m, err := nmux.New(":0", ":0", nlog.DiscardLogger, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	pubKey := privKey.PublicKey()
	var muxKey smux.ED25519PublicKey
	copy(muxKey[:], pubKey[:])
	stream, err := m.NewStream("host", addr, muxKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	if err := stream.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := smods.RPCWrite(stream, smods.RPCUpdatePriceTable); err != nil {
		t.Fatal(err)
	}

	var resp smods.RPCUpdatePriceTableResponse
	if err := smods.RPCRead(stream, &resp); err != nil {
		t.Fatal(err)
	} else if string(resp.PriceTableJSON) != "hello world" {
		t.Fatal("unexpected response")
	}
}
