package rhp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/internal/testutil"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type (
	quicTransportClient struct {
		peerKey types.PublicKey
		conn    quic.Connection
	}
	qtStream struct {
		quic.Stream

		localAddr, remoteAddr net.Addr
	}

	webTransportStream struct {
		webtransport.Stream

		localAddr, remoteAddr net.Addr
	}

	webTransportClient struct {
		peerKey types.PublicKey
		sess    *webtransport.Session
	}
)

func (qt *qtStream) LocalAddr() net.Addr {
	return qt.localAddr
}

func (qt *qtStream) RemoteAddr() net.Addr {
	return qt.remoteAddr
}

func (qc *quicTransportClient) DialStream(context.Context) net.Conn {
	s, err := qc.conn.OpenStream()
	if err != nil {
		panic(err)
	}
	return &qtStream{
		Stream: s,

		localAddr:  qc.conn.LocalAddr(),
		remoteAddr: qc.conn.RemoteAddr(),
	}
}

func (qc *quicTransportClient) FrameSize() int {
	return 1440
}

func (qc *quicTransportClient) PeerKey() types.PublicKey {
	return qc.peerKey
}

func (qc *quicTransportClient) Close() error {
	return qc.conn.CloseWithError(0, "")
}

func (ws *webTransportStream) LocalAddr() net.Addr {
	return ws.localAddr
}

func (ws *webTransportStream) RemoteAddr() net.Addr {
	return ws.remoteAddr
}

func (wt *webTransportClient) DialStream(context.Context) net.Conn {
	s, err := wt.sess.OpenStream()
	if err != nil {
		panic(err)
	}
	return &webTransportStream{
		Stream: s,

		localAddr:  wt.sess.LocalAddr(),
		remoteAddr: wt.sess.RemoteAddr(),
	}
}

func (wt *webTransportClient) FrameSize() int {
	return 1440
}

func (wt *webTransportClient) PeerKey() types.PublicKey {
	return wt.peerKey
}

func (wt *webTransportClient) Close() error {
	return wt.sess.CloseWithError(0, "")
}

// Generate bare-bones TLS config for the server
func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal("failed to generate key:", err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(frand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal("failed to create certificate:", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal("failed to create tls key pair:", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"sia/rhp4"},
	}
}

func TestQuic(t *testing.T) {
	log := zaptest.NewLogger(t)

	network, genesis := testutil.V2Network()
	pk := types.GeneratePrivateKey()
	host := testutil.NewHostNode(t, pk, network, genesis, log)
	s := rhp4.NewServer(pk, host.Chain, host.Syncer, host.Contracts, host.Wallet, host.Settings, host.Volumes)

	cfg := testTLSConfig(t)
	cfg.NextProtos = []string{"h3", "sia/rhp4"}
	ql, err := quic.ListenAddr("localhost:4848", cfg, &quic.Config{
		EnableDatagrams: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ql.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rhp.ServeRHP4QUIC(ctx, ql, s, log)

	t.Run("quic", func(t *testing.T) {
		qc, err := quic.DialAddr(context.Background(), "localhost:4848", &tls.Config{
			NextProtos:         []string{"sia/rhp4"},
			InsecureSkipVerify: true,
		}, &quic.Config{
			EnableDatagrams: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer qc.CloseWithError(0, "")

		qt := &quicTransportClient{
			conn:    qc,
			peerKey: pk.PublicKey(),
		}
		defer qt.Close()

		settings, err := rhp4.RPCSettings(ctx, qt)
		if err != nil {
			t.Fatal(err)
		}
		expected := host.Settings.RHP4Settings()
		settings.ProtocolVersion, expected.ProtocolVersion = [3]byte{0, 0, 0}, [3]byte{0, 0, 0}
		settings.Prices.Signature, expected.Prices.Signature = types.Signature{}, types.Signature{}
		settings.Prices.ValidUntil, expected.Prices.ValidUntil = time.Time{}, time.Time{}

		if !reflect.DeepEqual(settings, expected) {
			t.Fatal("unexpected settings:", settings, expected)
		}
	})

	t.Run("webtransport", func(t *testing.T) {
		d := &webtransport.Dialer{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}

		_, sess, err := d.Dial(ctx, "https://localhost:4848/sia/rhp/v4", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer sess.CloseWithError(0, "")

		wt := &webTransportClient{
			sess:    sess,
			peerKey: pk.PublicKey(),
		}
		defer wt.Close()

		settings, err := rhp4.RPCSettings(ctx, wt)
		if err != nil {
			t.Fatal(err)
		}
		expected := host.Settings.RHP4Settings()
		settings.ProtocolVersion, expected.ProtocolVersion = [3]byte{0, 0, 0}, [3]byte{0, 0, 0}
		settings.Prices.Signature, expected.Prices.Signature = types.Signature{}, types.Signature{}
		settings.Prices.ValidUntil, expected.Prices.ValidUntil = time.Time{}, time.Time{}

		if !reflect.DeepEqual(settings, expected) {
			t.Fatal("unexpected settings:", settings, expected)
		}
	})
}
