package rhp

import (
	"context"
	"errors"
	"net"

	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

// wtTranposrt is a rhp4.Transport that wraps a WebTransport Session
type wtTransport struct {
	s *webtransport.Session

	localAddr, remoteAddr net.Addr
}

type wtStream struct {
	webtransport.Stream

	localAddr, remoteAddr net.Addr
}

func (wts *wtStream) LocalAddr() net.Addr {
	return wts.localAddr
}

func (wts *wtStream) RemoteAddr() net.Addr {
	return wts.remoteAddr
}

// Close implements the rhp4.Transport interface.
func (wt *wtTransport) Close() error {
	return wt.s.CloseWithError(0, "")
}

// AcceptStream implements the rhp4.Transport interface.
func (wt *wtTransport) AcceptStream() (net.Conn, error) {
	conn, err := wt.s.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &wtStream{
		Stream:     conn,
		localAddr:  wt.localAddr,
		remoteAddr: wt.remoteAddr,
	}, nil
}

type (
	qtTransport struct {
		qc quic.Connection

		localAddr, remoteAddr net.Addr
	}

	qtStream struct {
		quic.Stream

		localAddr, remoteAddr net.Addr
	}
)

func (qt *qtStream) LocalAddr() net.Addr {
	return qt.localAddr
}

func (qt *qtStream) RemoteAddr() net.Addr {
	return qt.remoteAddr
}

func (qt *qtTransport) AcceptStream() (net.Conn, error) {
	s, err := qt.qc.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &qtStream{
		Stream:     s,
		localAddr:  qt.localAddr,
		remoteAddr: qt.remoteAddr,
	}, nil
}

func (qt *qtTransport) Close() error {
	return qt.qc.CloseWithError(0, "")
}

func ServeRHP4QUIC(ctx context.Context, l *quic.Listener, s *rhp4.Server, log *zap.Logger) {
	wts := &webtransport.Server{}
	defer wts.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/sia/rhp/v4", func(w http.ResponseWriter, r *http.Request) {
		conn, err := wts.Upgrade(w, r)
		if err != nil {
			log.Debug("webtransport upgrade failed", zap.Error(err))
			return
		}
		defer conn.CloseWithError(0, "")

		err = s.Serve(&wtTransport{
			s:          conn,
			localAddr:  conn.LocalAddr(),
			remoteAddr: conn.RemoteAddr(),
		}, log)
		if err != nil {
			log.Debug("failed to serve connection", zap.Error(err))
		}
	})
	wts.H3.Handler = mux

	for {
		conn, err := l.Accept(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, quic.ErrServerClosed) {
				log.Fatal("failed to accept connection", zap.Error(err))
			}
			return
		}
		log.Debug("accepted connection")
		proto := conn.ConnectionState().TLS.NegotiatedProtocol
		log := log.With(zap.String("peerAddress", conn.RemoteAddr().String()), zap.String("protocol", proto))

		switch proto {
		case "h3":
			go func() {
				defer conn.CloseWithError(0, "")
				wts.ServeQUICConn(conn)
			}()
		case "sia/rhp4":
			go func() {
				defer conn.CloseWithError(0, "")
				s.Serve(&qtTransport{
					qc:         conn,
					remoteAddr: conn.RemoteAddr(),
					localAddr:  conn.LocalAddr(),
				}, log)
			}()
		default:
			conn.CloseWithError(10, "invalid alpn") // define standard error codes
		}
	}
}
