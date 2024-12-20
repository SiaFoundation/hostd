package rhp

import (
	"crypto/ed25519"
	"errors"
	"net"

	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

// A muxTransport is a rhp4.Transport that wraps a mux.Mux.
type muxTransport struct {
	m *mux.Mux
}

// Close implements the rhp4.Transport interface.
func (mt *muxTransport) Close() error {
	return mt.m.Close()
}

// AcceptStream implements the rhp4.Transport interface.
func (mt *muxTransport) AcceptStream() (net.Conn, error) {
	return mt.m.AcceptStream()
}

// ServeRHP4SiaMux serves RHP4 connections on l using the provided server and logger.
func ServeRHP4SiaMux(l net.Listener, s *rhp4.Server, log *zap.Logger) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Error("failed to accept connection", zap.Error(err))
			}
			return
		}
		log := log.With(zap.String("peerAddress", conn.RemoteAddr().String()))
		go func() {
			defer conn.Close()

			m, err := mux.Accept(conn, ed25519.PrivateKey(s.HostKey()))
			if err != nil {
				log.Debug("failed to accept mux connection", zap.Error(err))
			} else if err := s.Serve(&muxTransport{m}, log); err != nil {
				log.Debug("failed to serve connection", zap.Error(err))
			}
		}()
	}
}
