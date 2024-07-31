package rhp

import (
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
)

// A SessionHandlerOption is a functional option for session handlers.
type SessionHandlerOption func(*SessionHandler)

// WithLog sets the logger for the session handler.
func WithLog(l *zap.Logger) SessionHandlerOption {
	return func(s *SessionHandler) {
		s.log = l
	}
}

// WithSessionReporter sets the session reporter for the session handler.
func WithSessionReporter(r SessionReporter) SessionHandlerOption {
	return func(s *SessionHandler) {
		s.sessions = r
	}
}

// WithDataMonitor sets the data monitor for the session handler.
func WithDataMonitor(m rhp.DataMonitor) SessionHandlerOption {
	return func(s *SessionHandler) {
		s.monitor = m
	}
}

type noopSessionReporter struct{}

func (noopSessionReporter) StartSession(conn *rhp.Conn, proto string, version int) (sessionID rhp.UID, end func()) {
	return rhp.UID{}, func() {}
}
func (noopSessionReporter) StartRPC(sessionID rhp.UID, rpc types.Specifier) (rpcID rhp.UID, end func(contracts.Usage, error)) {
	return rhp.UID{}, func(contracts.Usage, error) {}
}
