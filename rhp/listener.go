package rhp

import (
	"net"

	"golang.org/x/time/rate"
)

type noOpMonitor struct{}

func (noOpMonitor) ReadBytes(n int)  {}
func (noOpMonitor) WriteBytes(n int) {}

type (
	// An Option configures a listener.
	Option func(*rhpListener)

	// DataMonitor records the amount of data read and written across
	// all connections.
	DataMonitor interface {
		ReadBytes(int)
		WriteBytes(int)
	}

	rhpConn struct {
		net.Conn

		monitor DataMonitor
	}

	rhpListener struct {
		l net.Listener

		readLimiter  *rate.Limiter
		writeLimiter *rate.Limiter
		monitor      DataMonitor
	}
)

var _ net.Listener = &rhpListener{}
var _ net.Conn = &rhpConn{}

// WithReadLimit sets the read rate limit for the listener.
func WithReadLimit(r *rate.Limiter) Option {
	return func(l *rhpListener) {
		l.readLimiter = r
	}
}

// WithWriteLimit sets the write rate limit for the listener.
func WithWriteLimit(w *rate.Limiter) Option {
	return func(l *rhpListener) {
		l.writeLimiter = w
	}
}

// WithDataMonitor sets the data monitor for the listener.
func WithDataMonitor(m DataMonitor) Option {
	return func(l *rhpListener) {
		l.monitor = m
	}
}

// Read reads data from the connection. Read can be made to time out and return
// an error after a fixed time limit; see SetDeadline and SetReadDeadline.
func (c *rhpConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.monitor.ReadBytes(n)
	return n, err
}

// Write writes data to the connection. Write can be made to time out and return
// an error after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *rhpConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.monitor.WriteBytes(n)
	return n, err
}

func (l *rhpListener) Accept() (net.Conn, error) {
	c, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	return &rhpConn{
		Conn:    c,
		monitor: l.monitor,
	}, nil
}

func (l *rhpListener) Close() error {
	return l.l.Close()
}

func (l *rhpListener) Addr() net.Addr {
	return l.l.Addr()
}

// Listen returns a new listener with optional rate limiting and monitoring.
func Listen(network, address string, opts ...Option) (net.Listener, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	rhp := &rhpListener{
		l:            l,
		readLimiter:  rate.NewLimiter(rate.Inf, 0),
		writeLimiter: rate.NewLimiter(rate.Inf, 0),
		monitor:      noOpMonitor{},
	}
	for _, opt := range opts {
		opt(rhp)
	}
	return rhp, nil
}
