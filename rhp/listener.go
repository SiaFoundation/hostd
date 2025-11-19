package rhp

import (
	"context"
	"net"
	"time"

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
		rl, wl  *rate.Limiter
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
	if err != nil {
		return n, err
	}
	c.rl.WaitN(context.Background(), len(b)) // error can be ignored since context will never be cancelled and len(b) should never exceed burst size
	return n, err
}

// Write writes data to the connection. Write can be made to time out and return
// an error after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *rhpConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.monitor.WriteBytes(n)
	if err != nil {
		return n, err
	}
	c.wl.WaitN(context.Background(), len(b)) // error can be ignored since context will never be cancelled and len(b) should never exceed burst size
	return n, err
}

func (l *rhpListener) Accept() (net.Conn, error) {
	c, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	return &rhpConn{
		Conn:    c,
		rl:      l.readLimiter,
		wl:      l.writeLimiter,
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

type rhpPacketConn struct {
	inner   net.PacketConn
	rl, wl  *rate.Limiter
	monitor DataMonitor
}

// NewRHPPacketConn wraps a net.PacketConn with optional rate limiting and
// monitoring.
func NewRHPPacketConn(conn net.PacketConn, readLimiter, writeLimiter *rate.Limiter, monitor DataMonitor) net.PacketConn {
	return &rhpPacketConn{
		inner:   conn,
		rl:      readLimiter,
		wl:      writeLimiter,
		monitor: monitor,
	}
}

func (c *rhpPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.inner.ReadFrom(b)
	c.monitor.ReadBytes(n)
	if err != nil {
		return n, addr, err
	}
	c.rl.WaitN(context.Background(), len(b))
	return n, addr, err
}

func (c *rhpPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := c.inner.WriteTo(b, addr)
	c.monitor.WriteBytes(n)
	if err != nil {
		return n, err
	}
	c.wl.WaitN(context.Background(), len(b))
	return n, err
}

func (c *rhpPacketConn) Close() error {
	return c.inner.Close()
}

func (c *rhpPacketConn) LocalAddr() net.Addr {
	return c.inner.LocalAddr()
}

func (c *rhpPacketConn) SetDeadline(t time.Time) error {
	return c.inner.SetDeadline(t)
}

func (c *rhpPacketConn) SetReadDeadline(t time.Time) error {
	return c.inner.SetReadDeadline(t)
}

func (c *rhpPacketConn) SetWriteDeadline(t time.Time) error {
	return c.inner.SetWriteDeadline(t)
}
