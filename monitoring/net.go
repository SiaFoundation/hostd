package monitoring

import (
	"context"
	"net"

	"golang.org/x/time/rate"
)

type noOpMonitor struct{}

func (noOpMonitor) ReadBytes(n int)  {}
func (noOpMonitor) WriteBytes(n int) {}

type (
	// An Option configures a listener or dialer.
	Option func(*options)

	// DataMonitor records the amount of data read and written across
	// all connections.
	DataMonitor interface {
		ReadBytes(int)
		WriteBytes(int)
	}

	conn struct {
		net.Conn
		rl, wl  *rate.Limiter
		monitor DataMonitor
	}

	listener struct {
		l net.Listener

		readLimiter  *rate.Limiter
		writeLimiter *rate.Limiter
		monitor      DataMonitor
	}
)

var _ net.Listener = &listener{}
var _ net.Conn = &conn{}

type options struct {
	readLimiter  *rate.Limiter
	writeLimiter *rate.Limiter
	monitor      DataMonitor
}

// WithReadLimit sets the read rate limit for the listener.
func WithReadLimit(r *rate.Limiter) Option {
	return func(o *options) {
		o.readLimiter = r
	}
}

// WithWriteLimit sets the write rate limit for the listener.
func WithWriteLimit(w *rate.Limiter) Option {
	return func(o *options) {
		o.writeLimiter = w
	}
}

// WithDataMonitor sets the data monitor for the listener.
func WithDataMonitor(m DataMonitor) Option {
	return func(o *options) {
		o.monitor = m
	}
}

// Read reads data from the connection. Read can be made to time out and return
// an error after a fixed time limit; see SetDeadline and SetReadDeadline.
func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.monitor.ReadBytes(n)
	if err != nil {
		return n, err
	}
	c.rl.WaitN(context.Background(), n) // error can be ignored since context will never be cancelled and n should never exceed burst size
	return n, err
}

// Write writes data to the connection. Write can be made to time out and return
// an error after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.monitor.WriteBytes(n)
	if err != nil {
		return n, err
	}
	c.wl.WaitN(context.Background(), n) // error can be ignored since context will never be cancelled and n should never exceed burst size
	return n, err
}

func (l *listener) Accept() (net.Conn, error) {
	c, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	return &conn{
		Conn:    c,
		rl:      l.readLimiter,
		wl:      l.writeLimiter,
		monitor: l.monitor,
	}, nil
}

func (l *listener) Close() error {
	return l.l.Close()
}

func (l *listener) Addr() net.Addr {
	return l.l.Addr()
}

func defaultOptions() *options {
	return &options{
		readLimiter:  rate.NewLimiter(rate.Inf, 0),
		writeLimiter: rate.NewLimiter(rate.Inf, 0),
		monitor:      noOpMonitor{},
	}
}

// Listen returns a new listener with optional rate limiting and monitoring.
func Listen(network, address string, opts ...Option) (net.Listener, error) {
	l, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	listener := &listener{
		l:            l,
		readLimiter:  options.readLimiter,
		writeLimiter: options.writeLimiter,
		monitor:      options.monitor,
	}
	return listener, nil
}

// Dialer is a net.Dialer with optional rate limiting and monitoring.
type Dialer struct {
	readLimiter  *rate.Limiter
	writeLimiter *rate.Limiter
	monitor      DataMonitor
}

// NewDialer returns a new Dialer with optional rate limiting and monitoring.
func NewDialer(opts ...Option) *Dialer {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return &Dialer{
		readLimiter:  options.readLimiter,
		writeLimiter: options.writeLimiter,
		monitor:      options.monitor,
	}
}

// DialContext dials a new, optionally rate limited and monitored connection.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &conn{
		Conn:    c,
		rl:      d.readLimiter,
		wl:      d.writeLimiter,
		monitor: d.monitor,
	}, nil
}
