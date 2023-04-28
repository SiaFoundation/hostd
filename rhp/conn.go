package rhp

import (
	"context"
	"net"

	"golang.org/x/time/rate"
)

type (
	// A DataMonitor records the amount of data read and written across all connections.
	DataMonitor interface {
		ReadBytes(n int)
		WriteBytes(n int)
	}

	// An Conn wraps a net.Conn to track the amount of data read and written and
	// limit bandwidth usage.
	Conn struct {
		net.Conn
		r, w    uint64
		monitor DataMonitor
		rl, wl  *rate.Limiter
	}
)

// Usage returns the amount of data read and written by the connection.
func (c *Conn) Usage() (read, written uint64) {
	return c.r, c.w
}

// Read implements io.Reader
func (c *Conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.r += uint64(n)
	c.monitor.ReadBytes(n)
	if err := c.rl.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// Write implements io.Writer
func (c *Conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.w += uint64(n)
	c.monitor.WriteBytes(n)
	if err := c.wl.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// NewConn initializes a new RPC conn wrapper.
func NewConn(c net.Conn, m DataMonitor, rl, wl *rate.Limiter) *Conn {
	if c, ok := c.(*Conn); ok {
		return c
	}
	return &Conn{
		Conn:    c,
		monitor: m,
		rl:      rl,
		wl:      wl,
	}
}
