package rhp

import (
	"context"
	"net"

	"golang.org/x/time/rate"
)

// An Conn wraps a net.Conn to track the amount of data read and written and
// limit bandwidth usage.
type Conn struct {
	net.Conn
	r, w   uint64
	rl, wl *rate.Limiter
}

// Usage returns the amount of data read and written by the connection.
func (c *Conn) Usage() (read, written uint64) {
	return c.r, c.w
}

// Read implements io.Reader
func (c *Conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.r += uint64(n)
	if err := c.rl.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// Write implements io.Writer
func (c *Conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.w += uint64(n)
	if err := c.wl.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// NewConn initializes a new RPC conn wrapper.
func NewConn(c net.Conn, rl, wl *rate.Limiter) *Conn {
	return &Conn{
		Conn: c,
		rl:   rl,
		wl:   wl,
	}
}
