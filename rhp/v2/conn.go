package rhp

import (
	"context"
	"net"

	"golang.org/x/time/rate"
)

// An rpcConn wraps a net.Conn to track the amount of data read and written and
// limit bandwidth usage.
type rpcConn struct {
	net.Conn

	r, w uint64

	readLimiter, writeLimiter *rate.Limiter
}

// usage returns the amount of data read and written by the connection.
func (rc *rpcConn) usage() (read, written uint64) {
	return rc.r, rc.w
}

// Read implements io.Reader
func (rc *rpcConn) Read(b []byte) (int, error) {
	n, err := rc.Conn.Read(b)
	rc.r += uint64(n)
	if err := rc.readLimiter.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// Write implements io.Writer
func (rc *rpcConn) Write(b []byte) (int, error) {
	n, err := rc.Conn.Write(b)
	rc.w += uint64(n)
	if err := rc.writeLimiter.WaitN(context.Background(), n); err != nil {
		return n, err
	}
	return n, err
}

// newRPCConn initializes a new RPC conn wrapper.
func newRPCConn(c net.Conn, readLimiter, writeLimiter *rate.Limiter) *rpcConn {
	return &rpcConn{
		Conn:         c,
		readLimiter:  readLimiter,
		writeLimiter: writeLimiter,
	}
}
