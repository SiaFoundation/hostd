package rhp

import (
	"fmt"
	"io"
	"net"
	"time"
)

const minMessageSize = 1024

// An rpcSession keeps track of the state of a single RPC.
type rpcSession struct {
	stream net.Conn

	enc *encoder
}

func writeResponse(enc *encoder, obj rpcObject) {
	resp := &rpcResponse{nil, obj}
	enc.WritePrefix(resp.encodedLen())
	resp.encodeTo(enc)
}

func readResponse(dec *decoder, obj rpcObject, maxLen int64) error {
	length := dec.ReadPrefix()
	if err := dec.Err(); err != nil {
		return fmt.Errorf("failed to read length prefix: %w", err)
	} else if int64(length) > maxLen {
		return fmt.Errorf("length exceeds max length: %d > %d", length, maxLen)
	}

	resp := &rpcResponse{nil, obj}
	if err := resp.decodeFrom(dec); err != nil {
		return err
	} else if resp.err != nil {
		return fmt.Errorf("remote error: %w", resp.err)
	}
	return nil
}

// WriteObjects writes an object to the stream
func (s *rpcSession) WriteObjects(objs ...rpcObject) error {
	for _, obj := range objs {
		writeResponse(s.enc, obj)
	}
	return s.enc.Flush()
}

// WriteError writes an error to the stream
func (s *rpcSession) WriteError(err error) error {
	re, ok := err.(*rpcError)
	if err != nil && !ok {
		re = &rpcError{Description: err.Error()}
	}

	s.stream.SetWriteDeadline(time.Now().Add(30 * time.Second))
	enc := newEncoder(s.stream)
	resp := &rpcResponse{re, nil}
	enc.WritePrefix(resp.encodedLen())
	resp.encodeTo(enc)
	enc.Flush()
	return err
}

// ReadObject reads an object from the stream
func (s *rpcSession) ReadObject(obj rpcObject, maxLen int64, timeout time.Duration) error {
	if maxLen < minMessageSize {
		maxLen = minMessageSize
	}
	dec := newDecoder(io.LimitedReader{R: s.stream, N: maxLen})
	return readResponse(dec, obj, maxLen)
}
