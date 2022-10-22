package mux

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

type (
	// A SubscriberStream is a stream that subscribes to a specific
	// handler on the peer.
	SubscriberStream struct {
		*Stream

		// The subscriber must be prependend to the first call to Write(), but
		// the response is not sent until future calls to Write() have
		// completed.
		lazyMu  sync.Mutex // guards the lazy write buffer
		lazyBuf []byte

		// ch is used to signal the completion of the subscriber handshake.
		ch chan struct{}
	}

	// A SubscriberMux is a mux that also handles the subscriber handshake
	SubscriberMux struct {
		*Mux
	}

	// A SubscriberHandler is a function that handles subscribers.
	SubscriberHandler func(subscriber string, stream *SubscriberStream)
	// A SubscriberRouter routes subscribers to handlers.
	SubscriberRouter struct {
		key   ed25519.PrivateKey
		appID uint64

		mu       sync.Mutex
		handlers map[string]SubscriberHandler
	}
)

var (
	zeroCh = func() chan struct{} {
		ch := make(chan struct{})
		close(ch)
		return ch
	}()

	ErrUnknownSubscriber = errors.New("unknown subscriber")
)

// route reciprocates the subscriber handshake and routes the stream to the
// appropriate handler.
func (r *SubscriberRouter) route(stream *Stream) error {
	// create a new subscriber stream with the read channel unlocked.
	ss := &SubscriberStream{Stream: stream, ch: zeroCh}

	// read the subscriber directly from the stream. The subscriber is a
	// length-prefixed, length-prefixed string.
	subscriberReq, err := readPrefixedBytes(ss.Stream, 100)
	if err != nil {
		err = ss.setErr(fmt.Errorf("failed to read subscriber: %w", err))
		ss.Close()
		return err
	} else if len(subscriberReq) < 8 {
		// if the subscriber is not registered, send an error response
		_ = writePrefixedBytes(ss, []byte("unknown subscriber"))
		err = ss.setErr(fmt.Errorf("failed to read subscriber: %w", ErrUnknownSubscriber))
		ss.Close()
		return err
	}
	subscriber := string(subscriberReq[8:])
	// get the handler for the subscriber
	r.mu.Lock()
	handler, exists := r.handlers[subscriber]
	r.mu.Unlock()
	if !exists {
		// if the subscriber is not registered, send an error response
		_ = writePrefixedBytes(ss, []byte("unknown subscriber"))
		ss.Close()
		return fmt.Errorf("failed to route stream: %w", ErrUnknownSubscriber)
	}
	// send an empty error response to indicate success
	ss.lazyWrite([]byte{0: 8, 15: 0})
	go func() {
		handler(subscriber, ss)
		ss.Close()
	}()
	return nil
}

// setErr sets the error on the underlying stream and returns the error.
func (ss *SubscriberStream) setErr(err error) error {
	if err == nil {
		return nil
	}
	ss.cond.L.Lock()
	defer ss.cond.L.Unlock()
	if ss.err == nil {
		ss.err = err
	}
	ss.cond.Broadcast()
	return err
}

// lazyWrite appends the bytes to the lazy write buffer. The buffer is written
// to the stream during the next call to Write().
func (ss *SubscriberStream) lazyWrite(p []byte) {
	ss.lazyMu.Lock()
	defer ss.lazyMu.Unlock()
	ss.lazyBuf = append(ss.lazyBuf, p...)
}

// readSubscriberResponse reads the subscriber response from the stream.
func (ss *SubscriberStream) readSubscriberResponse() error {
	// read the response directly from the stream. The response is a uint64
	//indicating the response's total length, followed by another uint64
	// indicating the message string length, then the actual message string.
	// For success the response message should be empty.
	response, err := readPrefixedBytes(ss.Stream, 100)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// if the response is 8 bytes, it is an empty response
	if len(response) != 8 {
		switch s := string(response); s {
		case "unknown subscriber":
			err = ErrUnknownSubscriber
		default:
			err = errors.New(s)
		}
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	return nil
}

// Read implements io.Reader
func (ss *SubscriberStream) Read(p []byte) (int, error) {
	<-ss.ch // block until the subscriber handshake is complete
	n, err := ss.Stream.Read(p)
	return n, err
}

// Write implements io.Writer
func (ss *SubscriberStream) Write(p []byte) (int, error) {
	ss.lazyMu.Lock()
	var m int
	// if there is a lazy write buffer, write it to the stream
	if m = len(ss.lazyBuf); m != 0 {
		p = append(ss.lazyBuf, p...)
		ss.lazyBuf = nil
	}
	ss.lazyMu.Unlock()

	n, err := ss.Stream.Write(p)
	if n >= m {
		n -= m
	}
	return n, err
}

// NewSubscriberStream creates a new Stream that subscribes to the specified
// handler on the peer.
func (sm *SubscriberMux) NewSubscriberStream(subscriber string) (*SubscriberStream, error) {
	// lock the stream to prevent reads until the subscriber handshake is
	// complete.
	ss := &SubscriberStream{Stream: sm.Mux.NewStream(), ch: make(chan struct{})}
	// lazy write the subscriber. A length-prefixed, length-prefixed string.
	var buf = make([]byte, 16+len(subscriber))
	binary.LittleEndian.PutUint64(buf, uint64(8+len(subscriber)))
	binary.LittleEndian.PutUint64(buf[8:], uint64(len(subscriber)))
	copy(buf[16:], subscriber)
	ss.lazyWrite(buf)

	// The subscriber handshake must happen asynchronously for compatibility
	// with siad.
	go func() {
		// close the Read() guard after the handshake is complete
		defer close(ss.ch)

		// read the subscriber response
		if err := ss.readSubscriberResponse(); err != nil {
			ss.setErr(fmt.Errorf("failed to read subscriber response: %w", err))
			ss.Close()
			return
		}
	}()
	return ss, nil
}

// RegisterSubscriber registers a handler for the specified subscriber.
func (r *SubscriberRouter) RegisterSubscriber(subscriber string, fn SubscriberHandler) {
	r.mu.Lock()
	r.handlers[subscriber] = fn
	r.mu.Unlock()
}

// UnregisterSubscriber unregisters the handler for the specified subscriber.
func (r *SubscriberRouter) UnregisterSubscriber(subscriber string) {
	r.mu.Lock()
	delete(r.handlers, subscriber)
	r.mu.Unlock()
}

// Upgrade initializes the mux, reciprocate the app seed handshake, and routes
// new streams to the proper subscriber.
func (r *SubscriberRouter) Upgrade(conn net.Conn) error {
	// upgrade to a mux connection
	m, err := Accept(conn, r.key)
	if err != nil {
		return fmt.Errorf("failed to upgrade connection: %w", err)
	}

	// first stream handles the app seed handshake
	stream, err := m.AcceptStream()
	if err != nil {
		return fmt.Errorf("failed to accept stream: %w", err)
	}
	defer stream.Close()

	// read the peer seed from the stream
	if _, err = readPrefixedBytes(stream, 16); err != nil {
		return fmt.Errorf("failed to read peer seed: %w", err)
	}
	// write our app seed to the stream
	appSeed := make([]byte, 8)
	binary.LittleEndian.PutUint64(appSeed, r.appID)
	if err := writePrefixedBytes(stream, appSeed); err != nil {
		return fmt.Errorf("failed to write app seed: %w", err)
	}

	for {
		stream, err := m.AcceptStream()
		if errors.Is(err, ErrClosedConn) || errors.Is(err, ErrPeerClosedConn) {
			return nil
		}
		r.route(stream)
	}
}

func readPrefixedBytes(r io.Reader, maxLen uint64) ([]byte, error) {
	var n uint64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, fmt.Errorf("failed to read length: %w", err)
	} else if n > maxLen {
		return nil, fmt.Errorf("length exceeds max length: %d > %d", n, maxLen)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	return buf, nil
}

func writePrefixedBytes(w io.Writer, buf []byte) (err error) {
	b := make([]byte, 8+len(buf))
	binary.LittleEndian.PutUint64(b, uint64(len(buf)))
	copy(b[8:], buf)
	_, err = w.Write(b)
	return
}

// DialSubscriber iniates the mux and initiates the app seed handshake on the
// mux.
func DialSubscriber(conn net.Conn, appSeed uint64, theirKey ed25519.PublicKey) (sm *SubscriberMux, err error) {
	m, err := Dial(conn, theirKey)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade: %w", err)
	}

	// first stream handles the app seed handshake
	s := m.NewStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create app seed stream: %w", err)
	}
	defer s.Close()

	seed := make([]byte, 8)
	binary.LittleEndian.PutUint64(seed, appSeed)
	if err := writePrefixedBytes(s, seed); err != nil {
		return nil, fmt.Errorf("failed to write app seed: %w", err)
	} else if _, err = readPrefixedBytes(s, 100); err != nil {
		return nil, fmt.Errorf("failed to read peer seed: %w", err)
	}
	return &SubscriberMux{m}, nil
}

// NewSubscriberRouter creates a new mux router that routes streams to the
// appropriate subscriber handler.
func NewSubscriberRouter(appID uint64, key ed25519.PrivateKey) *SubscriberRouter {
	return &SubscriberRouter{
		appID:    appID,
		key:      key,
		handlers: make(map[string]SubscriberHandler),
	}
}
