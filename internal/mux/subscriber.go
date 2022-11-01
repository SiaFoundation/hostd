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

	// A SubscriberMux is a mux that also handles the subscriber and app seed
	// handshakes
	SubscriberMux struct {
		*Mux
	}

	// A SubscriberHandler is a function that handles subscribers.
	SubscriberHandler func(subscriber string, stream *SubscriberStream)
	// A SubscriberRouter routes subscribers to handlers.
	SubscriberRouter struct {
		key   ed25519.PrivateKey
		appID uint64

		mu       sync.RWMutex
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
func (r *SubscriberRouter) route(stream *Stream) {
	// create a new subscriber stream with the read channel unlocked.
	ss := &SubscriberStream{Stream: stream, ch: zeroCh}
	defer ss.Close()

	// read the subscriber request
	subscriber, err := ss.readSubscriberRequest()
	if err != nil {
		ss.setErr(fmt.Errorf("failed to read subscriber: %w", err))
		return
	}
	// get the handler for the subscriber
	r.mu.RLock()
	handler, exists := r.handlers[subscriber]
	r.mu.RUnlock()
	// if the handler doesn't exist, send the unknown subscriber response
	if !exists {
		ss.writeSubscriberResponse(ErrUnknownSubscriber)
		ss.setErr(fmt.Errorf("failed to route stream: %w", ErrUnknownSubscriber))
		return
	}
	// send an empty error response to indicate success
	ss.writeSubscriberResponse(nil)
	handler(subscriber, ss)
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

// readSubscriberRequest reads the subscriber request from the stream.
func (ss *SubscriberStream) readSubscriberRequest() (string, error) {
	subscriberReq, err := readPrefixedBytes(ss.Stream, 1024)
	if err != nil {
		return "", err
	} else if len(subscriberReq) < 8 {
		return "", errors.New("invalid subscriber request")
	}
	// remove the additional length prefix to get the subscriber
	subscriber := string(subscriberReq[8:])
	return subscriber, nil
}

// readSubscriberResponse reads the subscriber response from the stream.
func (ss *SubscriberStream) readSubscriberResponse() error {
	// read the response directly from the stream. The response is a uint64
	// indicating the response's total length, followed by another uint64
	// indicating the message string length, then the actual message string. For
	// success the response message should be empty.
	resp, err := readPrefixedBytes(ss.Stream, 1024)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	} else if len(resp) == 0 {
		return nil
	}

	n := binary.LittleEndian.Uint64(resp[:8])
	if n == 0 {
		return nil
	} else if n > uint64(len(resp[8:])) {
		return fmt.Errorf("invalid response length: %d", n)
	}
	response := string(resp[8 : 8+n])
	switch response {
	case "unknown subscriber":
		err = ErrUnknownSubscriber
	default:
		err = errors.New(response)
	}
	return fmt.Errorf("failed to subscribe: %w", err)
}

func (ss *SubscriberStream) writeSubscriberRequest(subscriber string) {
	// lazy write the subscriber. A length-prefixed, length-prefixed string.
	var buf = make([]byte, 16+len(subscriber))
	binary.LittleEndian.PutUint64(buf, uint64(8+len(subscriber)))
	binary.LittleEndian.PutUint64(buf[8:], uint64(len(subscriber)))
	copy(buf[16:], subscriber)
	ss.lazyWrite(buf)
}

func (ss *SubscriberStream) writeSubscriberResponse(err error) {
	if err == nil {
		// if the error is nil, lazy write the success response
		ss.lazyWrite([]byte{0: 8, 15: 0})
		return
	}

	s := err.Error()
	n := len(s)
	buf := make([]byte, 16+n)
	binary.LittleEndian.PutUint64(buf, uint64(8+n))
	binary.LittleEndian.PutUint64(buf[8:], uint64(n))
	copy(buf[16:], s)
	ss.Write(buf)
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
	ss.writeSubscriberRequest(subscriber)
	// The subscriber handshake must happen asynchronously for compatibility
	// with siad.
	go func() {
		// close the Read() guard after the handshake is complete
		defer close(ss.ch)

		// read the subscriber response
		if err := ss.readSubscriberResponse(); err != nil {
			defer ss.Close()
			ss.setErr(fmt.Errorf("failed to read subscriber response: %w", err))
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

// Upgrade initializes the mux, reciprocates the app seed handshake, and routes
// new streams to the proper subscriber.
func (r *SubscriberRouter) Upgrade(conn net.Conn) error {
	// upgrade to a mux connection
	m, err := Accept(conn, r.key)
	if err != nil {
		return fmt.Errorf("failed to upgrade connection: %w", err)
	} else if err := reciprocateAppSeedHandshake(m, r.appID); err != nil {
		return fmt.Errorf("failed to reciprocate app seed handshake: %w", err)
	}
	for {
		stream, err := m.AcceptStream()
		if errors.Is(err, ErrClosedConn) || errors.Is(err, ErrPeerClosedConn) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to accept stream: %w", err)
		}
		go r.route(stream)
	}
}

func reciprocateAppSeedHandshake(m *Mux, appID uint64) error {
	// first stream handles the app seed handshake
	stream, err := m.AcceptStream()
	if err != nil {
		return fmt.Errorf("failed to accept stream: %w", err)
	}
	defer stream.Close()

	// read the peer seed from the stream
	// note: siamux uses the seed to dedup connections, but
	// we've opted to drop that feature for now.
	if _, err := readPrefixedBytes(stream, 16); err != nil {
		return fmt.Errorf("failed to read peer seed: %w", err)
	}
	// write our app seed to the stream
	resp := make([]byte, 8)
	binary.LittleEndian.PutUint64(resp, appID)
	if err := writePrefixedBytes(stream, resp); err != nil {
		return fmt.Errorf("failed to write app seed: %w", err)
	}
	return nil
}

func initiateAppSeedHandshake(m *Mux, appID uint64) error {
	// first stream handles the app seed handshake
	s := m.NewStream()
	defer s.Close()

	seed := make([]byte, 8)
	binary.LittleEndian.PutUint64(seed, appID)
	if err := writePrefixedBytes(s, seed); err != nil {
		return fmt.Errorf("failed to write app seed: %w", err)
	} else if _, err = readPrefixedBytes(s, 16); err != nil {
		return fmt.Errorf("failed to read peer seed: %w", err)
	}
	return nil
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

func writePrefixedBytes(w io.Writer, buf []byte) error {
	b := make([]byte, 8+len(buf))
	binary.LittleEndian.PutUint64(b, uint64(len(buf)))
	copy(b[8:], buf)
	_, err := w.Write(b)
	return err
}

// DialSubscriber initiates the mux and initiates the app seed handshake on the
// mux.
func DialSubscriber(conn net.Conn, appSeed uint64, theirKey ed25519.PublicKey) (sm *SubscriberMux, err error) {
	m, err := Dial(conn, theirKey)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade: %w", err)
	} else if err := initiateAppSeedHandshake(m, appSeed); err != nil {
		return nil, fmt.Errorf("failed to initiate app seed handshake: %w", err)
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
