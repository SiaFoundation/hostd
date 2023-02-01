package mux

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"

	"go.sia.tech/mux/v1"
	"lukechampine.com/frand"
)

type (
	// A SubscriberStream is a stream that subscribes to a specific
	// handler on the peer.
	SubscriberStream struct {
		*mux.Stream

		// handshakeComplete is signals the completion of the subscriber
		// handshake.
		handshakeComplete bool
		// handshakeErr should be returned by Read() if the subscriber handshake
		// failed.
		handshakeErr error
	}

	// A SubscriberMux is a mux that also handles the subscriber and app seed
	// handshakes
	SubscriberMux struct {
		*mux.Mux
	}
)

var (
	// ErrUnknownSubscriber is returned when the subscriber is not registered
	// with the router.
	ErrUnknownSubscriber = errors.New("unknown subscriber")
)

func (ss *SubscriberStream) handleSubscriberResponse() {
	// read the subscriber response
	err := readSubscriberResponse(ss.Stream)
	ss.handshakeComplete = true
	ss.handshakeErr = err
}

// Read implements io.Reader.
func (ss *SubscriberStream) Read(p []byte) (int, error) {
	if !ss.handshakeComplete {
		ss.handleSubscriberResponse()
	}
	if ss.handshakeErr != nil {
		return 0, ss.handshakeErr
	}
	n, err := ss.Stream.Read(p)
	return n, err
}

// NewSubscriberStream creates a new Stream that subscribes to the specified
// handler on the peer.
func (sm *SubscriberMux) NewSubscriberStream(subscriber string) (*SubscriberStream, error) {
	// create a new stream with the subscriber handshake marked as incomplete
	ss := &SubscriberStream{Stream: sm.Mux.DialStream()}
	if err := writeSubscriberRequest(ss.Stream, subscriber); err != nil {
		return nil, fmt.Errorf("failed to write subscriber request: %w", err)
	}
	return ss, nil
}

// AcceptSubscriberStream accepts an incoming stream and reciprocates the
// subscriber handshake.
func (sm *SubscriberMux) AcceptSubscriberStream() (*SubscriberStream, string, error) {
	stream, err := sm.AcceptStream()
	if err != nil {
		return nil, "", fmt.Errorf("failed to accept stream: %w", err)
	}

	// read the subscriber request
	subscriber, err := readSubscriberRequest(stream)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read subscriber request: %w", err)
	}
	// since there are no longer registered handlers, write the
	// success response
	if err := writeSubscriberResponse(stream, nil); err != nil {
		return nil, "", fmt.Errorf("failed to write subscriber response: %w", err)
	}
	// create a new subscriber stream with handshake marked as complete
	return &SubscriberStream{Stream: stream, handshakeComplete: true}, subscriber, nil
}

// readSubscriberRequest reads the subscriber request from the stream.
func readSubscriberRequest(stream *mux.Stream) (string, error) {
	subscriberReq, err := readPrefixedBytes(stream, 1024)
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
func readSubscriberResponse(stream *mux.Stream) error {
	// read the response directly from the stream. The response is a uint64
	// indicating the response's total length, followed by another uint64
	// indicating the message string length, then the actual message string. For
	// success the response message should be empty.
	resp, err := readPrefixedBytes(stream, 1024)
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

func writeSubscriberRequest(stream *mux.Stream, subscriber string) error {
	// lazy write the subscriber. A length-prefixed, length-prefixed string.
	var buf = make([]byte, 16+len(subscriber))
	binary.LittleEndian.PutUint64(buf, uint64(8+len(subscriber)))
	binary.LittleEndian.PutUint64(buf[8:], uint64(len(subscriber)))
	copy(buf[16:], subscriber)
	_, err := stream.Write(buf)
	return err
}

func writeSubscriberResponse(stream *mux.Stream, err error) error {
	if err == nil {
		// if the error is nil, lazy write the success response
		_, err = stream.Write([]byte{0: 8, 15: 0})
		return err
	}

	s := err.Error()
	n := len(s)
	buf := make([]byte, 16+n)
	binary.LittleEndian.PutUint64(buf, uint64(8+n))
	binary.LittleEndian.PutUint64(buf[8:], uint64(n))
	copy(buf[16:], s)
	_, err = stream.Write(buf)
	return err
}

func reciprocateAppSeedHandshake(m *mux.Mux, appID uint64) error {
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

func initiateAppSeedHandshake(m *mux.Mux, appID uint64) error {
	// first stream handles the app seed handshake
	s := m.DialStream()
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

// DialSubscriber initiates the mux and initiates the app seed handshake
func DialSubscriber(conn net.Conn, theirKey ed25519.PublicKey) (*SubscriberMux, error) {
	m, err := mux.Dial(conn, theirKey)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade: %w", err)
	} else if err := initiateAppSeedHandshake(m, frand.Uint64n(math.MaxUint64)); err != nil {
		return nil, fmt.Errorf("failed to initiate app seed handshake: %w", err)
	}
	return &SubscriberMux{m}, nil
}

// AcceptSubscriber accepts the mux and reciprocates the app seed handshake
func AcceptSubscriber(conn net.Conn, ourKey ed25519.PrivateKey) (*SubscriberMux, error) {
	// upgrade to a mux connection
	m, err := mux.Accept(conn, ourKey)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade connection: %w", err)
	} else if err := reciprocateAppSeedHandshake(m, frand.Uint64n(math.MaxUint64)); err != nil {
		return nil, fmt.Errorf("failed to reciprocate app seed handshake: %w", err)
	}
	return &SubscriberMux{m}, nil
}
