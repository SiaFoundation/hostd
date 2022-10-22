package mux

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"gitlab.com/NebulousLabs/log"
	"gitlab.com/NebulousLabs/siamux"
	"gitlab.com/NebulousLabs/siamux/mux"
	"lukechampine.com/frand"
)

func startEchoSubscriber(l net.Listener, priv ed25519.PrivateKey) {
	router := NewSubscriberRouter(8000, priv)

	// simple echo handler
	router.RegisterSubscriber("echo", func(subscriber string, stream *SubscriberStream) {
		buf, err := readPrefixedBytes(stream, 1024)
		if err != nil {
			panic(fmt.Errorf("failed to read object from stream: %w", err))
		} else if err := writePrefixedBytes(stream, buf); err != nil {
			panic(fmt.Errorf("failed to write object to stream: %w", err))
		}
	})

	// spawn a goroutine to accept connections and upgrade them
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			if err := router.Upgrade(conn); err != nil {
				continue
			}
		}
	}()
}

func TestSubscriberRouter(t *testing.T) {
	serverKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("failed to listen:", err)
	}
	defer listener.Close()
	startEchoSubscriber(listener, serverKey)

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal("failed to dial:", err)
	}
	defer conn.Close()

	m, err := DialSubscriber(conn, 8000, serverKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal("failed to dial subscriber:", err)
	}
	defer m.Close()

	t.Run("test bad subscriber", func(t *testing.T) {
		// Due to the laziness expected by siad, we cannot detect the unknown
		// subscriber error as part of the handshake. We have to first write
		// to then read from the stream.
		s, err := m.NewSubscriberStream("bad subscriber")
		if err != nil {
			t.Fatal("failed to initiate stream:", err)
		}
		defer s.Close()

		if err := writePrefixedBytes(s, []byte("hello")); err != nil {
			t.Fatal("failed to write to stream:", err)
		} else if _, err := readPrefixedBytes(s, 1024); !errors.Is(err, ErrUnknownSubscriber) {
			t.Fatal("expected subscriber error, not", err)
		}
	})

	t.Run("test good subscriber", func(t *testing.T) {
		s, err := m.NewSubscriberStream("echo")
		if err != nil {
			t.Fatal("failed to create subscriber stream:", err)
		}
		defer s.Close()

		req := frand.Bytes(128)
		if err := writePrefixedBytes(s, req); err != nil {
			t.Fatal("failed to write object to stream:", err)
		} else if resp, err := readPrefixedBytes(s, 1024); err != nil {
			t.Fatal("failed to read subscriber reply:", err)
		} else if !bytes.Equal(req, resp) {
			t.Fatalf("unexpected reply: got %v expected %v", resp, req)
		}
	})
}

func TestSubscriberRouterCompat(t *testing.T) {
	dir := t.TempDir()

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("failed to create listener:", err)
	}
	defer l.Close()

	serverKey := ed25519.NewKeyFromSeed(frand.Bytes(32))
	startEchoSubscriber(l, serverKey)

	m, err := siamux.New(":0", ":0", log.DiscardLogger, filepath.Join(dir, "siamux"))
	if err != nil {
		t.Fatal("failed to create sia mux:", err)
	}
	defer m.Close()

	var key mux.ED25519PublicKey
	copy(key[:], serverKey.Public().(ed25519.PublicKey)[:])

	t.Run("bad subscriber", func(t *testing.T) {
		s, err := m.NewStream("bad sub", l.Addr().String(), key)
		if err != nil {
			t.Fatal("failed to create subscriber stream:", err)
		}

		if err := writePrefixedBytes(s, []byte("hello")); err != nil {
			t.Fatal("failed to write to stream:", err)
		} else if _, err := readPrefixedBytes(s, 1024); err == nil {
			t.Fatal("expected subscriber error:", err)
		}
	})

	t.Run("echo subscriber", func(t *testing.T) {
		s, err := m.NewStream("echo", l.Addr().String(), key)
		if err != nil {
			t.Fatal("failed to create subscriber stream:", err)
		}

		req := frand.Bytes(128)
		if err := writePrefixedBytes(s, req); err != nil {
			t.Fatal("failed to write object to stream:", err)
		} else if resp, err := readPrefixedBytes(s, 1024); err != nil {
			t.Fatal("failed to read subscriber reply:", err)
		} else if !bytes.Equal(req, resp) {
			t.Fatalf("unexpected reply: got %v expected %v", resp, req)
		}
	})
}

func TestSubscriberMuxCompat(t *testing.T) {
	dir := t.TempDir()

	serverMux, err := siamux.New(":0", ":0", log.DiscardLogger, filepath.Join(dir, "siamux"))
	if err != nil {
		t.Fatal("failed to create sia mux:", err)
	}
	defer serverMux.Close()

	serverMux.NewListener("echo", func(stream siamux.Stream) {
		if req, err := readPrefixedBytes(stream, 1024); err != nil {
			panic(fmt.Errorf("failed to read object from stream: %w", err))
		} else if err := writePrefixedBytes(stream, req); err != nil {
			panic(fmt.Errorf("failed to write object to stream: %w", err))
		}
	})

	conn, err := net.Dial("tcp", serverMux.Address().String())
	if err != nil {
		t.Fatal("failed to dial sia mux:", err)
	}
	defer conn.Close()

	serverKey := serverMux.PublicKey()
	m, err := DialSubscriber(conn, 5751, serverKey[:])
	if err != nil {
		t.Fatal("failed to dial subscriber mux:", err)
	}

	t.Run("bad subscriber", func(t *testing.T) {
		s, err := m.NewSubscriberStream("bad sub")
		if err != nil {
			t.Fatal("failed to create subscriber stream:", err)
		}

		if err := writePrefixedBytes(s, []byte("hello")); err != nil {
			t.Fatal("failed to write to stream:", err)
		} else if _, err := readPrefixedBytes(s, 1024); err == nil {
			t.Fatal("expected subscriber error:", err)
		}
	})

	t.Run("echo subscriber", func(t *testing.T) {
		s, err := m.NewSubscriberStream("echo")
		if err != nil {
			t.Fatal("failed to create subscriber stream:", err)
		}

		req := frand.Bytes(128)
		if err := writePrefixedBytes(s, req); err != nil {
			t.Fatal("failed to write object to stream:", err)
		} else if resp, err := readPrefixedBytes(s, 1024); err != nil {
			t.Fatal("failed to read subscriber reply:", err)
		} else if !bytes.Equal(req, resp) {
			t.Fatalf("unexpected reply: got %v expected %v", resp, req)
		}
	})

}
