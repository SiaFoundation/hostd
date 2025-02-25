package certificates

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type ManagerOpt func(*Manager)

// WithLocalCert sets the certificate and key files for the Manager.
func WithLocalCert(certFile, keyFile string) ManagerOpt {
	return func(m *Manager) {
		m.certFile = certFile
		m.keyFile = keyFile
	}
}

func WithLog(log *zap.Logger) ManagerOpt {
	return func(m *Manager) {
		m.log = log
	}
}

type Manager struct {
	tg  *threadgroup.ThreadGroup
	log *zap.Logger

	certFile string
	keyFile  string

	mu   sync.Mutex // protects the fields below
	cert tls.Certificate
}

// GetCertificate returns the TLS certificate for the given ClientHelloInfo.
// It implements the tls.Config.GetCertificate method.
func (m *Manager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &m.cert, nil
}

func (m *Manager) refreshCertificate() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.certFile == "" || m.keyFile == "" {
		// TODO: use ACME to get a certificate
		key, err := rsa.GenerateKey(frand.Reader, 2048)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}
		template := x509.Certificate{SerialNumber: big.NewInt(1)}
		certDER, err := x509.CreateCertificate(frand.Reader, &template, &template, &key.PublicKey, key)
		if err != nil {
			return fmt.Errorf("failed to create cert: %w", err)
		}
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return fmt.Errorf("failed to create tls cert: %w", err)
		}
		m.cert = cert
	} else {
		cert, err := tls.LoadX509KeyPair(m.certFile, m.keyFile)
		if err != nil {
			return fmt.Errorf("failed to load certificate and key: %w", err)
		}
		m.cert = cert
	}
	return nil
}

// Close closes the Manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

// NewManager creates a new Manager.
func NewManager(dir string, hostKey types.PrivateKey, opts ...ManagerOpt) (*Manager, error) {
	m := &Manager{
		tg:  threadgroup.New(),
		log: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(m)
	}

	if err := m.refreshCertificate(); err != nil {
		return nil, fmt.Errorf("failed to load certificates: %w", err)
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := m.refreshCertificate(); err != nil {
					m.log.Error("failed to refresh certificates", zap.Error(err))
				}
			}
		}
	}()
	return m, nil
}
