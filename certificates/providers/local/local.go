package local

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/hostd/v2/certificates"
)

type localProvider struct {
	certFile string
	keyFile  string

	mu       sync.Mutex
	lastLoad time.Time
	cert     *tls.Certificate
}

// GetCertificate implements [certificates.Provider].
func (p *localProvider) GetCertificate(ctx context.Context) (*tls.Certificate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cert == nil {
		return nil, certificates.ErrNotInitialized
	}

	if p.cert == nil || time.Since(p.lastLoad) > 5*time.Minute {
		cert, err := tls.LoadX509KeyPair(p.certFile, p.keyFile)
		if err != nil {
			return nil, err
		}

		p.cert = &cert
		p.lastLoad = time.Now()
	}
	return p.cert, nil
}

// NewProvider creates a new local certificate provider.
func NewProvider(certFile, keyFile string) (certificates.Provider, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}
	return &localProvider{
		certFile: certFile,
		keyFile:  keyFile,

		cert:     &cert,
		lastLoad: time.Now(),
	}, nil
}
