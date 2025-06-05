package certificates

import (
	"context"
	"crypto/tls"
	"errors"

	"go.sia.tech/coreutils/rhp/v4/quic"
)

// ErrNotInitialized is returned when the certificate provider has not
// finished initializing.
var ErrNotInitialized = errors.New("certificates not initialized")

// A Manager manages TLS certificates.
type (
	// A Provider provides TLS certificates.
	Provider interface {
		GetCertificate(context.Context) (*tls.Certificate, error)
	}

	quicCertManager struct {
		provider Provider
	}
)

// GetCertificate implements [quic.CertManager].
func (qm *quicCertManager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if qm.provider == nil {
		return nil, ErrNotInitialized
	}
	cert, err := qm.provider.GetCertificate(context.Background())
	if err != nil {
		return nil, err
	}
	if cert == nil {
		return nil, ErrNotInitialized
	}
	return cert, nil
}

// NewQUICCertManager creates a new QUIC certificate manager using the provided
// certificate provider.
func NewQUICCertManager(provider Provider) quic.CertManager {
	return &quicCertManager{
		provider: provider,
	}
}
