package selfsigned

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"

	"go.sia.tech/hostd/v2/certificates"
	"lukechampine.com/frand"
)

type selfSignedProvider struct {
	cert *tls.Certificate
}

// GetCertificate implements [certificates.Provider].
func (p *selfSignedProvider) GetCertificate(ctx context.Context) (*tls.Certificate, error) {
	return p.cert, nil
}

// New creates a new self-signed certificate provider used for testing or local development.
func New() (certificates.Provider, error) {
	key, err := rsa.GenerateKey(frand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		DNSNames: []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(frand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cert: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls cert: %w", err)
	}

	return &selfSignedProvider{
		cert: &cert,
	}, nil
}
