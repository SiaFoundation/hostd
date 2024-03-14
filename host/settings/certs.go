package settings

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

func (m *ConfigManager) reloadCertificates() error {
	if _, err := os.Stat(m.certKeyFilePath); errors.Is(err, os.ErrNotExist) {
		// if the certificate files do not exist, create a temporary certificate
		addr := m.settings.NetAddress
		if len(addr) == 0 {
			addr = m.discoveredRHPAddr
		}
		addr, _, err := net.SplitHostPort(addr)
		if err != nil {
			addr = "localhost"
		}

		certificate, err := tempCertificate(addr)
		if err != nil {
			return fmt.Errorf("failed to create temporary certificate: %w", err)
		}
		m.rhp3WSTLS.Certificates = []tls.Certificate{certificate}
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check for certificate: %w", err)
	}

	// load the certificate from disk
	certificate, err := tls.LoadX509KeyPair(m.certCertFilePath, m.certKeyFilePath)
	if err != nil {
		return fmt.Errorf("failed to load certificate: %w", err)
	}
	m.rhp3WSTLS.Certificates = []tls.Certificate{certificate}
	return nil
}

// RHP3TLSConfig returns the TLS config for the rhp3 WebSocket listener
func (m *ConfigManager) RHP3TLSConfig() *tls.Config {
	return m.rhp3WSTLS
}

func tempCertificate(name string) (tls.Certificate, error) {
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.Unix()),
		Subject: pkix.Name{
			CommonName:   name,
			Organization: []string{name},
		},
		NotBefore:             now,
		NotAfter:              now.AddDate(1, 0, 0), // valid for one year
		BasicConstraintsValid: true,
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate private key: %w", err)
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, priv.Public(), priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create certificate: %w", err)
	}

	var outCert tls.Certificate
	outCert.Certificate = append(outCert.Certificate, cert)
	outCert.PrivateKey = priv
	return outCert, nil
}
