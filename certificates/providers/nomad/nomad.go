package nomad

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/hostd/v2/certificates"
	"go.sia.tech/hostd/v2/internal/nomad"
	"go.uber.org/zap"
)

// A Provider is a certificate provider that issues and manages
// certificates using the Sia Foundation's nomad service.
type Provider struct {
	tg     *threadgroup.ThreadGroup
	log    *zap.Logger
	client *nomad.Client

	dir string

	mu   sync.Mutex
	cert *tls.Certificate
}

var _ certificates.Provider = (*Provider)(nil)

func (p *Provider) issueCertificate() error {
	log := p.log.Named("issueCertificate")
	certKey, certs, err := p.client.IssueCertificate(context.Background())
	if err != nil {
		return fmt.Errorf("failed to issue certificate: %w", err)
	}
	log.Debug("new certificate issued")

	log.Debug("cert times", zap.Time("notBefore", certs[0].NotBefore), zap.Time("notAfter", certs[0].NotAfter))

	if err := os.MkdirAll(p.dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", p.dir, err)
	}

	certFilePath, keyFilePath := filepath.Join(p.dir, "cert.pem"), filepath.Join(p.dir, "key.pem")
	certFile, err := os.OpenFile(certFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open cert file: %w", err)
	}
	defer certFile.Close()

	keyFile, err := os.OpenFile(keyFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open key file: %w", err)
	}
	defer keyFile.Close()

	for _, cert := range certs {
		err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
		if err != nil {
			return fmt.Errorf("failed to write certificate to file: %w", err)
		}
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(certKey)}); err != nil {
		return fmt.Errorf("failed to write key to file: %w", err)
	} else if err := certFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync cert file: %w", err)
	} else if err := keyFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync key file: %w", err)
	}
	log.Debug("certificate and key written to disk", zap.String("certFile", certFilePath), zap.String("keyFile", keyFilePath))
	// load it from disk to ensure the files were written correctly
	cert, err := tls.LoadX509KeyPair(certFilePath, keyFilePath)
	if err != nil {
		return fmt.Errorf("failed to load X509 key pair: %w", err)
	}
	p.mu.Lock()
	p.cert = &cert
	p.mu.Unlock()
	return nil
}

// GetCertificate implements [certificates.Provider].
func (p *Provider) GetCertificate(context.Context) (*tls.Certificate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cert == nil {
		return nil, certificates.ErrNotInitialized
	}
	return p.cert, nil
}

// Close stops the provider and cleans up resources.
func (p *Provider) Close() error {
	p.tg.Stop()
	return nil
}

func loadCertificate(certFile, keyFile, domain string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to load X509 key pair: %w", err)
	} else if cert.Leaf == nil {
		return tls.Certificate{}, fmt.Errorf("certificate is nil or not initialized")
	} else if len(cert.Leaf.DNSNames) == 0 {
		return tls.Certificate{}, fmt.Errorf("certificate has no DNS names")
	} else if strings.ToLower(cert.Leaf.DNSNames[0]) != domain {
		return tls.Certificate{}, fmt.Errorf("certificate DNS name %q does not match expected domain %q", cert.Leaf.DNSNames[0], domain)
	} else if timeToRefresh(&cert) <= 0 {
		return tls.Certificate{}, fmt.Errorf("certificate is ready to refresh: %s", time.Until(cert.Leaf.NotAfter))
	}
	return cert, nil
}

func timeToRefresh(cert *tls.Certificate) time.Duration {
	if cert == nil || cert.Leaf == nil {
		return 0
	}
	duration := cert.Leaf.NotAfter.Sub(cert.Leaf.NotBefore) / 4
	return time.Until(cert.Leaf.NotAfter.Add(-duration))
}

func backoff(failures int) time.Duration {
	if failures == 0 {
		return 0
	}
	// Exponential backoff with a maximum of 6 hours
	return time.Duration(min(6*time.Hour, time.Minute*time.Duration(math.Pow(2, float64(failures)))))
}

// NewProvider creates a new nomad certificate provider.
func NewProvider(dir string, hostKey types.PrivateKey, log *zap.Logger) *Provider {
	domain := nomad.HostDomain(hostKey.PublicKey())
	log = log.With(zap.String("domain", domain))
	provider := &Provider{
		dir: dir,

		log:    log,
		tg:     threadgroup.New(),
		client: nomad.NewClient(hostKey),
	}

	// try to load the certificate from disk
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if cert, err := loadCertificate(certFile, keyFile, domain); err == nil {
		log.Debug("loaded existing certificate from disk", zap.Duration("remaining", time.Until(cert.Leaf.NotAfter)))
		provider.cert = &cert
	}

	go func() {
		ctx, cancel, err := provider.tg.AddContext(context.Background())
		if err != nil {
			return
		}
		defer cancel()

		var consecutiveFailures int
		for {
			provider.mu.Lock()
			var reissueTime time.Duration
			if provider.cert == nil {
				log.Debug("no certificate loaded, issuing a new one")
				reissueTime = backoff(consecutiveFailures)
			} else {
				reissueTime = timeToRefresh(provider.cert)
			}
			provider.mu.Unlock()
			log.Debug("waiting for certificate refresh", zap.Duration("remaining", reissueTime))
			select {
			case <-ctx.Done():
				return
			case <-time.After(reissueTime):
				log.Debug("refreshing certificate", zap.Int("consecutiveFailures", consecutiveFailures))
				if err := provider.issueCertificate(); err != nil {
					log.Error("failed to refresh certificate", zap.Error(err))
					consecutiveFailures++
				} else {
					consecutiveFailures = 0
					log.Debug("certificate refreshed successfully", zap.String("domain", domain))
				}
			}
		}
	}()
	return provider
}

// HostDomain returns the domain for the given host key.
func HostDomain(hostKey types.PublicKey) string {
	return nomad.HostDomain(hostKey)
}
