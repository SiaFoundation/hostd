package local

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/certificates"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type localProvider struct {
	certFile string
	keyFile  string

	mu       sync.Mutex
	lastLoad time.Time
	cert     *tls.Certificate

	alerter Alerter
	log     *zap.Logger
}

const (
	localCertAlertCategory = "localCertificate"
)

// An Alerter is an interface for registering alerts.
type Alerter interface {
	Register(a alerts.Alert)
	DismissCategory(category string)
}

var alertID = frand.Entropy256()

// GetCertificate implements [certificates.Provider].
func (p *localProvider) GetCertificate(ctx context.Context) (*tls.Certificate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cert == nil || time.Since(p.lastLoad) > 5*time.Minute {
		cert, err := tls.LoadX509KeyPair(p.certFile, p.keyFile)
		if err != nil {
			return nil, err
		}

		p.cert = &cert
		p.lastLoad = time.Now()
	}

	if p.cert.Leaf == nil {
		leaf, err := x509.ParseCertificate(p.cert.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate leaf: %w", err)
		}
		p.cert.Leaf = leaf
	}

	if err := CheckCertificateExpiry(p.cert, p.alerter, p.log); err != nil {
		return nil, err
	}
	return p.cert, nil
}

// NewProvider creates a new local certificate provider.
func NewProvider(certFile, keyFile string, alerter Alerter, log *zap.Logger) (certificates.Provider, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}

	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0]) // Required to load certificate details
	if err != nil {
		return nil, err
	}

	return &localProvider{
		certFile: certFile,
		keyFile:  keyFile,

		cert:     &cert,
		lastLoad: time.Now(),

		alerter: alerter,
		log:     log,
	}, nil
}

// CheckCertificateExpiry checks if the certificate is expiring within 90 days and sends a warning alert.
func CheckCertificateExpiry(cert *tls.Certificate, a Alerter, log *zap.Logger) error {
	if cert == nil || len(cert.Certificate) == 0 {
		return nil
	}

	daysLeft := int(time.Until(cert.Leaf.NotAfter).Hours() / 24)
	logger := log.Named("localCertProvider")
	logger.Debug(fmt.Sprintf("Locally installed QUIC certificate expires in %d days", daysLeft))
	if daysLeft > 29 {
		a.DismissCategory(localCertAlertCategory) // Cert expires more than 29 days in the future.  Remove any previously registered alerts
		return nil
	}

	// Alert helpers
	if a == nil {
		return nil
	}
	a.DismissCategory(localCertAlertCategory) // Clear any previous alerts

	logger.Warn(fmt.Sprintf("Locally installed QUIC certificate expires in %d days (on %s)", daysLeft, cert.Leaf.NotAfter.Format("2006-01-02")))

	a.Register(alerts.Alert{
		ID:        alertID,
		Severity:  alerts.SeverityWarning,
		Category:  localCertAlertCategory,
		Message:   fmt.Sprintf("Locally installed QUIC Certificate expires in %d days (on %s)", daysLeft, cert.Leaf.NotAfter.Format("2006-01-02")),
		Data:      nil,
		Timestamp: time.Now(),
	})
	return nil
}
