package local

import (
	"context"
	"crypto/tls"
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
	minExpireTime          = 29 * 24 * time.Hour // 29 Days
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
		p.CheckCertificateExpiry(p.cert, p.alerter, p.log)
	}

	return p.cert, nil
}

// NewProvider creates a new local certificate provider.
func NewProvider(certFile, keyFile string, alerter Alerter, log *zap.Logger) (certificates.Provider, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}

	// Attempt to log with certificate domain
	domain := "unknown"
	if len(cert.Leaf.DNSNames) > 0 {
		domain = cert.Leaf.DNSNames[0]
	} else if cert.Leaf.Subject.CommonName != "" {
		domain = cert.Leaf.Subject.CommonName
	}

	log = log.With(zap.String("certFile", certFile), zap.String("domain", domain))

	p := &localProvider{
		certFile: certFile,
		keyFile:  keyFile,

		cert:     &cert,
		lastLoad: time.Now(),

		alerter: alerter,
		log:     log,
	}

	// CheckCertificateExpiry during startup
	p.CheckCertificateExpiry(p.cert, p.alerter, p.log)

	return p, nil
}

// CheckCertificateExpiry checks certificate NotAfter and sends a warning or error alert.
func (p *localProvider) CheckCertificateExpiry(cert *tls.Certificate, a Alerter, log *zap.Logger) {
	if cert == nil || len(cert.Certificate) == 0 {
		return
	}

	// Alert helper
	if a == nil {
		return
	}
	a.DismissCategory(localCertAlertCategory) // Clear any previous alerts

	hoursLeft := int(time.Until(cert.Leaf.NotAfter).Hours())

	// Compare by hours, warning if under minExpireTime and error if negative
	switch {
	case hoursLeft <= 0:
		log.Error("local certificate check expired",
			zap.Time("Not Valid After", cert.Leaf.NotAfter))

		a.Register(alerts.Alert{
			ID:       alertID,
			Severity: alerts.SeverityError,
			Category: localCertAlertCategory,
			Message:  "Local QUIC Certificate has expired",
			Data: map[string]any{
				"CertFile":  p.certFile,
				"NotAfter":  cert.Leaf.NotAfter,
				"NotBefore": cert.Leaf.NotBefore,
			},
			Timestamp: time.Now(),
		})
	case hoursLeft < int(minExpireTime.Hours()):
		log.Warn("local certificate check expiring soon",
			zap.Time("Not Valid After", cert.Leaf.NotAfter))

		a.Register(alerts.Alert{
			ID:       alertID,
			Severity: alerts.SeverityWarning,
			Category: localCertAlertCategory,
			Message:  "Local QUIC Certificate expires under 30 days",
			Data: map[string]any{
				"CertFile":  p.certFile,
				"NotAfter":  cert.Leaf.NotAfter,
				"NotBefore": cert.Leaf.NotBefore,
			},
			Timestamp: time.Now(),
		})
	default:
		log.Debug("local certificate check", zap.Duration("remaining", time.Until(cert.Leaf.NotAfter).Truncate(time.Second)))
		a.DismissCategory(localCertAlertCategory) // Cert isn't expiring. Ensure alerts are clear
		return
	}
}
