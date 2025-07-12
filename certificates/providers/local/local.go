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
	minExpireHours         = 84 * 24 * time.Hour
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

	domain := cert.Leaf.DNSNames[0]
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

// CheckCertificateExpiry checks if the certificate is expiring within 29 days and sends a warning alert.
func (p *localProvider) CheckCertificateExpiry(cert *tls.Certificate, a Alerter, log *zap.Logger) {
	if cert == nil || len(cert.Certificate) == 0 {
		return
	}

	hoursLeft := int(time.Until(cert.Leaf.NotAfter).Hours())
	log.Warn("const", zap.Float64("minExpireHours", minExpireHours.Hours()))
	log.Warn("cert", zap.Int("hoursLeft", hoursLeft))
	log.Warn("check certificate expiry", zap.Duration("remaining", time.Until(cert.Leaf.NotAfter)))
	if hoursLeft > int(minExpireHours) {
		a.DismissCategory(localCertAlertCategory) // Cert isn't expiring. Ensure alerts are clear
		return
	}

	// Alert helpers
	if a == nil {
		return
	}
	a.DismissCategory(localCertAlertCategory) // Clear any previous alerts

	log.Error("certificate check expiry", zap.Duration("remaining", time.Until(cert.Leaf.NotAfter)))

	a.Register(alerts.Alert{
		ID:       alertID,
		Severity: alerts.SeverityWarning,
		Category: localCertAlertCategory,
		Message:  "Locally installed QUIC Certificate expiring soon",
		Data: map[string]any{
			"days remaining": int(time.Until(cert.Leaf.NotAfter).Hours()) / 24,
		},
		Timestamp: time.Now(),
	})
}
