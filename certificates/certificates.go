package certificates

import (
	"crypto/tls"
)

// A Manager manages TLS certificates.
type (
	// A Provider provides TLS certificates.
	Provider interface {
		GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
	}
)
