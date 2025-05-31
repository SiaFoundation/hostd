package nomad

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/internal/ddns"
	"go.sia.tech/hostd/v2/internal/nomad"
)

// Provider implements the DNS provider interface for nomad.
type Provider struct {
	hostKey types.PrivateKey
}

var client = &http.Client{
	Timeout: 10 * time.Second,
}

// Update implements the ddns.Provider interface for nomad.
func (p *Provider) Update(ipv4, ipv6 net.IP) error {
	if ipv4 == nil && ipv6 == nil {
		return errors.New("no IP addresses provided")
	}

	_, err := (&nomad.Client{}).UpdateDNS(context.Background(), nomad.UpdateDNSRequest{
		IPv4: ipv4.String(),
		IPv6: ipv6.String(),
	}, p.hostKey)
	return err
}

// New creates a new nomad DNS provider with the given host key.
func New(hostKey types.PrivateKey) ddns.Provider {
	return &Provider{
		hostKey: hostKey,
	}
}
