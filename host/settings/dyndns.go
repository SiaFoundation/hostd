package settings

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/hostd/internal/dyndns"
	"go.sia.tech/hostd/internal/dyndns/providers/cloudflare"
	"go.sia.tech/hostd/internal/dyndns/providers/duckdns"
	"go.sia.tech/hostd/internal/dyndns/providers/noip"
	"go.sia.tech/hostd/internal/dyndns/providers/route53"
	"go.uber.org/zap"
)

const (
	DNSProviderCloudflare = "cloudflare"
	DNSProviderDuckDNS    = "duckdns"
	DNSProviderNoIP       = "noip"
	DNSProviderRoute53    = "route53"
)

type (
	// Route53Settings contains the settings for the Route53 DNS provider.
	Route53Settings struct {
		ID     string `json:"ID"`
		Secret string `json:"secret"`
		ZoneID string `json:"zoneID"`
	}

	// NoIPSettings contains the settings for the No-IP DNS provider.
	NoIPSettings struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	// DuckDNSSettings contains the settings for the DuckDNS DNS provider.
	DuckDNSSettings struct {
		Token string `json:"token"`
	}

	// CloudflareSettings contains the settings for the Cloudflare DNS provider.
	CloudflareSettings struct {
		Token  string `json:"token"`
		ZoneID string `json:"zoneID"`
	}

	// DNSSettings contains the settings for the host's dynamic DNS.
	DNSSettings struct {
		Provider string          `json:"provider"`
		IPv4     bool            `json:"ipv4"`
		IPv6     bool            `json:"ipv6"`
		Options  json.RawMessage `json:"options"`
	}
)

func (m *ConfigManager) triggerDNSUpdate() {
	if err := m.UpdateDynDNS(false); err != nil {
		m.log.Named("dyndns").Error("failed to update dyndns", zap.Error(err))
	}
	m.dyndnsUpdateTimer.Reset(dnsUpdateFrequency)
}

func (m *ConfigManager) resetDynDNS() {
	m.lastIPv4, m.lastIPv6 = nil, nil
	if m.dyndnsUpdateTimer != nil {
		// stop the timer if it's running
		if !m.dyndnsUpdateTimer.Stop() {
			select {
			case <-m.dyndnsUpdateTimer.C:
			default:
			}
		}
	}
	if len(m.settings.DynDNS.Provider) == 0 {
		return
	} else if m.dyndnsUpdateTimer == nil {
		m.dyndnsUpdateTimer = time.AfterFunc(0, m.triggerDNSUpdate)
		return
	}
	m.dyndnsUpdateTimer.Reset(dnsUpdateFrequency)
}

// UpdateDynDNS triggers an update of the host's dynamic DNS records.
func (m *ConfigManager) UpdateDynDNS(force bool) error {
	m.mu.Lock()
	hostname, _, err := net.SplitHostPort(m.settings.NetAddress)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("failed to split netaddress host and port: %w", err)
	}
	settings := m.settings.DynDNS
	lastIPv4, lastIPv6 := m.lastIPv4, m.lastIPv6
	m.mu.Unlock()

	if force {
		lastIPv4, lastIPv6 = nil, nil
	}

	var ipv4 net.IP
	if settings.IPv4 {
		// get the IPv4 address
		ipv4, err = dyndns.GetIPv4()
		if err != nil {
			return fmt.Errorf("failed to get ipv4 address: %w", err)
		} else if ipv4.Equal(lastIPv4) {
			ipv4 = nil
		}
	}
	var ipv6 net.IP
	if settings.IPv6 {
		// get the IPv6 address
		ipv6, err = dyndns.GetIPv6()
		if err != nil {
			return fmt.Errorf("failed to get ipv6 address: %w", err)
		} else if ipv6.Equal(lastIPv6) {
			ipv6 = nil
		}
	}

	if ipv4 == nil && ipv6 == nil {
		return nil
	}

	var provider dyndns.Provider
	switch settings.Provider {
	case DNSProviderCloudflare:
		var options CloudflareSettings
		if err := json.Unmarshal(settings.Options, &options); err != nil {
			return fmt.Errorf("failed to parse cloudflare options: %w", err)
		}
		provider = cloudflare.New(cloudflare.Options{
			Token:    options.Token,
			ZoneID:   options.ZoneID,
			Hostname: hostname,
		})
	case DNSProviderDuckDNS:
		var options DuckDNSSettings
		if err := json.Unmarshal(settings.Options, &options); err != nil {
			return fmt.Errorf("failed to parse duckdns options: %w", err)
		}
		provider = duckdns.New(duckdns.Options{
			Token:    options.Token,
			Hostname: hostname,
		})
	case DNSProviderNoIP:
		var options NoIPSettings
		if err := json.Unmarshal(settings.Options, &options); err != nil {
			return fmt.Errorf("failed to parse noip options: %w", err)
		}
		provider = noip.New(noip.Options{
			Email:    options.Email,
			Password: options.Password,
			Hostname: hostname,
		})
	case DNSProviderRoute53:
		var options Route53Settings
		if err := json.Unmarshal(settings.Options, &options); err != nil {
			return fmt.Errorf("failed to parse route53 options: %w", err)
		}
		provider = route53.New(route53.Options{
			ID:       options.ID,
			Secret:   options.Secret,
			ZoneID:   options.ZoneID,
			Hostname: hostname,
		})
	default:
		return fmt.Errorf("unknown dns provider: %q", settings.Provider)
	}

	// update the DNS provider
	if err := provider.Update(ipv4, ipv6); err != nil {
		return fmt.Errorf("failed to update dns: %w", err)
	}
	m.mu.Lock()
	m.lastIPv4, m.lastIPv6 = ipv4, ipv6
	m.mu.Unlock()
	m.log.Named("dyndns").Info("updated dyndns", zap.String("hostname", hostname), zap.String("ipv4", ipv4.String()), zap.String("ipv6", ipv6.String()), zap.String("provider", settings.Provider))
	return nil
}

// validateDNSSettings validates the DNS settings and returns an error if they
// are invalid. The Options field will be rewritten to ensure the JSON object
// matches the expected schema.
func validateDNSSettings(s *DNSSettings) error {
	if len(s.Provider) == 0 {
		// clear DNS settings if provider is empty
		s.IPv4 = false
		s.IPv6 = false
		s.Options = nil
		return nil
	} else if !s.IPv4 && !s.IPv6 {
		return errors.New("at least one of IPv4 or IPv6 must be enabled")
	}

	switch s.Provider {
	case DNSProviderCloudflare:
		var opts CloudflareSettings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal cloudflare settings: %w", err)
		}
		switch {
		case len(opts.Token) == 0:
			return errors.New("token must be set")
		case len(opts.ZoneID) == 0:
			return errors.New("zone id must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderDuckDNS:
		var opts DuckDNSSettings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal duckdns settings: %w", err)
		} else if len(opts.Token) == 0 {
			return errors.New("token must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderNoIP:
		var opts NoIPSettings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal noip settings: %w", err)
		}
		switch {
		case len(opts.Email) == 0:
			return errors.New("email must be set")
		case len(opts.Password) == 0:
			return errors.New("password must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderRoute53:
		var opts Route53Settings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal route53 settings: %w", err)
		}
		switch {
		case len(opts.ID) == 0:
			return errors.New("id must be set")
		case len(opts.Secret) == 0:
			return errors.New("secret must be set")
		case len(opts.ZoneID) == 0:
			return errors.New("zone id must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	default:
		return fmt.Errorf("unknown dns provider: %q", s.Provider)
	}
	return nil
}
