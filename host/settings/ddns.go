package settings

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/hostd/internal/ddns"
	"go.sia.tech/hostd/internal/ddns/providers/cloudflare"
	"go.sia.tech/hostd/internal/ddns/providers/duckdns"
	"go.sia.tech/hostd/internal/ddns/providers/noip"
	"go.sia.tech/hostd/internal/ddns/providers/route53"
	"go.uber.org/zap"
)

// defines DNS providers
const (
	DNSProviderCloudflare = "cloudflare"
	DNSProviderDuckDNS    = "duckdns"
	DNSProviderNoIP       = "noip"
	DNSProviderRoute53    = "route53"
)

type (
	// Route53Settings contains the settings for the Route53 DNS provider.
	Route53Settings struct {
		ID     string `json:"id"`
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
	if err := m.UpdateDDNS(false); err != nil {
		m.log.Named("ddns").Error("failed to update ddns", zap.Error(err))
	}
	m.ddnsUpdateTimer.Reset(dnsUpdateFrequency)
}

func (m *ConfigManager) resetDDNS() {
	m.lastIPv4, m.lastIPv6 = nil, nil
	if m.ddnsUpdateTimer != nil {
		// stop the timer if it's running
		if !m.ddnsUpdateTimer.Stop() {
			select {
			case <-m.ddnsUpdateTimer.C:
			default:
			}
		}
	}
	if m.settings.DDNS.Provider == "" {
		return
	} else if m.ddnsUpdateTimer == nil {
		m.ddnsUpdateTimer = time.AfterFunc(0, m.triggerDNSUpdate)
		return
	}
	m.ddnsUpdateTimer.Reset(dnsUpdateFrequency)
}

// UpdateDDNS triggers an update of the host's dynamic DNS records.
func (m *ConfigManager) UpdateDDNS(force bool) error {
	m.mu.Lock()
	hostname, _, err := net.SplitHostPort(m.settings.NetAddress)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("failed to split netaddress host and port: %w", err)
	}
	settings := m.settings.DDNS
	lastIPv4, lastIPv6 := m.lastIPv4, m.lastIPv6
	m.mu.Unlock()

	if force {
		lastIPv4, lastIPv6 = nil, nil
	}

	var ipv4 net.IP
	if settings.IPv4 {
		// get the IPv4 address
		ipv4, err = ddns.GetIPv4()
		if err != nil {
			return fmt.Errorf("failed to get ipv4 address: %w", err)
		} else if ipv4.Equal(lastIPv4) {
			ipv4 = nil
		}
	}
	var ipv6 net.IP
	if settings.IPv6 {
		// get the IPv6 address
		ipv6, err = ddns.GetIPv6()
		if err != nil {
			return fmt.Errorf("failed to get ipv6 address: %w", err)
		} else if ipv6.Equal(lastIPv6) {
			ipv6 = nil
		}
	}

	if ipv4 == nil && ipv6 == nil {
		return nil
	}

	var provider ddns.Provider
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
	if !force {
		m.log.Named("ddns").Info("provider updated", zap.String("hostname", hostname), zap.String("ipv4", ipv4.String()), zap.String("ipv6", ipv6.String()), zap.String("provider", settings.Provider))
	}
	return nil
}

// validateDNSSettings validates the DNS settings and returns an error if they
// are invalid. The Options field will be rewritten to ensure the JSON object
// matches the expected schema.
func validateDNSSettings(s *DNSSettings) error {
	if s.Provider == "" {
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
		case opts.Token == "":
			return errors.New("token must be set")
		case opts.ZoneID == "":
			return errors.New("zone id must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderDuckDNS:
		var opts DuckDNSSettings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal duckdns settings: %w", err)
		} else if opts.Token == "" {
			return errors.New("token must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderNoIP:
		var opts NoIPSettings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal noip settings: %w", err)
		}
		switch {
		case opts.Email == "":
			return errors.New("email must be set")
		case opts.Password == "":
			return errors.New("password must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	case DNSProviderRoute53:
		var opts Route53Settings
		if err := json.Unmarshal(s.Options, &opts); err != nil {
			return fmt.Errorf("failed to unmarshal route53 settings: %w", err)
		}
		switch {
		case opts.ID == "":
			return errors.New("id must be set")
		case opts.Secret == "":
			return errors.New("secret must be set")
		case opts.ZoneID == "":
			return errors.New("zone id must be set")
		}
		s.Options, _ = json.Marshal(opts) // re-encode the options to enforce the correct schema
	default:
		return fmt.Errorf("unknown dns provider: %q", s.Provider)
	}
	return nil
}
