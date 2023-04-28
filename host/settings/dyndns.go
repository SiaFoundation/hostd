package settings

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/hostd/internal/dyndns"
	"go.sia.tech/hostd/internal/dyndns/providers/duckdns"
	"go.sia.tech/hostd/internal/dyndns/providers/noip"
	"go.sia.tech/hostd/internal/dyndns/providers/route53"
	"go.uber.org/zap"
)

type (
	// DNSSettings contains the settings for the host's dynamic DNS.
	DNSSettings struct {
		Provider string         `json:"provider"`
		IPv4     bool           `json:"ipv4"`
		IPv6     bool           `json:"ipv6"`
		Options  map[string]any `json:"options"`
	}
)

func (m *ConfigManager) triggerDNSUpdate() {
	if err := m.UpdateDynDNS(false); err != nil {
		m.log.Named("dyndns").Error("failed to update dyndns", zap.Error(err))
		return
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
	settings := m.settings.DynDNS
	lastIPv4, lastIPv6 := m.lastIPv4, m.lastIPv6
	m.mu.Unlock()

	if force {
		lastIPv4, lastIPv6 = nil, nil
	}

	var err error
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

	optsBuf, err := json.Marshal(settings.Options)
	if err != nil {
		return fmt.Errorf("failed to marshal dns settings: %w", err)
	}

	var provider dyndns.Provider
	switch settings.Provider {
	case "duckdns":
		var options duckdns.Options
		if err := json.Unmarshal(optsBuf, &options); err != nil {
			return fmt.Errorf("failed to parse duckdns options: %w", err)
		}
		provider = duckdns.New(options)
	case "route53":
		var options route53.Options
		if err := json.Unmarshal(optsBuf, &options); err != nil {
			return fmt.Errorf("failed to parse route53 options: %w", err)
		}
		provider = route53.New(options)
	case "noip":
		var options noip.Options
		if err := json.Unmarshal(optsBuf, &options); err != nil {
			return fmt.Errorf("failed to parse noip options: %w", err)
		}
		provider = noip.New(options)
	default:
		return fmt.Errorf("unknown dns provider: %s", settings.Provider)
	}

	// update the DNS provider
	if err := provider.Update(ipv4, ipv6); err != nil {
		return fmt.Errorf("failed to update dns: %w", err)
	}
	m.mu.Lock()
	m.lastIPv4, m.lastIPv6 = ipv4, ipv6
	m.mu.Unlock()
	m.log.Named("dyndns").Debug("updated dyndns", zap.String("ipv4", ipv4.String()), zap.String("ipv6", ipv6.String()), zap.String("provider", settings.Provider))
	return nil
}

func validateDNSSettings(s DNSSettings) error {
	if len(s.Provider) == 0 {
		return nil
	} else if !s.IPv4 && !s.IPv6 {
		return errors.New("at least one of IPv4 or IPv6 must be enabled")
	}

	buf, err := json.Marshal(s.Options)
	if err != nil {
		return fmt.Errorf("failed to marshal dns settings: %w", err)
	}

	switch s.Provider {
	case "route53":
		var r53 route53.Options
		if err = json.Unmarshal(buf, &r53); err != nil {
			return fmt.Errorf("failed to unmarshal route53 settings: %w", err)
		}
	case "noip":
		var noip noip.Options
		if err = json.Unmarshal(buf, &noip); err != nil {
			return fmt.Errorf("failed to unmarshal noip settings: %w", err)
		}
	case "duckdns":
		var duck duckdns.Options
		if err = json.Unmarshal(buf, &duck); err != nil {
			return fmt.Errorf("failed to unmarshal duckdns settings: %w", err)
		}
	default:
		return fmt.Errorf("unknown dns provider: %s", s.Provider)
	}
	return nil
}
