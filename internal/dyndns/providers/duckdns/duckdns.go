package duckdns

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.sia.tech/hostd/internal/dyndns"
)

type (
	// Options is the set of options for the DuckDNS provider.
	Options struct {
		Token    string
		Hostname string
	}

	// Provider implements the DNS provider interface for DuckDNS.
	Provider struct {
		options Options
	}
)

var (
	c = &http.Client{
		Timeout: time.Second * 15,
	}

	// ErrUnknown is returned if an unknown error occurs.
	ErrUnknown = errors.New("unknown error")
)

// Update updates the DNS provider using the specified update mode.
// UpdateModeDefault will detect both the IPv4 and IPv6 external IP
// addresses and update the provider with both; an error should only be
// returned if IPv4 and IPv6 updates fail.
// UpdateModeIPv4 will detect the external IPv4 address and update the
// provider's A record.
// UpdateModeIPv6 will detect the external IPv6 address and update the
// provider's AAAA record.
func (p *Provider) Update(mode dyndns.UpdateMode) error {
	u, err := url.Parse("https://www.duckdns.org/update")
	if err != nil {
		return fmt.Errorf("failed to parse update url: %w", err)
	}

	v := url.Values{
		"domains": []string{p.options.Hostname},
		"token":   []string{p.options.Token},
	}

	updateV4 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP4)
		if err != nil {
			return fmt.Errorf("unable to get ipv4 address: %w", err)
		}
		v["ip"] = []string{ip}
		return nil
	}

	updateV6 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP6)
		if err != nil {
			return fmt.Errorf("unable to get ipv6 address: %w", err)
		}
		v["ipv6"] = []string{ip}
		return nil
	}

	switch mode {
	case dyndns.UpdateModeIPv4:
		if err := updateV4(); err != nil {
			return err
		}
	case dyndns.UpdateModeIPv6:
		if err := updateV6(); err != nil {
			return err
		}
	case dyndns.UpdateModeDefault:
		var v4Err, v6Err error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			v4Err = updateV4()
		}()
		go func() {
			defer wg.Done()
			v6Err = updateV6()
		}()
		if v4Err != nil && v6Err != nil {
			return fmt.Errorf("unable to get ipv4: %w or ipv6: %s", v4Err, v6Err)
		}
	default:
		return fmt.Errorf("unable to update mode %v: %w", mode, dyndns.ErrUnsupported)
	}

	u.RawQuery = v.Encode()
	resp, err := c.Get(u.String())
	if err != nil {
		return fmt.Errorf("failed to make update request: %w", err)
	}
	defer resp.Body.Close()

	lr := io.LimitReader(resp.Body, 10)
	body, err := io.ReadAll(lr)
	if err != nil {
		return fmt.Errorf("failed to read response status: %w", err)
	} else if string(body) == "OK" {
		return nil
	}

	return fmt.Errorf("failed to update host: %w", ErrUnknown)
}

// New creates a new DuckDNS provider.
func New(opts Options) dyndns.Provider {
	return &Provider{
		options: opts,
	}
}
