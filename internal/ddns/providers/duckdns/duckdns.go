package duckdns

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"go.sia.tech/hostd/v2/internal/ddns"
)

type (
	// Options is the set of options for the DuckDNS provider.
	Options struct {
		Token    string `json:"token"`
		Hostname string `json:"hostname"`
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

// Update implements the ddns.Provider interface for DuckDNS.
func (p *Provider) Update(ipv4, ipv6 net.IP) error {
	if ipv4 == nil && ipv6 == nil {
		return errors.New("no ip addresses provided")
	}

	u, err := url.Parse("https://www.duckdns.org/update")
	if err != nil {
		panic(fmt.Errorf("failed to parse update url: %w", err))
	}

	v := url.Values{
		"domains": []string{p.options.Hostname},
		"token":   []string{p.options.Token},
	}

	if ipv4 != nil {
		v["ip"] = []string{ipv4.String()}
	}

	if ipv6 != nil {
		v["ipv6"] = []string{ipv6.String()}
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

	// DuckDNS returns "KO" for any error, so return a generic error.
	return fmt.Errorf("failed to update host: %w", ErrUnknown)
}

// ValidateOptions validates the options for the DuckDNS provider.
func ValidateOptions(opts Options) error {
	switch {
	case opts.Token == "":
		return errors.New("token is required")
	case opts.Hostname == "":
		return errors.New("hostname is required")
	}
	return nil
}

// New creates a new DuckDNS provider.
func New(opts Options) ddns.Provider {
	return &Provider{
		options: opts,
	}
}
