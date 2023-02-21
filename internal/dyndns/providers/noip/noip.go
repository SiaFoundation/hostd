// Package noip implements the No-IP dynamic DNS provider.
package noip

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/internal/dyndns"
)

type (
	// Options is the set of options for the No-IP provider.
	Options struct {
		Email    string
		Password string
		Hostname string
	}
	// Provider implements the DNS provider interface for No-IP.
	Provider struct {
		options Options
	}
)

var (
	c = &http.Client{
		Timeout: time.Second * 15,
	}
	noIPUserAgent = fmt.Sprintf("hostd/%s-%s hi@sia.tech", runtime.GOOS, build.Version())

	// ErrClientBlocked is returned if the client is blocked by No-IP.
	ErrClientBlocked = errors.New("client blocked")
	// ErrInvalidAuth is returned if the provided email and password are incorrect.
	ErrInvalidAuth = errors.New("incorrect email or password")
	// ErrServerError is returned if No-IP is having issues.
	ErrServerError = errors.New("server error")
	// ErrUnknownHost is returned if the hostname is not found.
	ErrUnknownHost = errors.New("host does not exist")
	// ErrUserBlocked is returned if the user is blocked by No-IP.
	ErrUserBlocked = errors.New("user is blocked")
	// ErrUnknown is returned if an unknown error occurs.
	ErrUnknown = errors.New("unknown error")
)

func (p *Provider) updateIPs(ips []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	u, err := url.Parse("https://dynupdate.no-ip.com/nic/update")
	if err != nil {
		return fmt.Errorf("failed to parse update url: %w", err)
	}

	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(p.options.Email+":"+p.options.Password))
	u.RawQuery = (url.Values{
		"hostname": []string{p.options.Hostname},
		"myip":     []string{strings.Join(ips, ",")},
	}).Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create update request: %w", err)
	}

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("User-Agent", noIPUserAgent)
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make update request: %w", err)
	}
	defer resp.Body.Close()

	lr := io.LimitReader(resp.Body, 1000)
	body, err := io.ReadAll(lr)
	if err != nil {
		return fmt.Errorf("failed to read response status: %w", err)
	}

	switch status := string(body); status {
	case "good", "nochg":
		return nil
	case "nohost":
		return fmt.Errorf("unable to update host %s: %w", p.options.Hostname, ErrUnknownHost)
	case "badauth":
		return ErrInvalidAuth
	case "badagent":
		return ErrClientBlocked
	case "abuse":
		return ErrUserBlocked
	case "911":
		return ErrServerError
	default:
		return fmt.Errorf("received unknown status \"%s\": %w", status, ErrUnknown)
	}
}

// Update updates the DNS provider using the specified update mode.
// UpdateModeDefault will detect both the IPv4 and IPv6 external IP
// addresses and update the provider with both; an error should only be
// returned if IPv4 and IPv6 updates fail.
// UpdateModeIPv4 will detect the external IPv4 address and update the
// provider's A record.
// UpdateModeIPv6 will detect the external IPv6 address and update the
// provider's AAAA record.
func (p *Provider) Update(mode dyndns.UpdateMode) error {
	var ips []string
	updateV4 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP4)
		if err != nil {
			return fmt.Errorf("unable to get ipv4: %w", err)
		}
		ips = append(ips, ip)

		return nil
	}

	updateV6 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP6)
		if err != nil {
			return fmt.Errorf("unable to get ipv6: %w", err)
		}
		ips = append(ips, ip)

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
			v4Err = updateV4()
			wg.Done()
		}()

		go func() {
			v6Err = updateV6()
			wg.Done()
		}()

		if v4Err != nil && v6Err != nil {
			return fmt.Errorf("unable to get ipv4: %w or ipv6: %s", v4Err, v6Err)
		}
	default:
		return fmt.Errorf("unable to update mode %v: %w", mode, dyndns.ErrUnsupported)
	}

	return p.updateIPs(ips)
}

// New creates a new No-IP provider.
func New(opts Options) dyndns.Provider {
	return &Provider{
		options: opts,
	}
}
