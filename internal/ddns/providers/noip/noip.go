// Package noip implements the No-IP dynamic DNS provider.
package noip

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/internal/ddns"
)

type (
	// Options is the set of options for the No-IP provider.
	Options struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Hostname string `json:"hostname"`
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

// Update implements the ddns.Provider interface for No-IP.
func (p *Provider) Update(ipv4, ipv6 net.IP) error {
	if ipv4 == nil && ipv6 == nil {
		return errors.New("no ip addresses provided")
	}

	var ips []string
	if ipv4 != nil {
		ips = append(ips, ipv4.String())
	}
	if ipv6 != nil {
		ips = append(ips, ipv6.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	u, err := url.Parse("https://dynupdate.no-ip.com/nic/update")
	if err != nil {
		panic(fmt.Errorf("failed to parse update url: %w", err))
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

	body, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return fmt.Errorf("failed to read response status: %w", err)
	}

	switch status := strings.ToLower(strings.TrimSpace(string(body))); status {
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

// ValidateOptions validates the options for the No-IP provider.
func ValidateOptions(opts Options) error {
	switch {
	case len(opts.Email) == 0:
		return errors.New("email is required")
	case len(opts.Password) == 0:
		return errors.New("password is required")
	case len(opts.Hostname) == 0:
		return errors.New("hostname is required")
	}
	return nil
}

// New creates a new No-IP provider.
func New(opts Options) ddns.Provider {
	return &Provider{
		options: opts,
	}
}
