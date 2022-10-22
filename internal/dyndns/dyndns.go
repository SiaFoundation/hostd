package dyndns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type (
	// NetworkMode is the network mode to use when getting the IP address.
	NetworkMode string

	// UpdateMode determines how the provider should update the IP address.
	UpdateMode uint8

	// Provider is an interface for dynamically updating the IP address on
	// a DNS provider.
	Provider interface {
		// Update updates the DNS provider using the specified update mode.
		// UpdateModeDefault will detect both the IPv4 and IPv6 external IP
		// addresses and update the provider with both; an error should only be
		// returned if IPv4 and IPv6 updates fail.
		// UpdateModeIPv4 will detect the external IPv4 address and update the
		// provider's A record.
		// UpdateModeIPv6 will detect the external IPv6 address and update the
		// provider's AAAA record.
		Update(mode UpdateMode) error
	}
)

const (
	// UpdateModeDefault will detect whether the host has a public IPv4 or
	// IPv6 address and update the DNS provider accordingly.
	UpdateModeDefault = iota
	// UpdateModeIPv4 will force the DNS provider to update using only IPv4.
	UpdateModeIPv4
	// UpdateModeIPv6 will force the DNS provider to update using only IPv6.
	UpdateModeIPv6

	// NetworkModeTCP4 will get the IP address using TCP4.
	NetworkModeTCP4 = "tcp4"
	// NetworkModeTCP6 will get the IP address using TCP6.
	NetworkModeTCP6 = "tcp6"
)

var (
	tcp4Client = fixedHTTPClient(NetworkModeTCP4)
	tcp6Client = fixedHTTPClient(NetworkModeTCP6)

	// ErrEmptyResponse is returned when the ip service returns an empty
	// response.
	ErrEmptyResponse = errors.New("empty response")
	// ErrInvalidIP is returned when the ip service returns an invalid IP.
	ErrInvalidIP = errors.New("invalid ip address")
	// ErrUnsupported is returned if the provider does not support the update
	// mode.
	ErrUnsupported = errors.New("unsupported")

	// ErrUnexpectedResponse is returned when the ip service returns an
	// unexpected response status code.
	ErrUnexpectedResponse = errors.New("unexpected response")
)

// fixedHTTPClient creates a new http client with a customer dialer using only the
// specified TCP mode (tcp4/tcp6) instead of automatically resolving.
func fixedHTTPClient(forcedNetwork string) *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}

	return &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, forcedNetwork, addr)
			},
			DisableKeepAlives:   true,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func getClient(network NetworkMode) (*http.Client, error) {
	switch network {
	case NetworkModeTCP4:
		return tcp4Client, nil
	case NetworkModeTCP6:
		return tcp6Client, nil
	default:
		return nil, fmt.Errorf("unknown network mode: %v", network)
	}
}

// GetIP returns the IP address using the specified network mode.
func GetIP(network NetworkMode) (string, error) {
	client, err := getClient(network)
	if err != nil {
		return "", fmt.Errorf("failed to get client: %w", err)
	}

	// TODO: add fallbacks
	resp, err := client.Get("https://icanhazip.com")
	if err != nil {
		return "", fmt.Errorf("failed to get address: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("received status code: %d: %w", resp.StatusCode, ErrUnexpectedResponse)
	}

	lr := io.LimitReader(resp.Body, 64)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return "", fmt.Errorf("failed to read ip address: %w", err)
	}
	str := strings.TrimSpace(string(buf))

	if len(str) == 0 {
		return "", ErrEmptyResponse
	}

	ip := net.ParseIP(str)
	if ip == nil {
		return "", ErrInvalidIP
	}

	return ip.String(), nil
}
