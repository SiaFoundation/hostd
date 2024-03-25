package ddns

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
	// Provider is an interface for dynamically updating the IP address on
	// a DNS provider.
	Provider interface {
		// Update updates the DNS provider using the specified update mode.
		// One or both of ipv4 or ipv6 may be included. If both are included,
		// both the 'A' and 'AAAA' records will be updated. If only one is
		// included, only the corresponding record will be updated. If neither
		// are included, the function should return an error.
		Update(ipv4, ipv6 net.IP) error
	}
)

const (
	// NetworkModeTCP4 will get the IP address using TCP4.
	networkModeTCP4 = "tcp4"
	// NetworkModeTCP6 will get the IP address using TCP6.
	networkModeTCP6 = "tcp6"
)

var (
	tcp4Client = fixedHTTPClient(networkModeTCP4)
	tcp6Client = fixedHTTPClient(networkModeTCP6)

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

func getIP(client *http.Client) (net.IP, error) {
	resp, err := client.Get("https://icanhazip.com")
	if err != nil {
		return nil, fmt.Errorf("failed to get address: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received status code: %d: %w", resp.StatusCode, ErrUnexpectedResponse)
	}

	lr := io.LimitReader(resp.Body, 64)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("failed to read ip address: %w", err)
	}
	str := strings.TrimSpace(string(buf))

	if str == "" {
		return nil, ErrEmptyResponse
	}

	ip := net.ParseIP(str)
	if ip == nil {
		return nil, ErrInvalidIP
	}

	return ip, nil
}

// GetIPv4 returns the IPv4 address of the current machine.
func GetIPv4() (net.IP, error) {
	return getIP(tcp4Client)
}

// GetIPv6 returns the IPv6 address of the current machine.
func GetIPv6() (net.IP, error) {
	return getIP(tcp6Client)
}
