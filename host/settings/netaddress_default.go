//go:build !testing

package settings

import (
	"errors"
	"fmt"
	"net"
	"strconv"
)

func validateNetAddress(netaddress string) error {
	host, port, err := net.SplitHostPort(netaddress)
	if err != nil {
		return fmt.Errorf("invalid net address %q: net addresses must contain a host and port: %w", netaddress, err)
	}

	// Check that the host is not empty or localhost.
	if host == "" {
		return errors.New("empty net address")
	} else if host == "localhost" {
		return errors.New("net address cannot be localhost")
	}

	// Check that the port is a valid number.
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	} else if n < 1 || n > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	// If the host is an IP address, check that it is a public IP address.
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || !ip.IsGlobalUnicast() {
			return fmt.Errorf("invalid net address %q: only public IP addresses allowed", host)
		}
		return nil
	}
	return nil
}
