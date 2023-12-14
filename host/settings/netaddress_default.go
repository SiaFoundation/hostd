//go:build !testing

package settings

import (
	"fmt"
	"net"
)

func validateNetAddress(netaddress string) error {
	addr, _, err := net.SplitHostPort(netaddress)
	if err != nil {
		return fmt.Errorf("invalid net address %q: net addresses must contain an IP and port: %w", netaddress, err)
	}

	ip := net.ParseIP(addr)
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || !ip.IsGlobalUnicast() {
			return fmt.Errorf("invalid net address %q: only public IP addresses allowed", addr)
		}
		return nil
	}

	addrs, err := net.LookupIP(addr)
	if err != nil {
		return fmt.Errorf("failed to resolve net address %q: %w", addr, err)
	} else if len(addrs) == 0 {
		return fmt.Errorf("failed to resolve net address: no addresses found")
	}
	return nil
}
