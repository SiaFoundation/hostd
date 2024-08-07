//go:build testing

package settings

import "errors"

// enables announcements on localhost
func validateNetAddress(netaddress string) error {
	if netaddress == "" {
		return errors.New("net address is empty")
	}
	return nil
}
