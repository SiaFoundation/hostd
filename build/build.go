// Package build contains build-time information.
package build

//go:generate go run gen.go

import "time"

// NetworkName returns the human-readable name of the current network.
func NetworkName() string {
	n, _ := Network()
	switch n.Name {
	case "mainnet":
		return "Mainnet"
	case "zen":
		return "Zen Testnet"
	default:
		return n.Name
	}
}

// Commit returns the commit hash of hostd
func Commit() string {
	return commit
}

// Version returns the version of hostd
func Version() string {
	return version
}

// Time returns the time at which the binary was built.
func Time() time.Time {
	return time.Unix(buildTime, 0)
}
