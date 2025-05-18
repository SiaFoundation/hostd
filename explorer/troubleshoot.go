package explorer

import (
	"context"
	"fmt"
	"net/http"
	"time"

	proto2 "go.sia.tech/core/rhp/v2"
	proto3 "go.sia.tech/core/rhp/v3"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// A Host is a host on the Sia network. It contains the public key of the
	// host, the address of the host's RHP2 endpoint, and a list of addresses for
	// RHP4.
	Host struct {
		PublicKey        types.PublicKey    `json:"publicKey"`
		RHP2NetAddress   string             `json:"rhp2NetAddress"`
		RHP4NetAddresses []chain.NetAddress `json:"rhp4NetAddresses"`
	}

	// RHP2Result is the result of testing a host's RHP2 endpoint. It contains
	// the results of the connection, handshake, and scan, as well as any errors
	// or warnings that occurred during the test.
	RHP2Result struct {
		Connected bool          `json:"connected"`
		DialTime  time.Duration `json:"dialTime"`

		Handshake     bool          `json:"handshake"`
		HandshakeTime time.Duration `json:"handshakeTime"`

		Scanned  bool          `json:"scanned"`
		ScanTime time.Duration `json:"scanTime"`

		ResolvedAddresses []string `json:"resolvedAddress"`

		Settings *proto2.HostSettings `json:"settings"`

		Errors   []string `json:"errors"`
		Warnings []string `json:"warnings"`
	}

	// RHP3Result is the result of testing a host's RHP3 endpoint. It contains
	// the results of the connection, handshake, and scan, as well as any errors
	// or warnings that occurred during the test.
	RHP3Result struct {
		Connected bool          `json:"connected"`
		DialTime  time.Duration `json:"dialTime"`

		Handshake     bool          `json:"handshake"`
		HandshakeTime time.Duration `json:"handshakeTime"`

		Scanned  bool          `json:"scanned"`
		ScanTime time.Duration `json:"scanTime"`

		PriceTable *proto3.HostPriceTable `json:"priceTable"`

		Errors   []string `json:"errors"`
		Warnings []string `json:"warnings"`
	}

	// RHP4Result is the result of testing a host's RHP4 endpoint. It contains
	// the results of the connection, handshake, and scan, as well as any errors
	// or warnings that occurred during the test.
	RHP4Result struct {
		NetAddress        chain.NetAddress `json:"netAddress"`
		ResolvedAddresses []string         `json:"resolvedAddress"`

		Connected bool          `json:"connected"`
		DialTime  time.Duration `json:"dialTime"`

		Handshake     bool          `json:"handshake"`
		HandshakeTime time.Duration `json:"handshakeTime"`

		Scanned  bool          `json:"scanned"`
		ScanTime time.Duration `json:"scanTime"`

		Settings *proto4.HostSettings `json:"settings"`

		Errors   []string `json:"errors"`
		Warnings []string `json:"warnings"`
	}

	// A TestResult is the result of testing a host. It contains the public key of the
	// host, the version of the host, and the results of the RHP2, RHP3, and RHP4
	TestResult struct {
		PublicKey types.PublicKey `json:"publicKey"`
		Version   string          `json:"version"`

		// RHP2 and RHP3 are pointers so they are automatically deprecated
		// after the v2 hardfork activates.
		RHP2 *RHP2Result `json:"rhp2,omitempty"`
		RHP3 *RHP3Result `json:"rhp3,omitempty"`

		RHP4 []RHP4Result `json:"rhp4"`
	}
)

// TestConnection tests the connection to a host on the Sia network.
func (e *Explorer) TestConnection(ctx context.Context, host Host) (result TestResult, err error) {
	err = makeRequest(ctx, http.MethodPost, fmt.Sprintf("%s/troubleshoot", e.url), host, &result)
	return
}
