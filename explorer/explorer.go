package explorer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.sia.tech/core/types"
)

// An Explorer retrieves data about the Sia network from an external source.
type Explorer struct {
	url string
}

var client = &http.Client{
	Timeout: 30 * time.Second,
}

func drainAndClose(r io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(r, 1024*1024))
	r.Close()
}

func makeRequest(ctx context.Context, method, url string, requestBody, response any) error {
	var body io.Reader
	if requestBody != nil {
		js, _ := json.Marshal(requestBody)
		body = bytes.NewReader(js)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		buf, err := io.ReadAll(io.LimitReader(resp.Body, 4096)) // the body should return an error message
		if err != nil {
			return fmt.Errorf("unexpected status code: %d, failed to read response body: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(buf))
	}

	if response == nil {
		return nil
	} else if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}

// BaseURL returns the base URL of the Explorer.
func (e *Explorer) BaseURL() string {
	return e.url
}

// SiacoinExchangeRate returns the exchange rate for the given currency.
func (e *Explorer) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	err = makeRequest(ctx, http.MethodGet, fmt.Sprintf("%s/exchange-rate/siacoin/%s", e.url, currency), nil, &rate)
	return
}

// AddressCheckpoint returns the chain index at which the given address was first seen or
// the current tip if the address is not found.
func (e *Explorer) AddressCheckpoint(ctx context.Context, address types.Address) (index types.ChainIndex, err error) {
	err = makeRequest(ctx, http.MethodGet, fmt.Sprintf("%s/addresses/%s/checkpoint", e.url, address.String()), nil, &index)
	return
}

// New returns a new Explorer client.
func New(url string) *Explorer {
	return &Explorer{url: url}
}
