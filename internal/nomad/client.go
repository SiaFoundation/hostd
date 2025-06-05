package nomad

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base32"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// issueCertRequest represents a request to issue a certificate.
type issueCertRequest struct {
	CSR       []byte          `json:"csr"`
	Timestamp time.Time       `json:"timestamp"`
	HostKey   types.PublicKey `json:"hostKey"`

	Signature types.Signature `json:"signature"` // covers the CSR and timestamp
}

// SigHash computes the signature hash for the issueCertRequest.
func (u *issueCertRequest) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteBytes(u.CSR)
	h.E.WriteTime(u.Timestamp)
	u.HostKey.EncodeTo(h.E)
	return h.Sum()
}

// signedUpdateRequest represents a request to update DNS records.
type signedUpdateRequest struct {
	IPv4      string          `json:"ipv4"`
	IPv6      string          `json:"ipv6"`
	Timestamp time.Time       `json:"timestamp"`
	HostKey   types.PublicKey `json:"hostKey"`

	Signature types.Signature `json:"signature"` // covers the IPv4 and IPv6 and timestamp
}

// SigHash computes the signature hash for the signedUpdateRequest.
func (u *signedUpdateRequest) SigHash() types.Hash256 {
	h := types.NewHasher()
	h.E.WriteString(u.IPv4)
	h.E.WriteString(u.IPv6)
	h.E.WriteTime(u.Timestamp)
	u.HostKey.EncodeTo(h.E)
	return h.Sum()
}

// An UpdateDNSRequest is a request to update DNS records
type UpdateDNSRequest struct {
	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
}

// An UpdateDNSResponse is the response from a DNS update request
type UpdateDNSResponse struct {
	Domain string `json:"domain"` // the domain that was updated
}

// A Client is a client for the nomad service that issues certificates and updates DNS records.
type Client struct {
	hostKey types.PrivateKey // the host's private key used for signing requests
}

var client = &http.Client{
	Timeout: 5 * time.Minute,
}
var urlEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// HostDomain generates the host domain for a given public key.
func HostDomain(hostKey types.PublicKey) string {
	return fmt.Sprintf("%s.sia.host", strings.ToLower(urlEncoding.EncodeToString(hostKey[:])))
}

// IssueCertificate issues a new certificate for the host.
func (c *Client) IssueCertificate(ctx context.Context) (*rsa.PrivateKey, []*x509.Certificate, error) {
	certKey, err := rsa.GenerateKey(frand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate key: %w", err)
	}
	domain := HostDomain(c.hostKey.PublicKey())
	csr, err := x509.CreateCertificateRequest(frand.Reader, &x509.CertificateRequest{
		DNSNames:           []string{domain},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}, certKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CSR: %w", err)
	}
	certReq := issueCertRequest{
		CSR:       csr,
		Timestamp: time.Now(),
		HostKey:   c.hostKey.PublicKey(),
	}
	certReq.Signature = c.hostKey.SignHash(certReq.SigHash())

	buf, err := json.Marshal(certReq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal certificate request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://nomad.sia.host/api/certificate", bytes.NewReader(buf))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		lr := io.LimitReader(resp.Body, 1024)
		buf, err := io.ReadAll(lr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, nil, fmt.Errorf("received status code %d: %w", resp.StatusCode, errors.New(string(buf)))
	}

	buf, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, buf = pem.Decode(buf)
		if block == nil {
			break
		} else if block.Type != "CERTIFICATE" {
			return nil, nil, fmt.Errorf("unexpected PEM block type: %q", block.Type)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, nil, errors.New("no certificates returned")
	}
	return certKey, certs, nil
}

// UpdateDNS updates the DNS records for the host with the given IPv4 and/or IPv6 addresses.
// It returns the domain that was updated.
func (c *Client) UpdateDNS(ctx context.Context, update UpdateDNSRequest, hostKey types.PrivateKey) (string, error) {
	if update.IPv4 == "" && update.IPv6 == "" {
		return "", errors.New("no IPv4 or IPv6 address provided")
	}
	sr := signedUpdateRequest{
		IPv4:      update.IPv4,
		IPv6:      update.IPv6,
		Timestamp: time.Now(),
		HostKey:   hostKey.PublicKey(),
	}
	sr.Signature = hostKey.SignHash(sr.SigHash())
	buf, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("failed to marshal update request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://nomad.sia.host/api/dns", bytes.NewReader(buf))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		lr := io.LimitReader(resp.Body, 1024)
		buf, err := io.ReadAll(lr)
		if err != nil {
			return "", fmt.Errorf("failed to read error response: %w", err)
		}
		return "", fmt.Errorf("received status code %d: %w", resp.StatusCode, errors.New(string(buf)))
	}

	var respData UpdateDNSResponse
	err = json.NewDecoder(resp.Body).Decode(&respData)
	return respData.Domain, err
}

// NewClient creates a new nomad client with the given host key.
func NewClient(hostKey types.PrivateKey) *Client {
	return &Client{
		hostKey: hostKey,
	}
}
