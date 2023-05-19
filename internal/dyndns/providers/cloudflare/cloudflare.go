package cloudflare

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cloudflare/cloudflare-go"
)

type (
	// Options is the set of options for the Cloudflare provider.
	Options struct {
		Token    string `json:"token"`
		ZoneID   string `json:"zoneID"`
		Hostname string `json:"hostname"`
	}

	// Provider implements the dyndns.Provider interface for Cloudflare.
	Provider struct {
		opts Options
	}
)

var errNotFound = errors.New("record not found")

func getRecordID(client *cloudflare.API, zoneID *cloudflare.ResourceContainer, hostname, recordType string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	records, _, err := client.ListDNSRecords(ctx, zoneID, cloudflare.ListDNSRecordsParams{
		Name: hostname,
		Type: recordType,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list dns records: %w", err)
	} else if len(records) == 0 {
		return "", errNotFound
	}
	return records[0].ID, nil
}

// Update implements the dyndns.Provider interface for DuckDNS.
func (p *Provider) Update(ipv4, ipv6 net.IP) error {
	if ipv4 == nil && ipv6 == nil {
		return errors.New("no ip addresses provided")
	}

	client, err := cloudflare.NewWithAPIToken(p.opts.Token)
	if err != nil {
		return fmt.Errorf("failed to create cloudflare client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	zoneID := cloudflare.ZoneIdentifier(p.opts.ZoneID)

	if ipv4 != nil {
		recordID, err := getRecordID(client, zoneID, p.opts.Hostname, "A")
		if err != nil && !errors.Is(err, errNotFound) {
			return fmt.Errorf("failed to get record id: %w", err)
		}

		if len(recordID) == 0 {
			_, err := client.CreateDNSRecord(ctx, zoneID, cloudflare.CreateDNSRecordParams{
				Type:    "A",
				Name:    p.opts.Hostname,
				ZoneID:  p.opts.ZoneID,
				Content: ipv4.String(),
				TTL:     300,
				Proxied: cloudflare.BoolPtr(false),
				Comment: "managed by hostd",
			})
			if err != nil {
				return fmt.Errorf("failed to create ipv4 record: %w", err)
			}
			return nil
		}

		_, err = client.UpdateDNSRecord(ctx, zoneID, cloudflare.UpdateDNSRecordParams{
			ID:      recordID,
			Type:    "A",
			Name:    p.opts.Hostname,
			Content: ipv4.String(),
			TTL:     300,
			Proxied: cloudflare.BoolPtr(false),
			Comment: "managed by hostd",
		})
		if err != nil {
			return fmt.Errorf("failed to update ipv4 record: %w", err)
		}
	}

	if ipv6 != nil {
		recordID, err := getRecordID(client, zoneID, p.opts.Hostname, "AAAA")
		if err != nil && !errors.Is(err, errNotFound) {
			return fmt.Errorf("failed to get record id: %w", err)
		}

		if len(recordID) == 0 {
			_, err := client.CreateDNSRecord(ctx, zoneID, cloudflare.CreateDNSRecordParams{
				Type:    "AAAA",
				Name:    p.opts.Hostname,
				ZoneID:  p.opts.ZoneID,
				Content: ipv6.String(),
				TTL:     300,
				Proxied: cloudflare.BoolPtr(false),
				Comment: "managed by hostd",
			})
			if err != nil {
				return fmt.Errorf("failed to create ipv6 record: %w", err)
			}
			return nil
		}

		_, err = client.UpdateDNSRecord(ctx, zoneID, cloudflare.UpdateDNSRecordParams{
			ID:      recordID,
			Type:    "AAAA",
			Name:    p.opts.Hostname,
			Content: ipv6.String(),
			TTL:     300,
			Proxied: cloudflare.BoolPtr(false),
			Comment: "managed by hostd",
		})
		if err != nil {
			return fmt.Errorf("failed to update ipv6 record: %w", err)
		}
	}
	return nil
}

// ValidateOptions validates the options for the Cloudflare provider.
func ValidateOptions(opts Options) error {
	switch {
	case len(opts.Token) == 0:
		return errors.New("token is required")
	case len(opts.ZoneID) == 0:
		return errors.New("zone id is required")
	case len(opts.Hostname) == 0:
		return errors.New("hostname is required")
	}
	return nil
}

// New returns a new Cloudflare provider.
func New(opts Options) *Provider {
	return &Provider{
		opts: opts,
	}
}
