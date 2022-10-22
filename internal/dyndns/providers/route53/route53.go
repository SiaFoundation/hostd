package route53

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"go.sia.tech/hostd/internal/dyndns"
)

type (
	// Options is the set of configuration options for the Route53 provider.
	Options struct {
		ID       string `json:"id"`
		Secret   string `json:"secret"`
		ZoneID   string `json:"zoneID"`
		Hostname string `json:"hostname"`
	}
	// Provider implements the DNS provider interface for AWS Route 53.
	Provider struct {
		options Options
	}
)

// ErrUnsupported is returned if the provider does not support the update
// mode.
var ErrUnsupported = errors.New("unsupported")

func (p *Provider) buildChange(ip, recordType string) *route53.Change {
	return &route53.Change{
		Action: aws.String(route53.ChangeActionUpsert),
		ResourceRecordSet: &route53.ResourceRecordSet{
			Name: aws.String(p.options.Hostname),
			Type: aws.String(recordType),
			TTL:  aws.Int64(300),
			ResourceRecords: []*route53.ResourceRecord{
				{
					Value: aws.String(ip),
				},
			},
		},
	}
}

// Update updates the DNS provider using the specified update mode.
// UpdateModeDefault will detect both the IPv4 and IPv6 external IP
// addresses and update the provider with both; an error should only be
// returned if IPv4 and IPv6 updates fail.
// UpdateModeIPv4 will detect the external IPv4 address and update the
// provider's A record.
// UpdateModeIPv6 will detect the external IPv6 address and update the
// provider's AAAA record.
func (p *Provider) Update(mode dyndns.UpdateMode) error {
	creds := credentials.NewStaticCredentials(p.options.ID, p.options.Secret, "")
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
	})
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	svc := route53.New(sess)

	var changes []*route53.Change

	updateV4 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP4)
		if err != nil {
			return fmt.Errorf("unable to get ipv4 address: %w", err)
		}
		changes = append(changes, p.buildChange(ip, "A"))

		return nil
	}

	updateV6 := func() error {
		ip, err := dyndns.GetIP(dyndns.NetworkModeTCP6)
		if err != nil {
			return fmt.Errorf("unable to get ipv6 address: %w", err)
		}
		changes = append(changes, p.buildChange(ip, "AAAA"))

		return nil
	}

	switch mode {
	case dyndns.UpdateModeIPv4:
		if err := updateV4(); err != nil {
			return err
		}
	case dyndns.UpdateModeIPv6:
		if err := updateV6(); err != nil {
			return err
		}
	case dyndns.UpdateModeDefault:
		var v4Err, v6Err error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()

			ip, err := dyndns.GetIP(dyndns.NetworkModeTCP4)
			if err != nil {
				v4Err = fmt.Errorf("unable to get ipv4 address: %w", err)
				return
			}
			changes = append(changes, p.buildChange(ip, "A"))
		}()
		go func() {
			defer wg.Done()

			ip, err := dyndns.GetIP(dyndns.NetworkModeTCP6)
			if err != nil {
				v6Err = fmt.Errorf("unable to get ipv6 address: %w", err)
				return
			}
			changes = append(changes, p.buildChange(ip, "AAAA"))
		}()
		if v4Err != nil && v6Err != nil {
			return fmt.Errorf("unable to get ipv4: %w or ipv6: %s", v4Err, v6Err)
		}
	default:
		return fmt.Errorf("unable to update mode %v: %w", mode, dyndns.ErrUnsupported)
	}

	_, err = svc.ChangeResourceRecordSets(&route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(p.options.ZoneID),
		ChangeBatch: &route53.ChangeBatch{
			Changes: changes,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update dns: %w", err)
	}

	return nil
}

// New creates a new Route53 provider.
func New(opts Options) dyndns.Provider {
	return &Provider{
		options: opts,
	}
}
