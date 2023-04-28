package route53

import (
	"errors"
	"fmt"
	"net"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"go.sia.tech/hostd/internal/dyndns"
)

type (
	// Options is the set of configuration options for the Route53 provider.
	Options struct {
		ID       string `json:"ID"`
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

// Update implements the dyndns.Provider interface for AWS Route 53.
func (p *Provider) Update(ipv4, ipv6 net.IP) error {
	if ipv4 == nil && ipv6 == nil {
		return errors.New("no ip addresses provided")
	}
	creds := credentials.NewStaticCredentials(p.options.ID, p.options.Secret, "")
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
	})
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	svc := route53.New(sess)

	var changes []*route53.Change
	if ipv4 != nil {
		changes = append(changes, p.buildChange(ipv4.String(), "A"))
	}
	if ipv6 != nil {
		changes = append(changes, p.buildChange(ipv6.String(), "AAAA"))
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
