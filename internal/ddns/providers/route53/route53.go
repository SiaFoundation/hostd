package route53

import (
	"errors"
	"fmt"
	"net"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"go.sia.tech/hostd/v2/internal/ddns"
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

var (
	// ErrCredentialsInvalid is returned when client credentials are invalid.
	ErrCredentialsInvalid = errors.New("invalid credentials")
	// ErrUnknownZone is returned when the hostname is not found.
	ErrUnknownZone = errors.New("unknown zoneID")
	// ErrInvalidHostname is returned when the hostname is invalid.
	ErrInvalidHostname = errors.New("invalid hostname")
)

func (p *Provider) buildChange(ip, recordType string) *route53.Change {
	return &route53.Change{
		Action: aws.String(route53.ChangeActionUpsert),
		ResourceRecordSet: &route53.ResourceRecordSet{
			Name: new(p.options.Hostname),
			Type: new(recordType),
			TTL:  aws.Int64(300),
			ResourceRecords: []*route53.ResourceRecord{
				{
					Value: new(ip),
				},
			},
		},
	}
}

// Update implements the ddns.Provider interface for AWS Route 53.
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
		HostedZoneId: new(p.options.ZoneID),
		ChangeBatch: &route53.ChangeBatch{
			Changes: changes,
		},
	})
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case "InvalidClientTokenId":
			return ErrCredentialsInvalid
		case route53.ErrCodeHostedZoneNotFound:
			return ErrUnknownZone
		case route53.ErrCodeInvalidDomainName:
			return ErrInvalidHostname
		}
	}
	return err
}

// ValidateOptions validates the options for the Route53 provider.
func ValidateOptions(opts Options) error {
	switch {
	case opts.ID == "":
		return errors.New("ID is required")
	case opts.Secret == "":
		return errors.New("secret is required")
	case opts.ZoneID == "":
		return errors.New("zone ID is required")
	case opts.Hostname == "":
		return errors.New("hostname is required")
	}
	return nil
}

// New creates a new Route53 provider.
func New(opts Options) ddns.Provider {
	return &Provider{
		options: opts,
	}
}
