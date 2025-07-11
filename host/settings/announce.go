package settings

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

type (
	// An Announcement contains the host's announced netaddress
	Announcement struct {
		Index   types.ChainIndex `json:"index"`
		Address string           `json:"address"`
	}
)

// RHP2NetAddress returns the host's RHP2 net address.
func (m *ConfigManager) RHP2NetAddress() string {
	return net.JoinHostPort(m.Settings().NetAddress, strconv.Itoa(int(m.rhp2Port)))
}

// RHP4NetAddresses returns the host's RHP4 net addresses.
// SiaMux should always be enabled by default. If a host
// certificate is available, it will also include a quic
// address using the common name from the certificate.
func (m *ConfigManager) RHP4NetAddresses() []chain.NetAddress {
	netAddress := m.Settings().NetAddress
	rhp4SiaMuxAddress := net.JoinHostPort(netAddress, strconv.Itoa(int(m.rhp4Port)))

	protos := []chain.NetAddress{
		{Protocol: siamux.Protocol, Address: rhp4SiaMuxAddress},
	}

	if m.certs == nil {
		return protos
	}

	cert, err := m.certs.GetCertificate(context.Background())
	if err != nil {
		m.log.Error("failed to get certificate for RHP4 net address", zap.Error(err))
		return protos
	} else if cert == nil || len(cert.Certificate) == 0 {
		m.log.Warn("no certificate found for RHP4 net address, skipping")
		return protos
	}

	if cert.Leaf == nil {
		// try to load the leaf certificate from the certificate chain
		m.log.Debug("parsing certificate leaf for RHP4 net address")
		c, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			m.log.Error("failed to parse certificate for RHP4 net address", zap.Error(err))
			return protos
		}
		cert.Leaf = c
	}

	var hostname string
	if len(cert.Leaf.DNSNames) != 0 {
		hostname = cert.Leaf.DNSNames[0]
	} else {
		// Fallback to the common name if no SAN names are present
		hostname = cert.Leaf.Subject.CommonName
	}

	if hostname == "" {
		m.log.Warn("certificate name is empty, skipping RHP4 net address")
		return protos
	} else if strings.HasPrefix(hostname, "*") {
		m.log.Debug("wildcard certificate -- using netaddress as is", zap.String("certHost", hostname), zap.String("netAddress", netAddress))
		hostname = netAddress
	}
	// Add the RHP4 QUIC address using the common name from the certificate
	// and the RHP4 port.
	rhp4QuicAddress := net.JoinHostPort(hostname, strconv.Itoa(int(m.rhp4Port)))
	protos = append(protos, chain.NetAddress{
		Protocol: quic.Protocol,
		Address:  rhp4QuicAddress,
	})
	return protos
}

// Announce announces the host to the network
func (m *ConfigManager) Announce() error {
	// get the current settings
	settings := m.Settings()

	if m.validateNetAddress {
		if err := validateHostname(settings.NetAddress); err != nil {
			return fmt.Errorf("failed to validate net address %q: %w", settings.NetAddress, err)
		}
	}

	minerFee := m.wallet.RecommendedFee().Mul64(announcementTxnSize)

	cs := m.chain.TipState()
	if cs.Index.Height < cs.Network.HardforkV2.AllowHeight {
		// create a transaction with an announcement
		txn := types.Transaction{
			ArbitraryData: [][]byte{
				chain.HostAnnouncement{
					PublicKey:  m.hostKey.PublicKey(),
					NetAddress: m.RHP2NetAddress(),
				}.ToArbitraryData(m.hostKey),
			},
			MinerFees: []types.Currency{minerFee},
		}

		// fund the transaction
		toSign, err := m.wallet.FundTransaction(&txn, minerFee, true)
		if err != nil {
			return fmt.Errorf("failed to fund transaction: %w", err)
		}
		m.wallet.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
		txnset := append(m.chain.UnconfirmedParents(txn), txn)
		if err := m.wallet.BroadcastTransactionSet(txnset); err != nil {
			m.wallet.ReleaseInputs([]types.Transaction{txn}, nil)
			return fmt.Errorf("failed to broadcast transaction set: %w", err)
		}
		m.log.Debug("broadcast announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	} else {
		// create a v2 transaction with an announcement
		txn := types.V2Transaction{
			Attestations: []types.Attestation{
				chain.V2HostAnnouncement(m.RHP4NetAddresses()).ToAttestation(cs, m.hostKey),
			},
			MinerFee: minerFee,
		}
		basis, toSign, err := m.wallet.FundV2Transaction(&txn, minerFee, true)
		if err != nil {
			return fmt.Errorf("failed to fund transaction: %w", err)
		}
		m.wallet.SignV2Inputs(&txn, toSign)
		basis, txnset, err := m.chain.V2TransactionSet(basis, txn)
		if err != nil {
			m.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			return fmt.Errorf("failed to create transaction set: %w", err)
		} else if err := m.wallet.BroadcastV2TransactionSet(cs.Index, txnset); err != nil {
			m.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			return fmt.Errorf("failed to add transaction to pool: %w", err)
		}
		m.log.Debug("broadcast v2 announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	}
	return nil
}

func validateHostname(host string) error {
	// Check that the host is not empty or localhost.
	if host == "" {
		return errors.New("empty hostname")
	} else if host == "localhost" {
		return errors.New("hostname cannot be localhost")
	} else if _, _, err := net.SplitHostPort(host); err == nil {
		return errors.New("hostname should not contain a port")
	} else if strings.HasPrefix(host, "[") || strings.HasSuffix(host, "]") {
		return errors.New(`hostname must not start with "[" or end with "]"`)
	}

	// If the host is an IP address, check that it is a public IP address.
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || !ip.IsGlobalUnicast() {
			return errors.New("only public IP addresses allowed")
		}
		return nil
	}
	return nil
}
