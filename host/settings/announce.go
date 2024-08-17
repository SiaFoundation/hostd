package settings

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
)

type (
	// An Announcement contains the host's announced netaddress
	Announcement struct {
		Index   types.ChainIndex `json:"index"`
		Address string           `json:"address"`
	}
)

// Announce announces the host to the network
func (m *ConfigManager) Announce() error {
	// get the current settings
	settings := m.Settings()

	if err := validateNetAddress(settings.NetAddress); err != nil {
		if m.validateNetAddress {
			return fmt.Errorf("failed to validate net address: %w", err)
		} else {
			m.log.Warn("invalid net address", zap.Error(err))
		}
	}

	minerFee := m.chain.RecommendedFee().Mul64(announcementTxnSize)

	ha := chain.HostAnnouncement{
		NetAddress: settings.NetAddress,
	}

	cs := m.chain.TipState()
	if cs.Index.Height < cs.Network.HardforkV2.AllowHeight {
		// create a transaction with an announcement
		txn := types.Transaction{
			ArbitraryData: [][]byte{
				ha.ToArbitraryData(m.hostKey),
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
		if _, err := m.chain.AddPoolTransactions(txnset); err != nil {
			m.wallet.ReleaseInputs([]types.Transaction{txn}, nil)
			return fmt.Errorf("failed to add transaction to pool: %w", err)
		}
		m.syncer.BroadcastTransactionSet(txnset)
		m.log.Debug("broadcast announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	} else {
		// create a v2 transaction with an announcement
		txn := types.V2Transaction{
			Attestations: []types.Attestation{
				ha.ToAttestation(cs, m.hostKey),
			},
			MinerFee: minerFee,
		}
		basis, toSign, err := m.wallet.FundV2Transaction(&txn, minerFee, true)
		if err != nil {
			return fmt.Errorf("failed to fund transaction: %w", err)
		}
		m.wallet.SignV2Inputs(basis, &txn, toSign)
		txnset := append(m.chain.V2UnconfirmedParents(txn), txn)
		if _, err := m.chain.AddV2PoolTransactions(cs.Index, txnset); err != nil {
			m.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			return fmt.Errorf("failed to add transaction to pool: %w", err)
		}
		m.syncer.BroadcastV2TransactionSet(cs.Index, txnset)
		m.log.Debug("broadcast v2 announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	}
	return nil
}

func validateNetAddress(netaddress string) error {
	host, port, err := net.SplitHostPort(netaddress)
	if err != nil {
		return fmt.Errorf("invalid net address %q: net addresses must contain a host and port: %w", netaddress, err)
	}

	// Check that the host is not empty or localhost.
	if host == "" {
		return errors.New("empty net address")
	} else if host == "localhost" {
		return errors.New("net address cannot be localhost")
	}

	// Check that the port is a valid number.
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	} else if n < 1 || n > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	// If the host is an IP address, check that it is a public IP address.
	ip := net.ParseIP(host)
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || !ip.IsGlobalUnicast() {
			return fmt.Errorf("invalid net address %q: only public IP addresses allowed", host)
		}
		return nil
	}
	return nil
}
