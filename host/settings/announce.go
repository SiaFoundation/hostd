package settings

import (
	"fmt"

	"go.sia.tech/core/types"
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
		return err
	}

	// create a transaction with an announcement
	minerFee := m.chain.RecommendedFee().Mul64(announcementTxnSize)
	txn := types.Transaction{
		ArbitraryData: [][]byte{
			createAnnouncement(m.hostKey, settings.NetAddress),
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
	return nil
}
