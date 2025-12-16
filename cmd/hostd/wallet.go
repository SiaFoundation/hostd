package main

import (
	"errors"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/alerts"
	"lukechampine.com/frand"
)

var alertID = frand.Entropy256()

type rhpWallet struct {
	am  *alerts.Manager
	saw *wallet.SingleAddressWallet

	mu               sync.Mutex
	lastBalanceAlert time.Time
}

func newRHPWallet(saw *wallet.SingleAddressWallet, am *alerts.Manager) *rhpWallet {
	return &rhpWallet{
		am:  am,
		saw: saw,
	}
}

// Address returns the host's address
func (w *rhpWallet) Address() types.Address {
	return w.saw.Address()
}

// FundV2Transaction funds a transaction with the specified amount of
// Siacoins. If useUnconfirmed is true, the transaction may spend
// unconfirmed outputs. The outputs spent by the transaction are locked
// until they are released by ReleaseInputs.
func (w *rhpWallet) FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error) {
	ci, toSign, err := w.saw.FundV2Transaction(txn, amount, useUnconfirmed)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		// avoid spamming the alert
		w.mu.Lock()
		if time.Since(w.lastBalanceAlert) < time.Hour {
			w.mu.Unlock()
			return ci, toSign, err
		}
		w.lastBalanceAlert = time.Now()
		w.mu.Unlock()

		// register alert
		w.am.Register(alerts.Alert{
			ID:       alertID,
			Severity: alerts.SeverityWarning,
			Category: "wallet",
			Message:  "At least one RPC failed due to insufficient wallet balance.",
			Data: map[string]any{
				"amount": amount.String(),
			},
			Timestamp: time.Now(),
		})
	}
	return ci, toSign, err
}

// SignV2Inputs signs the inputs of a transaction.
func (w *rhpWallet) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	w.saw.SignV2Inputs(txn, toSign)
}

// ReleaseInputs releases the inputs of a transaction. It should only
// be used if the transaction is not going to be broadcast
func (w *rhpWallet) ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) {
	w.saw.ReleaseInputs(txns, v2txns)
}

// BroadcastV2TransactionSet broadcasts a transaction set to the network.
func (w *rhpWallet) BroadcastV2TransactionSet(ci types.ChainIndex, txns []types.V2Transaction) error {
	return w.saw.BroadcastV2TransactionSet(ci, txns)
}
