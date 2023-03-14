package test

import (
	"fmt"
	"path/filepath"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.sia.tech/hostd/wallet"
	"go.uber.org/zap"
)

// A Wallet is an ephemeral wallet that can be used for testing.
type Wallet struct {
	*Node
	store  *sqlite.Store
	wallet *wallet.SingleAddressWallet
}

// Close closes the wallet.
func (w *Wallet) Close() error {
	w.wallet.Close()
	w.store.Close()
	w.Node.Close()
	return nil
}

// Store returns the wallet's store.
func (w *Wallet) Store() *sqlite.Store {
	return w.store
}

// Wallet returns the wallet's wallet.
func (w *Wallet) Wallet() *wallet.SingleAddressWallet {
	return w.wallet
}

// SendSiacoins helper func to send siacoins from a wallet.
func (w *Wallet) SendSiacoins(outputs []types.SiacoinOutput) (txn types.Transaction, err error) {
	var siacoinOutput types.Currency
	for _, o := range outputs {
		siacoinOutput = siacoinOutput.Add(o.Value)
	}
	txn.SiacoinOutputs = outputs

	toSign, release, err := w.wallet.FundTransaction(&txn, siacoinOutput)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()
	if err := w.wallet.SignTransaction(w.ChainManager().TipState(), &txn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		return types.Transaction{}, fmt.Errorf("failed to sign transaction: %w", err)
	} else if err := w.tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		return types.Transaction{}, fmt.Errorf("failed to accept transaction set: %w", err)
	}
	return txn, nil
}

// NewWallet initializes a new test wallet.
func NewWallet(privKey types.PrivateKey, dir string) (*Wallet, error) {
	node, err := NewNode(privKey, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}
	opt := zap.NewDevelopmentConfig()
	opt.OutputPaths = []string{filepath.Join(dir, "hostd.log")}
	log, err := opt.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}
	wallet, err := wallet.NewSingleAddressWallet(privKey, node.cm, node.tp, db, log.Named("wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}
	return &Wallet{
		Node:   node,
		store:  db,
		wallet: wallet,
	}, nil
}
