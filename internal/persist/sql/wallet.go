package sql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// A walletTxn is a SQL transaction that modifies the wallet state.
	walletTxn struct {
		tx tx
	}

	// A WalletStore gets and sets the current state of a wallet.
	WalletStore struct {
		db     *Store
		closed chan struct{}
	}
)

// AddSiacoinElement adds a spendable siacoin output to the wallet.
func (wtx *walletTxn) AddSiacoinElement(utxo wallet.SiacoinElement) error {
	_, err := wtx.tx.Exec(`
		INSERT INTO wallet_utxos (id, amount, unlock_hash) VALUES (?, ?, ?)
	`, valueHash(utxo.ID), valueCurrency(utxo.Value), valueHash(utxo.UnlockHash))
	return err
}

// RemoveSiacoinElement removes a spendable siacoin output from the wallet
// either due to a spend or a reorg.
func (wtx *walletTxn) RemoveSiacoinElement(id types.SiacoinOutputID) error {
	_, err := wtx.tx.Exec(`DELETE FROM wallet_utxos WHERE id=?`, valueHash(id))
	return err
}

// AddTransaction adds a transaction to the wallet.
func (wtx *walletTxn) AddTransaction(txn wallet.Transaction) error {
	var buf bytes.Buffer
	if err := txn.Transaction.MarshalSia(&buf); err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}
	_, err := wtx.tx.Exec(`INSERT INTO wallet_transactions (id, block_id, block_height, source, inflow, outflow, raw_data, date_created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, valueHash(txn.ID), valueHash(txn.Index.ID), txn.Index.Height, txn.Source, valueCurrency(txn.Inflow), valueCurrency(txn.Outflow), buf.Bytes(), valueTime(txn.Timestamp))
	return err
}

// RemoveTransaction removes a transaction from the wallet.
func (wtx *walletTxn) RemoveTransaction(id types.TransactionID) error {
	_, err := wtx.tx.Exec(`DELETE FROM wallet_transactions WHERE id=?`, valueHash(id))
	return err
}

// SetLastChange sets the last processed consensus change.
func (wtx *walletTxn) SetLastChange(id modules.ConsensusChangeID) error {
	_, err := wtx.tx.Exec(`INSERT INTO wallet_settings (last_processed_change) VALUES(?) ON CONFLICT (ID) DO UPDATE SET last_processed_change=excluded.last_processed_change`, valueHash(id))
	return err
}

// GetLastChange gets the last processed consensus change.
func (ws *WalletStore) GetLastChange() (id modules.ConsensusChangeID, err error) {
	select {
	case <-ws.closed:
		return id, ErrStoreClosed
	default:
	}
	err = ws.db.db.QueryRow(`SELECT last_processed_change FROM wallet_settings`).Scan(scanHash((*[32]byte)(&id)))
	if errors.Is(err, sql.ErrNoRows) {
		return modules.ConsensusChangeBeginning, nil
	}
	return
}

// UnspentSiacoinElements returns the spendable siacoin outputs in the wallet.
func (ws *WalletStore) UnspentSiacoinElements() (utxos []wallet.SiacoinElement, err error) {
	select {
	case <-ws.closed:
		return nil, ErrStoreClosed
	default:
	}
	rows, err := ws.db.db.Query(`SELECT id, amount, unlock_hash FROM wallet_utxos`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to query unspent siacoin elements: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var utxo wallet.SiacoinElement
		if err := rows.Scan(scanHash((*[32]byte)(&utxo.ID)), scanCurrency(&utxo.Value), scanHash((*[32]byte)(&utxo.UnlockHash))); err != nil {
			return nil, fmt.Errorf("failed to scan unspent siacoin element: %w", err)
		}
		utxos = append(utxos, utxo)
	}
	return utxos, nil
}

// Transactions returns the transactions in the wallet.
func (ws *WalletStore) Transactions(skip, max int) (txns []wallet.Transaction, err error) {
	select {
	case <-ws.closed:
		return nil, ErrStoreClosed
	default:
	}
	rows, err := ws.db.db.Query(`SELECT id, block_id, block_height, inflow, outflow, raw_data, date_created FROM wallet_transactions ORDER BY date_created DESC LIMIT ? OFFSET ?`, max, skip)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var txn wallet.Transaction
		var buf []byte
		if err := rows.Scan(scanHash((*[32]byte)(&txn.ID)), scanHash((*[32]byte)(&txn.Index.ID)), &txn.Index.Height, scanCurrency(&txn.Inflow), scanCurrency(&txn.Outflow), &buf, scanTime(&txn.Timestamp)); err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		} else if err := txn.Transaction.UnmarshalSia(bytes.NewReader(buf)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal transaction data: %w", err)
		}
		txns = append(txns, txn)
	}
	return
}

// Transaction begins an update transaction on the wallet store.
func (ws *WalletStore) Transaction(ctx context.Context, fn func(wallet.UpdateTransaction) error) error {
	select {
	case <-ws.closed:
		return ErrStoreClosed
	default:
	}
	return ws.db.transaction(ctx, func(tx tx) error {
		return fn(&walletTxn{tx})
	})
}

// Close prevents the store from being used.
func (ws *WalletStore) Close() error {
	select {
	case <-ws.closed:
		return nil
	default:
	}
	close(ws.closed)
	return nil
}

// NewWalletStore initializes a new wallet store.
func NewWalletStore(db *Store) *WalletStore {
	return &WalletStore{
		db: db,
	}
}
