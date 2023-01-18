package sqlite

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
		tx txn
	}

	// A WalletStore gets and sets the current state of a wallet.
	WalletStore struct {
		db *Store
	}
)

// AddSiacoinElement adds a spendable siacoin output to the wallet.
func (wtx *walletTxn) AddSiacoinElement(utxo wallet.SiacoinElement) error {
	_, err := wtx.tx.Exec(`INSERT INTO wallet_utxos (id, amount, unlock_hash) VALUES (?, ?, ?)`, valueHash(utxo.ID), valueCurrency(utxo.Value), valueHash(utxo.UnlockHash))
	return err
}

// RemoveSiacoinElement removes a spendable siacoin output from the wallet
// either due to a spend or a reorg.
func (wtx *walletTxn) RemoveSiacoinElement(id types.SiacoinOutputID) error {
	_, err := wtx.tx.Exec(`DELETE FROM wallet_utxos WHERE id=?`, valueHash(id))
	return err
}

// AddTransaction adds a transaction to the wallet.
func (wtx *walletTxn) AddTransaction(txn wallet.Transaction, idx uint64) error {
	var buf bytes.Buffer
	if err := txn.Transaction.MarshalSia(&buf); err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}
	_, err := wtx.tx.Exec(`INSERT INTO wallet_transactions (id, block_id, block_height, block_index, source, inflow, outflow, raw_data, date_created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, valueHash(txn.ID), valueHash(txn.Index.ID), txn.Index.Height, idx, txn.Source, valueCurrency(txn.Inflow), valueCurrency(txn.Outflow), buf.Bytes(), valueTime(txn.Timestamp))
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
	err = ws.db.db.QueryRow(`SELECT last_processed_change FROM wallet_settings`).Scan(scanHash((*[32]byte)(&id)))
	if errors.Is(err, sql.ErrNoRows) {
		return modules.ConsensusChangeBeginning, nil
	}
	return
}

// UnspentSiacoinElements returns the spendable siacoin outputs in the wallet.
func (ws *WalletStore) UnspentSiacoinElements() (utxos []wallet.SiacoinElement, err error) {
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

// Transactions returns a paginated list of transactions ordered by block height
// descending. If no transactions are found, (nil, nil) is returned.
func (ws *WalletStore) Transactions(limit, offset int) (txns []wallet.Transaction, err error) {
	rows, err := ws.db.db.Query(`SELECT id, block_id, block_height, source, inflow, outflow, raw_data, date_created FROM wallet_transactions ORDER BY block_height DESC, block_index ASC LIMIT ? OFFSET ?`, limit, offset)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var txn wallet.Transaction
		var buf []byte
		if err := rows.Scan(scanHash((*[32]byte)(&txn.ID)), scanHash((*[32]byte)(&txn.Index.ID)), &txn.Index.Height, &txn.Source, scanCurrency(&txn.Inflow), scanCurrency(&txn.Outflow), &buf, scanTime(&txn.Timestamp)); err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		} else if err := txn.Transaction.UnmarshalSia(bytes.NewReader(buf)); err != nil {
			return nil, fmt.Errorf("failed to unmarshal transaction data: %w", err)
		}
		txns = append(txns, txn)
	}
	return
}

// TransactionCount returns the total number of transactions in the wallet.
func (ws *WalletStore) TransactionCount() (count uint64, err error) {
	err = ws.db.db.QueryRow(`SELECT COUNT(*) FROM wallet_transactions`).Scan(&count)
	return
}

// Update begins an update transaction on the wallet store.
func (ws *WalletStore) Update(ctx context.Context, fn func(wallet.UpdateTransaction) error) error {
	return ws.db.transaction(func(tx txn) error {
		return fn(&walletTxn{tx})
	})
}

// NewWalletStore initializes a new wallet store.
func NewWalletStore(db *Store) *WalletStore {
	return &WalletStore{
		db: db,
	}
}
