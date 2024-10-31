package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var _ wallet.SingleAddressStore = (*Store)(nil)

// UnspentSiacoinElements returns the spendable siacoin outputs in the wallet.
func (s *Store) UnspentSiacoinElements() (utxos []types.SiacoinElement, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT id, siacoin_value, sia_address, leaf_index, merkle_proof, maturity_height FROM wallet_siacoin_elements`)
		if err != nil {
			return fmt.Errorf("failed to query unspent siacoin elements: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var se types.SiacoinElement
			if err := rows.Scan(decode(&se.ID), decode(&se.SiacoinOutput.Value), decode(&se.SiacoinOutput.Address), decode(&se.StateElement.LeafIndex), decode(&se.StateElement.MerkleProof), &se.MaturityHeight); err != nil {
				return fmt.Errorf("failed to scan unspent siacoin element: %w", err)
			}
			utxos = append(utxos, se)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate unspent siacoin elements: %w", err)
		}
		return nil
	})
	return
}

// WalletEventCount returns the total number of events relevant to the
// wallet.
func (s *Store) WalletEventCount() (count uint64, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT COUNT(*) FROM wallet_events`).Scan(&count)
		return err
	})
	return
}

// WalletEvents returns a paginated list of transactions ordered by
// maturity height, descending. If no more transactions are available,
// (nil, nil) should be returned.
func (s *Store) WalletEvents(offset, limit int) (events []wallet.Event, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT raw_data FROM wallet_events ORDER BY maturity_height DESC LIMIT ? OFFSET ?`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query wallet events: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var buf []byte
			if err := rows.Scan(&buf); err != nil {
				return fmt.Errorf("failed to scan wallet event: %w", err)
			}
			var event wallet.Event
			if err := json.Unmarshal(buf, &event); err != nil {
				return fmt.Errorf("failed to unmarshal wallet event: %w", err)
			}
			events = append(events, event)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate wallet events: %w", err)
		}
		return nil
	})
	return
}

// VerifyWalletKey checks that the wallet seed matches the seed hash.
// This detects if the user's recovery phrase has changed and the wallet needs
// to rescan.
func (s *Store) VerifyWalletKey(seedHash types.Hash256) error {
	var buf []byte
	return s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT wallet_hash FROM global_settings`).Scan(&buf)
		if errors.Is(err, sql.ErrNoRows) {
			_, err := tx.Exec(`UPDATE global_settings SET wallet_hash=?`, encode(seedHash)) // wallet not initialized, set seed hash
			return err
		} else if err != nil {
			return fmt.Errorf("failed to query wallet seed hash: %w", err)
		} else if seedHash != *(*types.Hash256)(buf) {
			return wallet.ErrDifferentSeed
		}
		return nil
	})
}
