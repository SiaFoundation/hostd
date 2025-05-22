package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var _ wallet.SingleAddressStore = (*Store)(nil)

func cleanupLockedUTXOs(tx *txn) error {
	_, err := tx.Exec(`DELETE FROM wallet_locked_utxos WHERE unlock_timestamp < $1`, encode(time.Now()))
	return err
}

// LockUTXOs locks the given UTXOs until the given unlock time. If the UTXO is
// already locked, it is ignored. The unlock time must be in the future.
func (s *Store) LockUTXOs(ids []types.SiacoinOutputID, unlockTime time.Time) error {
	return s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`INSERT INTO wallet_locked_utxos (id, unlock_timestamp) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET unlock_timestamp=EXCLUDED.unlock_timestamp`)
		if err != nil {
			return fmt.Errorf("failed to prepare lock statement: %w", err)
		}
		defer stmt.Close()

		ts := encode(unlockTime)
		for _, id := range ids {
			if _, err := stmt.Exec(encode(id), ts); err != nil {
				return fmt.Errorf("failed to lock UTXO %s: %w", id, err)
			}
		}
		return cleanupLockedUTXOs(tx)
	})
}

// LockedUTXOs returns the IDs of all locked UTXOs. A locked UTXO is one that
// has an unlock timestamp greater than ts.
func (s *Store) LockedUTXOs(ts time.Time) (ids []types.SiacoinOutputID, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT id FROM wallet_locked_utxos WHERE unlock_timestamp > ?`, encode(ts))
		if err != nil {
			return fmt.Errorf("failed to query locked UTXOs: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id types.SiacoinOutputID
			if err := rows.Scan(decode(&id)); err != nil {
				return fmt.Errorf("failed to scan locked UTXO: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate locked UTXOs: %w", err)
		}
		return nil
	})
	return
}

// ReleaseUTXOs unlocks the given UTXOs. If the UTXO is not locked, it is
// ignored.
func (s *Store) ReleaseUTXOs(ids []types.SiacoinOutputID) error {
	return s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`DELETE FROM wallet_locked_utxos WHERE id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare unlock statement: %w", err)
		}
		defer stmt.Close()

		for _, id := range ids {
			if _, err := stmt.Exec(encode(id)); err != nil {
				return fmt.Errorf("failed to unlock UTXO %s: %w", id, err)
			}
		}
		return cleanupLockedUTXOs(tx)
	})
}

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
			var event wallet.Event
			if err := rows.Scan(decode(&event)); err != nil {
				return fmt.Errorf("failed to scan wallet event: %w", err)
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
