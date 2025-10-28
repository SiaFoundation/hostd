package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var _ wallet.SingleAddressStore = (*Store)(nil)

// AddBroadcastedSet adds a set of broadcasted transactions. The wallet
// will periodically rebroadcast the transactions in this set until all
// transactions are gone from the transaction pool or one week has
// passed.
func (s *Store) AddBroadcastedSet(txnset wallet.BroadcastedSet) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`INSERT INTO wallet_broadcasted_txnsets (id, basis, raw_transactions, date_created) VALUES (?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
			encode(txnset.ID()), encode(txnset.Basis), encodeSlice(txnset.Transactions), encode(txnset.BroadcastedAt))
		return err
	})
}

// BroadcastedSets returns recently broadcasted sets.
func (s *Store) BroadcastedSets() (sets []wallet.BroadcastedSet, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT basis, raw_transactions, date_created FROM wallet_broadcasted_txnsets ORDER BY date_created DESC`)
		if err != nil {
			return fmt.Errorf("failed to query broadcasted sets: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var buf []byte
			var set wallet.BroadcastedSet
			if err := rows.Scan(decode(&set.Basis), &buf, decode(&set.BroadcastedAt)); err != nil {
				return fmt.Errorf("failed to scan broadcasted set: %w", err)
			}
			dec := types.NewBufDecoder(buf)
			types.DecodeSlice(dec, &set.Transactions)
			if err := dec.Err(); err != nil {
				return fmt.Errorf("failed to decode broadcasted set transactions: %w", err)
			}
			sets = append(sets, set)
		}
		return nil
	})
	return
}

// RemoveBroadcastedSet removes a set so it's no longer rebroadcasted.
func (s *Store) RemoveBroadcastedSet(txnset wallet.BroadcastedSet) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`DELETE FROM wallet_broadcasted_txnsets WHERE id = ?`, encode(txnset.ID()))
		return err
	})
}

func getProofBasis(tx *txn) (index types.ChainIndex, err error) {
	err = tx.QueryRow(`SELECT last_scanned_index FROM global_settings`).Scan(decode(&index))
	return
}

// UnspentSiacoinElements returns the spendable siacoin outputs in the wallet.
func (s *Store) UnspentSiacoinElements() (basis types.ChainIndex, utxos []types.SiacoinElement, err error) {
	err = s.transaction(func(tx *txn) error {
		basis, err = getProofBasis(tx)
		if err != nil {
			return fmt.Errorf("failed to get proof basis: %w", err)
		}
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

// WalletEvent retrieves a wallet event by its ID.
// If the event does not exist, wallet.ErrEventNotFound is returned.
func (s *Store) WalletEvent(id types.Hash256) (event wallet.Event, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT raw_data FROM wallet_events WHERE id = ?`, encode(id)).Scan(decode(&event))
		if errors.Is(err, sql.ErrNoRows) {
			return wallet.ErrEventNotFound
		}
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
		if (err == nil && len(buf) != len(seedHash)) || errors.Is(err, sql.ErrNoRows) {
			// wallet not initialized, set seed hash
			_, err := tx.Exec(`UPDATE global_settings SET wallet_hash=?`, encode(seedHash))
			return err
		} else if err != nil {
			return fmt.Errorf("failed to query wallet seed hash: %w", err)
		} else if seedHash != *(*types.Hash256)(buf) {
			return wallet.ErrDifferentSeed
		}
		return nil
	})
}
