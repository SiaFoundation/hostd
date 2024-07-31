package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/hostd/host/registry"
)

// GetRegistryValue returns the registry value for the given key. If the key is not
// found should return ErrEntryNotFound.
func (s *Store) GetRegistryValue(key rhp3.RegistryKey) (entry rhp3.RegistryValue, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT revision_number, entry_type, entry_data, entry_signature FROM registry_entries WHERE registry_key=$1`, encode(key.Hash())).Scan(
			decode(&entry.Revision),
			&entry.Type,
			&entry.Data,
			decode(&entry.Signature),
		)
		if errors.Is(err, sql.ErrNoRows) {
			return registry.ErrEntryNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get registry entry: %w", err)
		}
		return nil
	})
	return
}

// SetRegistryValue sets the registry value for the given key.
func (s *Store) SetRegistryValue(entry rhp3.RegistryEntry, expiration uint64) error {
	const (
		selectQuery = `SELECT registry_key FROM registry_entries re WHERE re.registry_key=$1`
		insertQuery = `INSERT INTO registry_entries (registry_key, revision_number, entry_type, entry_signature, entry_data, expiration_height) VALUES ($1, $2, $3, $4, $5, $6) RETURNING registry_key`
		updateQuery = `UPDATE registry_entries SET (registry_key, revision_number, entry_type, entry_signature, entry_data, expiration_height) = ($1, $2, $3, $4, $5, $6) WHERE registry_key=$1 RETURNING registry_key`
	)
	// note: need to error when the registry is full, so can't use upsert
	registryKey := entry.RegistryKey.Hash()
	return s.transaction(func(tx *txn) error {
		err := tx.QueryRow(selectQuery, encode(registryKey)).Scan(decode(&registryKey))
		if errors.Is(err, sql.ErrNoRows) {
			// key doesn't exist, insert it
			count, limit, err := registryLimits(tx)
			if err != nil {
				return fmt.Errorf("failed to get registry limits: %w", err)
			} else if count >= limit {
				return registry.ErrNotEnoughSpace
			}
			err = tx.QueryRow(insertQuery, encode(registryKey), encode(entry.Revision), entry.Type, encode(entry.Signature), entry.Data, encode(expiration)).Scan(decode(&registryKey))
			if err != nil {
				return fmt.Errorf("failed to insert registry entry: %w", err)
			} else if err := incrementNumericStat(tx, metricRegistryEntries, 1, time.Now()); err != nil {
				return fmt.Errorf("failed to track registry entry: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get registry entry: %w", err)
		}
		// key exists, update it
		return tx.QueryRow(updateQuery, encode(registryKey), encode(entry.Revision), entry.Type, encode(entry.Signature), entry.Data, encode(expiration)).Scan(decode(&registryKey))
	})
}

// RegistryEntries returns the current number of entries as well as the
// maximum number of entries the registry can hold.
func (s *Store) RegistryEntries() (count, limit uint64, err error) {
	err = s.transaction(func(tx *txn) error {
		count, limit, err = registryLimits(tx)
		return err
	})
	return
}

func registryLimits(tx *txn) (count, limit uint64, err error) {
	err = tx.QueryRow(`SELECT COALESCE(COUNT(re.registry_key), 0), COALESCE(hs.registry_limit, 0) FROM host_settings hs LEFT JOIN registry_entries re ON (true);`).Scan(&count, &limit)
	return
}
