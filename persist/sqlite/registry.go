package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/hostd/host/registry"
)

// GetRegistryValue returns the registry value for the given key. If the key is not
// found should return ErrEntryNotFound.
func (s *Store) GetRegistryValue(key rhpv3.RegistryKey) (entry rhpv3.RegistryValue, _ error) {
	err := s.queryRow(`SELECT revision_number, entry_type, entry_data, entry_signature FROM registry_entries WHERE registry_key=$1`, sqlHash256(key.Hash())).Scan(
		(*sqlUint64)(&entry.Revision),
		&entry.Type,
		&entry.Data,
		(*sqlHash512)(&entry.Signature),
	)
	if errors.Is(err, sql.ErrNoRows) {
		return rhpv3.RegistryValue{}, registry.ErrEntryNotFound
	} else if err != nil {
		return rhpv3.RegistryValue{}, fmt.Errorf("failed to get registry entry: %w", err)
	}
	return
}

// SetRegistryValue sets the registry value for the given key.
func (s *Store) SetRegistryValue(entry rhpv3.RegistryEntry, expiration uint64) error {
	const (
		selectQuery = `SELECT registry_key FROM registry_entries re WHERE re.registry_key=$1`
		insertQuery = `INSERT INTO registry_entries (registry_key, revision_number, entry_type, entry_signature, entry_data, expiration_height) VALUES ($1, $2, $3, $4, $5, $6) RETURNING registry_key`
		updateQuery = `UPDATE registry_entries SET (registry_key, revision_number, entry_type, entry_signature, entry_data, expiration_height) = ($1, $2, $3, $4, $5, $6) WHERE registry_key=$1 RETURNING registry_key`
	)
	// note: need to error when the registry is full, so can't use upsert
	registryKey := entry.RegistryKey.Hash()
	return s.transaction(func(tx txn) error {
		err := tx.QueryRow(selectQuery, sqlHash256(registryKey)).Scan((*sqlHash256)(&registryKey))
		if errors.Is(err, sql.ErrNoRows) {
			// key doesn't exist, insert it
			count, max, err := registryLimits(tx)
			if err != nil {
				return fmt.Errorf("failed to get registry limits: %w", err)
			} else if count >= max {
				return registry.ErrNotEnoughSpace
			}
			err = tx.QueryRow(insertQuery, sqlHash256(registryKey), sqlUint64(entry.Revision), entry.Type, sqlHash512(entry.Signature), entry.Data, sqlUint64(expiration)).Scan((*sqlHash256)(&registryKey))
			if err != nil {
				return fmt.Errorf("failed to insert registry entry: %w", err)
			} else if err := incrementNumericStat(tx, metricRegistryEntries, 1, time.Now()); err != nil {
				return fmt.Errorf("failed to track registry entry: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get registry entry: %w", err)
		}
		// key exists, update it
		return tx.QueryRow(updateQuery, sqlHash256(registryKey), sqlUint64(entry.Revision), entry.Type, sqlHash512(entry.Signature), entry.Data, sqlUint64(expiration)).Scan((*sqlHash256)(&registryKey))
	})
}

// RegistryEntries returns the current number of entries as well as the
// maximum number of entries the registry can hold.
func (s *Store) RegistryEntries() (count, max uint64, err error) {
	return registryLimits(&dbTxn{s})
}

func registryLimits(tx txn) (count, max uint64, err error) {
	err = tx.QueryRow(`SELECT COALESCE(COUNT(re.registry_key), 0), COALESCE(hs.registry_limit, 0) FROM host_settings hs LEFT JOIN registry_entries re ON (true);`).Scan(&count, &max)
	return
}
