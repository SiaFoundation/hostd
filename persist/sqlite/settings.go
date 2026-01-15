package sqlite

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/settings/pin"
	"go.uber.org/zap"
)

// PinnedSettings returns the host's pinned settings.
func (s *Store) PinnedSettings(context.Context) (pinned pin.PinnedSettings, err error) {
	const query = `SELECT currency, threshold, storage_pinned, storage_price, ingress_pinned, ingress_price, egress_pinned, egress_price, max_collateral_pinned, max_collateral
FROM host_pinned_settings;`

	err = s.transaction(func(tx *txn) error {
		err = tx.QueryRow(query).Scan(&pinned.Currency, &pinned.Threshold, &pinned.Storage.Pinned, &pinned.Storage.Value, &pinned.Ingress.Pinned, &pinned.Ingress.Value, &pinned.Egress.Pinned, &pinned.Egress.Value, &pinned.MaxCollateral.Pinned, &pinned.MaxCollateral.Value)
		if errors.Is(err, sql.ErrNoRows) {
			pinned = pin.PinnedSettings{
				Currency:  "usd",
				Threshold: 0.02,
			}
			return nil
		}
		return err
	})
	return
}

// UpdatePinnedSettings updates the host's pinned settings.
func (s *Store) UpdatePinnedSettings(_ context.Context, p pin.PinnedSettings) error {
	const query = `INSERT INTO host_pinned_settings (id, currency, threshold, storage_pinned, storage_price, ingress_pinned, ingress_price, egress_pinned, egress_price, max_collateral_pinned, max_collateral)
VALUES (0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET currency=EXCLUDED.currency, threshold=EXCLUDED.threshold,
storage_pinned=EXCLUDED.storage_pinned, storage_price=EXCLUDED.storage_price, ingress_pinned=EXCLUDED.ingress_pinned,
ingress_price=EXCLUDED.ingress_price, egress_pinned=EXCLUDED.egress_pinned, egress_price=EXCLUDED.egress_price,
max_collateral_pinned=EXCLUDED.max_collateral_pinned, max_collateral=EXCLUDED.max_collateral;`

	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query, p.Currency, p.Threshold, p.Storage.Pinned, p.Storage.Value, p.Ingress.Pinned, p.Ingress.Value, p.Egress.Pinned, p.Egress.Value, p.MaxCollateral.Pinned, p.MaxCollateral.Value)
		return err
	})
}

// Settings returns the current host settings.
func (s *Store) Settings() (config settings.Settings, err error) {
	var dyndnsBuf []byte
	const query = `SELECT settings_revision, accepting_contracts, net_address,
	contract_price, base_rpc_price, sector_access_price, collateral_multiplier,
	max_collateral, storage_price, egress_price, ingress_price,
	max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size,
	ingress_limit, egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size,
	syncer_ingress_limit, syncer_egress_limit
FROM host_settings;`

	err = s.transaction(func(tx *txn) error {
		err = tx.QueryRow(query).Scan(&config.Revision, &config.AcceptingContracts,
			&config.NetAddress, decode(&config.ContractPrice),
			decode(&config.BaseRPCPrice), decode(&config.SectorAccessPrice),
			&config.CollateralMultiplier, decode(&config.MaxCollateral),
			decode(&config.StoragePrice), decode(&config.EgressPrice),
			decode(&config.IngressPrice), decode(&config.MaxAccountBalance),
			&config.AccountExpiry, &config.PriceTableValidity, &config.MaxContractDuration, &config.WindowSize,
			&config.IngressLimit, &config.EgressLimit, &config.MaxRegistryEntries,
			&config.DDNS.Provider, &config.DDNS.IPv4, &config.DDNS.IPv6, &dyndnsBuf, &config.SectorCacheSize,
			&config.SyncerIngressLimit, &config.SyncerEgressLimit)
		if errors.Is(err, sql.ErrNoRows) {
			return settings.ErrNoSettings
		}
		if dyndnsBuf != nil {
			err = json.Unmarshal(dyndnsBuf, &config.DDNS.Options)
			if err != nil {
				return fmt.Errorf("failed to unmarshal ddns options: %w", err)
			}
		}
		return nil
	})
	return
}

// UpdateSettings updates the host's stored settings.
func (s *Store) UpdateSettings(settings settings.Settings) error {
	const query = `INSERT INTO host_settings (id, settings_revision,
		accepting_contracts, net_address, contract_price, base_rpc_price,
		sector_access_price, collateral_multiplier, max_collateral, storage_price,
		egress_price, ingress_price, max_account_balance,
		max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit,
		egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size,
		syncer_ingress_limit, syncer_egress_limit)
		VALUES (0, 0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
ON CONFLICT (id) DO UPDATE SET (settings_revision,
	accepting_contracts, net_address, contract_price, base_rpc_price,
	sector_access_price, collateral_multiplier, max_collateral, storage_price,
	egress_price, ingress_price, max_account_balance,
	max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit,
	egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size,
	syncer_ingress_limit, syncer_egress_limit) = (
	settings_revision + 1, EXCLUDED.accepting_contracts, EXCLUDED.net_address,
	EXCLUDED.contract_price, EXCLUDED.base_rpc_price, EXCLUDED.sector_access_price,
	EXCLUDED.collateral_multiplier, EXCLUDED.max_collateral, EXCLUDED.storage_price,
	EXCLUDED.egress_price, EXCLUDED.ingress_price, EXCLUDED.max_account_balance,
	EXCLUDED.max_account_age, EXCLUDED.price_table_validity, EXCLUDED.max_contract_duration, EXCLUDED.window_size,
	EXCLUDED.ingress_limit, EXCLUDED.egress_limit, EXCLUDED.registry_limit, EXCLUDED.ddns_provider,
	EXCLUDED.ddns_update_v4, EXCLUDED.ddns_update_v6, EXCLUDED.ddns_opts, EXCLUDED.sector_cache_size,
	EXCLUDED.syncer_ingress_limit, EXCLUDED.syncer_egress_limit);`
	var dnsOptsBuf []byte
	if settings.DDNS.Provider != "" {
		var err error
		dnsOptsBuf, err = json.Marshal(settings.DDNS.Options)
		if err != nil {
			return fmt.Errorf("failed to marshal ddns options: %w", err)
		}
	}

	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query, settings.AcceptingContracts,
			settings.NetAddress, encode(settings.ContractPrice),
			encode(settings.BaseRPCPrice), encode(settings.SectorAccessPrice),
			settings.CollateralMultiplier, encode(settings.MaxCollateral),
			encode(settings.StoragePrice), encode(settings.EgressPrice),
			encode(settings.IngressPrice), encode(settings.MaxAccountBalance),
			settings.AccountExpiry, settings.PriceTableValidity, settings.MaxContractDuration, settings.WindowSize,
			settings.IngressLimit, settings.EgressLimit, settings.MaxRegistryEntries,
			settings.DDNS.Provider, settings.DDNS.IPv4, settings.DDNS.IPv6, dnsOptsBuf, settings.SectorCacheSize,
			settings.SyncerIngressLimit, settings.SyncerEgressLimit)
		if err != nil {
			return fmt.Errorf("failed to update settings: %w", err)
		}

		// update the currency stats
		timestamp := time.Now()
		if err := setCurrencyStat(tx, metricContractPrice, settings.ContractPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update contract price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricBaseRPCPrice, settings.BaseRPCPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update base RPC price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricSectorAccessPrice, settings.SectorAccessPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update sector access price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricStoragePrice, settings.StoragePrice, timestamp); err != nil {
			return fmt.Errorf("failed to update storage price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricEgressPrice, settings.EgressPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update egress price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricIngressPrice, settings.IngressPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update ingress price stat: %w", err)
		} else if err := setNumericStat(tx, metricMaxRegistryEntries, settings.MaxRegistryEntries, timestamp); err != nil {
			return fmt.Errorf("failed to update max registry entries stat: %w", err)
		} else if err := setFloat64Stat(tx, metricCollateralMultiplier, settings.CollateralMultiplier, timestamp); err != nil {
			return fmt.Errorf("failed to update collateral stat: %w", err)
		}
		return nil
	})
}

// HostKey returns the host's private key.
func (s *Store) HostKey() (pk types.PrivateKey) {
	err := s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT host_key FROM global_settings WHERE id=0;`).Scan(&pk)
	})
	if err != nil {
		s.log.Panic("failed to get host key", zap.Error(err), zap.Stack("stacktrace"))
	} else if n := len(pk); n != ed25519.PrivateKeySize {
		s.log.Panic("host key has incorrect length", zap.Int("expected", ed25519.PrivateKeySize), zap.Int("actual", n))
	}
	return
}

// LastAnnouncement returns the last announcement.
func (s *Store) LastAnnouncement() (ann settings.Announcement, err error) {
	var address sql.NullString

	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT last_announce_index, last_announce_address FROM global_settings`).
			Scan(decodeNullable(&ann.Index), &address)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return settings.Announcement{}, nil
	} else if address.Valid {
		ann.Address = address.String
	}
	return
}

// LastV2AnnouncementHash returns the hash of the last v2 announcement and the
// chain index it was confirmed in.
func (s *Store) LastV2AnnouncementHash() (h types.Hash256, index types.ChainIndex, err error) {
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow(`SELECT last_v2_announce_hash, last_announce_index FROM global_settings`).
			Scan(decodeNullable(&h), decodeNullable(&index))
	})
	if errors.Is(err, sql.ErrNoRows) {
		return types.Hash256{}, types.ChainIndex{}, nil
	}
	return
}

// UpdateLastAnnouncement updates the last announcement.
func (s *Store) UpdateLastAnnouncement(ann settings.Announcement) error {
	const query = `UPDATE global_settings SET last_announce_index=$1, last_announce_address=$2;`

	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query, encode(ann.Index), ann.Address)
		return err
	})
}

// RevertLastAnnouncement reverts the last announcement.
func (s *Store) RevertLastAnnouncement() error {
	const query = `UPDATE global_settings SET last_announce_index=NULL, last_announce_address=NULL, last_announce_key=NULL;`
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(query)
		return err
	})
}

// UpdateWalletHash updates the stored wallet hash.
func (s *Store) UpdateWalletHash(walletHash types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`UPDATE global_settings SET wallet_hash=?`, encode(walletHash))
		return err
	})
}
