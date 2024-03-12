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
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

// PinnedSettings returns the host's pinned settings.
func (s *Store) PinnedSettings(context.Context) (pinned pin.PinnedSettings, err error) {
	const query = `SELECT currency, threshold, storage_pinned, storage_price, ingress_pinned, ingress_price, egress_pinned, egress_price, max_collateral_pinned, max_collateral
FROM host_pinned_settings;`

	err = s.queryRow(query).Scan(&pinned.Currency, &pinned.Threshold, &pinned.Storage.Pinned, &pinned.Storage.Value, &pinned.Ingress.Pinned, &pinned.Ingress.Value, &pinned.Egress.Pinned, &pinned.Egress.Value, &pinned.MaxCollateral.Pinned, &pinned.MaxCollateral.Value)
	if errors.Is(err, sql.ErrNoRows) {
		return pin.PinnedSettings{}, nil
	}
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
	_, err := s.exec(query, p.Currency, p.Threshold, p.Storage.Pinned, p.Storage.Value, p.Ingress.Pinned, p.Ingress.Value, p.Egress.Pinned, p.Egress.Value, p.MaxCollateral.Pinned, p.MaxCollateral.Value)
	return err
}

// Settings returns the current host settings.
func (s *Store) Settings() (config settings.Settings, err error) {
	var dyndnsBuf []byte
	const query = `SELECT settings_revision, accepting_contracts, net_address, 
	contract_price, base_rpc_price, sector_access_price, collateral_multiplier, 
	max_collateral, storage_price, egress_price, ingress_price, 
	max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, 
	ingress_limit, egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size
FROM host_settings;`
	err = s.queryRow(query).Scan(&config.Revision, &config.AcceptingContracts,
		&config.NetAddress, (*sqlCurrency)(&config.ContractPrice),
		(*sqlCurrency)(&config.BaseRPCPrice), (*sqlCurrency)(&config.SectorAccessPrice),
		&config.CollateralMultiplier, (*sqlCurrency)(&config.MaxCollateral),
		(*sqlCurrency)(&config.StoragePrice), (*sqlCurrency)(&config.EgressPrice),
		(*sqlCurrency)(&config.IngressPrice), (*sqlCurrency)(&config.MaxAccountBalance),
		&config.AccountExpiry, &config.PriceTableValidity, &config.MaxContractDuration, &config.WindowSize,
		&config.IngressLimit, &config.EgressLimit, &config.MaxRegistryEntries,
		&config.DDNS.Provider, &config.DDNS.IPv4, &config.DDNS.IPv6, &dyndnsBuf, &config.SectorCacheSize)
	if errors.Is(err, sql.ErrNoRows) {
		return settings.Settings{}, settings.ErrNoSettings
	}
	if dyndnsBuf != nil {
		err = json.Unmarshal(dyndnsBuf, &config.DDNS.Options)
		if err != nil {
			return settings.Settings{}, fmt.Errorf("failed to unmarshal ddns options: %w", err)
		}
	}
	return
}

// UpdateSettings updates the host's stored settings.
func (s *Store) UpdateSettings(settings settings.Settings) error {
	const query = `INSERT INTO host_settings (id, settings_revision, 
		accepting_contracts, net_address, contract_price, base_rpc_price, 
		sector_access_price, collateral_multiplier, max_collateral, storage_price, 
		egress_price, ingress_price, max_account_balance, 
		max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
		egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size) 
		VALUES (0, 0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) 
ON CONFLICT (id) DO UPDATE SET (settings_revision, 
	accepting_contracts, net_address, contract_price, base_rpc_price, 
	sector_access_price, collateral_multiplier, max_collateral, storage_price, 
	egress_price, ingress_price, max_account_balance, 
	max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
	egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts, sector_cache_size) = (
	settings_revision + 1, EXCLUDED.accepting_contracts, EXCLUDED.net_address,
	EXCLUDED.contract_price, EXCLUDED.base_rpc_price, EXCLUDED.sector_access_price,
	EXCLUDED.collateral_multiplier, EXCLUDED.max_collateral, EXCLUDED.storage_price,
	EXCLUDED.egress_price, EXCLUDED.ingress_price, EXCLUDED.max_account_balance,
	EXCLUDED.max_account_age, EXCLUDED.price_table_validity, EXCLUDED.max_contract_duration, EXCLUDED.window_size, 
	EXCLUDED.ingress_limit, EXCLUDED.egress_limit, EXCLUDED.registry_limit, EXCLUDED.ddns_provider, 
	EXCLUDED.ddns_update_v4, EXCLUDED.ddns_update_v6, EXCLUDED.ddns_opts, EXCLUDED.sector_cache_size);`
	var dnsOptsBuf []byte
	if len(settings.DDNS.Provider) > 0 {
		var err error
		dnsOptsBuf, err = json.Marshal(settings.DDNS.Options)
		if err != nil {
			return fmt.Errorf("failed to marshal ddns options: %w", err)
		}
	}

	return s.transaction(func(tx txn) error {
		_, err := tx.Exec(query, settings.AcceptingContracts,
			settings.NetAddress, sqlCurrency(settings.ContractPrice),
			sqlCurrency(settings.BaseRPCPrice), sqlCurrency(settings.SectorAccessPrice),
			settings.CollateralMultiplier, sqlCurrency(settings.MaxCollateral),
			sqlCurrency(settings.StoragePrice), sqlCurrency(settings.EgressPrice),
			sqlCurrency(settings.IngressPrice), sqlCurrency(settings.MaxAccountBalance),
			settings.AccountExpiry, settings.PriceTableValidity, settings.MaxContractDuration, settings.WindowSize,
			settings.IngressLimit, settings.EgressLimit, settings.MaxRegistryEntries,
			settings.DDNS.Provider, settings.DDNS.IPv4, settings.DDNS.IPv6, dnsOptsBuf, settings.SectorCacheSize)
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
	err := s.queryRow(`SELECT host_key FROM global_settings WHERE id=0;`).Scan(&pk)
	if err != nil {
		s.log.Panic("failed to get host key", zap.Error(err), zap.Stack("stacktrace"))
	} else if n := len(pk); n != ed25519.PrivateKeySize {
		s.log.Panic("host key has incorrect length", zap.Int("expected", ed25519.PrivateKeySize), zap.Int("actual", n))
	}
	return
}

// LastAnnouncement returns the last announcement.
func (s *Store) LastAnnouncement() (ann settings.Announcement, err error) {
	var height sql.NullInt64
	var address sql.NullString
	err = s.queryRow(`SELECT last_announce_id, last_announce_height, last_announce_address, last_announce_key FROM global_settings`).
		Scan(nullable((*sqlHash256)(&ann.Index.ID)), &height, &address, nullable((*sqlHash256)(&ann.PublicKey)))
	if errors.Is(err, sql.ErrNoRows) {
		return settings.Announcement{}, nil
	}

	if height.Valid {
		ann.Index.Height = uint64(height.Int64)
	}
	if address.Valid {
		ann.Address = address.String
	}
	return
}

// UpdateLastAnnouncement updates the last announcement.
func (s *Store) UpdateLastAnnouncement(ann settings.Announcement) error {
	const query = `UPDATE global_settings SET 
last_announce_id=$1, last_announce_height=$2, last_announce_address=$3, last_announce_key=$4;`
	_, err := s.exec(query, sqlHash256(ann.Index.ID), ann.Index.Height, ann.Address, sqlHash256(ann.PublicKey))
	return err
}

// RevertLastAnnouncement reverts the last announcement.
func (s *Store) RevertLastAnnouncement() error {
	const query = `UPDATE global_settings SET
last_announce_id=NULL, last_announce_height=NULL, last_announce_address=NULL, last_announce_key=NULL;`
	_, err := s.exec(query)
	return err
}

// LastSettingsConsensusChange returns the last processed consensus change ID of
// the settings manager
func (s *Store) LastSettingsConsensusChange() (cc modules.ConsensusChangeID, height uint64, err error) {
	var nullHeight sql.NullInt64
	n := nullable((*sqlHash256)(&cc))
	err = s.queryRow(`SELECT settings_last_processed_change, settings_height FROM global_settings WHERE id=0;`).Scan(n, &nullHeight)
	if errors.Is(err, sql.ErrNoRows) || !n.Valid {
		return modules.ConsensusChangeRecent, 0, nil // as a special case don't scan the chain for new announcements
	} else if nullHeight.Valid {
		height = uint64(nullHeight.Int64)
	}
	return
}
