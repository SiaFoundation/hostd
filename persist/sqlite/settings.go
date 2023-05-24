package sqlite

import (
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.uber.org/zap"
)

// Settings returns the current host settings.
func (s *Store) Settings() (config settings.Settings, err error) {
	var dyndnsBuf []byte
	const query = `SELECT settings_revision, accepting_contracts, net_address, 
	contract_price, base_rpc_price, sector_access_price, collateral, 
	max_collateral, storage_price, egress_price, ingress_price, 
	max_account_balance, max_account_age, price_table_validity, max_contract_duration, window_size, 
	ingress_limit, egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts
FROM host_settings;`
	err = s.queryRow(query).Scan(&config.Revision, &config.AcceptingContracts,
		&config.NetAddress, (*sqlCurrency)(&config.ContractPrice),
		(*sqlCurrency)(&config.BaseRPCPrice), (*sqlCurrency)(&config.SectorAccessPrice),
		(*sqlCurrency)(&config.Collateral), (*sqlCurrency)(&config.MaxCollateral),
		(*sqlCurrency)(&config.StoragePrice), (*sqlCurrency)(&config.EgressPrice),
		(*sqlCurrency)(&config.IngressPrice), (*sqlCurrency)(&config.MaxAccountBalance),
		&config.AccountExpiry, &config.PriceTableValidity, &config.MaxContractDuration, &config.WindowSize,
		&config.IngressLimit, &config.EgressLimit, &config.MaxRegistryEntries,
		&config.DDNS.Provider, &config.DDNS.IPv4, &config.DDNS.IPv6, &dyndnsBuf)
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
		sector_access_price, collateral, max_collateral, storage_price, 
		egress_price, ingress_price, max_account_balance, 
		max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
		egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts) 
		VALUES (0, 0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22) 
ON CONFLICT (id) DO UPDATE SET (settings_revision, 
	accepting_contracts, net_address, contract_price, base_rpc_price, 
	sector_access_price, collateral, max_collateral, storage_price, 
	egress_price, ingress_price, max_account_balance, 
	max_account_age, price_table_validity, max_contract_duration, window_size, ingress_limit, 
	egress_limit, registry_limit, ddns_provider, ddns_update_v4, ddns_update_v6, ddns_opts) = (
	settings_revision + 1, EXCLUDED.accepting_contracts, EXCLUDED.net_address,
	EXCLUDED.contract_price, EXCLUDED.base_rpc_price, EXCLUDED.sector_access_price,
	EXCLUDED.collateral, EXCLUDED.max_collateral, EXCLUDED.storage_price,
	EXCLUDED.egress_price, EXCLUDED.ingress_price, EXCLUDED.max_account_balance,
	EXCLUDED.max_account_age, EXCLUDED.price_table_validity, EXCLUDED.max_contract_duration, EXCLUDED.window_size, 
	EXCLUDED.ingress_limit, EXCLUDED.egress_limit, EXCLUDED.registry_limit, EXCLUDED.ddns_provider, 
	EXCLUDED.ddns_update_v4, EXCLUDED.ddns_update_v6, EXCLUDED.ddns_opts);`
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
			sqlCurrency(settings.Collateral), sqlCurrency(settings.MaxCollateral),
			sqlCurrency(settings.StoragePrice), sqlCurrency(settings.EgressPrice),
			sqlCurrency(settings.IngressPrice), sqlCurrency(settings.MaxAccountBalance),
			settings.AccountExpiry, settings.PriceTableValidity, settings.MaxContractDuration, settings.WindowSize,
			settings.IngressLimit, settings.EgressLimit, settings.MaxRegistryEntries,
			settings.DDNS.Provider, settings.DDNS.IPv4, settings.DDNS.IPv6, dnsOptsBuf)
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
		} else if err := setCurrencyStat(tx, metricCollateral, settings.Collateral, timestamp); err != nil {
			return fmt.Errorf("failed to update collateral stat: %w", err)
		} else if err := setCurrencyStat(tx, metricStoragePrice, settings.StoragePrice, timestamp); err != nil {
			return fmt.Errorf("failed to update storage price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricEgressPrice, settings.EgressPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update egress price stat: %w", err)
		} else if err := setCurrencyStat(tx, metricIngressPrice, settings.IngressPrice, timestamp); err != nil {
			return fmt.Errorf("failed to update ingress price stat: %w", err)
		} else if err := setNumericStat(tx, metricMaxRegistryEntries, settings.MaxRegistryEntries, timestamp); err != nil {
			return fmt.Errorf("failed to update max registry entries stat: %w", err)
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
