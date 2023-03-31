package sqlite

import (
	"crypto/ed25519"
	"database/sql"
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.uber.org/zap"
)

// Settings returns the current host settings.
func (s *Store) Settings() (config settings.Settings, err error) {
	const query = `SELECT settings_revision, accepting_contracts, net_address, 
	contract_price, base_rpc_price, sector_access_price, collateral, 
	max_collateral, min_storage_price, min_egress_price, min_ingress_price, 
	max_account_balance, max_account_age, max_contract_duration, window_size, 
	ingress_limit, egress_limit, registry_limit
FROM host_settings;`
	err = s.queryRow(query).Scan(&config.Revision, &config.AcceptingContracts,
		&config.NetAddress, (*sqlCurrency)(&config.ContractPrice),
		(*sqlCurrency)(&config.BaseRPCPrice), (*sqlCurrency)(&config.SectorAccessPrice),
		(*sqlCurrency)(&config.Collateral), (*sqlCurrency)(&config.MaxCollateral),
		(*sqlCurrency)(&config.MinStoragePrice), (*sqlCurrency)(&config.MinEgressPrice),
		(*sqlCurrency)(&config.MinIngressPrice), (*sqlCurrency)(&config.MaxAccountBalance),
		&config.AccountExpiry, &config.MaxContractDuration, &config.WindowSize,
		&config.IngressLimit, &config.EgressLimit, &config.MaxRegistryEntries)
	if errors.Is(err, sql.ErrNoRows) {
		return settings.Settings{}, settings.ErrNoSettings
	}
	return
}

// UpdateSettings updates the host's stored settings.
func (s *Store) UpdateSettings(settings settings.Settings) error {
	const query = `INSERT INTO host_settings (id, settings_revision, 
		accepting_contracts, net_address, contract_price, base_rpc_price, 
		sector_access_price, collateral, max_collateral, min_storage_price, 
		min_egress_price, min_ingress_price, max_account_balance, 
		max_account_age, max_contract_duration, window_size, ingress_limit, 
		egress_limit, registry_limit) 
		VALUES (0, 0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) 
ON CONFLICT (id) DO UPDATE SET (settings_revision, 
	accepting_contracts, net_address, contract_price, base_rpc_price, 
	sector_access_price, collateral, max_collateral, min_storage_price, 
	min_egress_price, min_ingress_price, max_account_balance, 
	max_account_age, max_contract_duration, window_size, ingress_limit, 
	egress_limit, registry_limit) = (
	settings_revision + 1, EXCLUDED.accepting_contracts, EXCLUDED.net_address,
	EXCLUDED.contract_price, EXCLUDED.base_rpc_price, EXCLUDED.sector_access_price,
	EXCLUDED.collateral, EXCLUDED.max_collateral, EXCLUDED.min_storage_price,
	EXCLUDED.min_egress_price, EXCLUDED.min_ingress_price, EXCLUDED.max_account_balance,
	EXCLUDED.max_account_age, EXCLUDED.max_contract_duration, EXCLUDED.window_size, 
	EXCLUDED.ingress_limit, EXCLUDED.egress_limit, EXCLUDED.registry_limit);`

	_, err := s.exec(query, settings.AcceptingContracts,
		settings.NetAddress, sqlCurrency(settings.ContractPrice),
		sqlCurrency(settings.BaseRPCPrice), sqlCurrency(settings.SectorAccessPrice),
		sqlCurrency(settings.Collateral), sqlCurrency(settings.MaxCollateral),
		sqlCurrency(settings.MinStoragePrice), sqlCurrency(settings.MinEgressPrice),
		sqlCurrency(settings.MinIngressPrice), sqlCurrency(settings.MaxAccountBalance),
		settings.AccountExpiry, settings.MaxContractDuration, settings.WindowSize,
		settings.IngressLimit, settings.EgressLimit, settings.MaxRegistryEntries)
	return err
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
