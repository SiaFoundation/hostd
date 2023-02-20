package sqlite

import (
	"database/sql"
	"errors"

	"go.sia.tech/hostd/host/settings"
)

// Settings returns the current host settings.
func (s *Store) Settings() (config settings.Settings, err error) {
	const query = `SELECT settings_revision, accepting_contracts, net_address, 
	contract_price, base_rpc_price, sector_access_price, collateral, 
	max_collateral, min_storage_price, min_egress_price, min_ingress_price, 
	max_account_balance, max_account_age, max_contract_duration, 
	ingress_limit, egress_limit
FROM host_settings;`
	err = s.db.QueryRow(query).Scan(&config.Revision, &config.AcceptingContracts,
		&config.NetAddress, (*sqlCurrency)(&config.ContractPrice),
		(*sqlCurrency)(&config.BaseRPCPrice), (*sqlCurrency)(&config.SectorAccessPrice),
		(*sqlCurrency)(&config.Collateral), (*sqlCurrency)(&config.MaxCollateral),
		(*sqlCurrency)(&config.MinStoragePrice), (*sqlCurrency)(&config.MinEgressPrice),
		(*sqlCurrency)(&config.MinIngressPrice), (*sqlCurrency)(&config.MaxAccountBalance),
		&config.AccountExpiry, &config.MaxContractDuration, &config.IngressLimit,
		&config.EgressLimit)
	if errors.Is(err, sql.ErrNoRows) {
		return settings.Settings{}, settings.ErrNoSettings
	}
	return
}

// UpdateSettings updates the host's stored settings.
func (s *Store) UpdateSettings(settings settings.Settings) error {
	const query = `INSERT INTO host_settings (settings_revision, 
		accepting_contracts, net_address, contract_price, base_rpc_price, 
		sector_access_price, collateral, max_collateral, min_storage_price, 
		min_egress_price, min_ingress_price, max_account_balance, 
		max_account_age, max_contract_duration, ingress_limit, egress_limit) 
		VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
ON CONFLICT (id) DO UPDATE SET settings_revision=settings_revision+1, 
	accepting_contracts=excluded.accepting_contracts, 
	net_address=excluded.net_address, contract_price=excluded.contract_price, 
	base_rpc_price=excluded.base_rpc_price, 
	sector_access_price=excluded.sector_access_price, collateral=excluded.collateral,
	max_collateral=excluded.max_collateral, min_storage_price=excluded.min_storage_price, 
	min_egress_price=excluded.min_egress_price, min_ingress_price=excluded.min_ingress_price,
	max_account_balance=excluded.max_account_balance, max_account_age=excluded.max_account_age, 
	max_contract_duration=excluded.max_contract_duration, ingress_limit=excluded.ingress_limit, 
	egress_limit=excluded.egress_limit`

	_, err := s.db.Exec(query, settings.AcceptingContracts,
		settings.NetAddress, sqlCurrency(settings.ContractPrice),
		sqlCurrency(settings.BaseRPCPrice), sqlCurrency(settings.SectorAccessPrice),
		sqlCurrency(settings.Collateral), sqlCurrency(settings.MaxCollateral),
		sqlCurrency(settings.MinStoragePrice), sqlCurrency(settings.MinEgressPrice),
		sqlCurrency(settings.MinIngressPrice), sqlCurrency(settings.MaxAccountBalance),
		settings.AccountExpiry, settings.MaxContractDuration,
		settings.IngressLimit, settings.EgressLimit)
	return err
}
