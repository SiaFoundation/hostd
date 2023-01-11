package sqlite

import (
	"database/sql"
	"errors"

	"go.sia.tech/hostd/host/settings"
)

// SettingsStore is a store for host settings.
type SettingsStore struct {
	db *Store
}

// Settings returns the current host settings.
func (ss *SettingsStore) Settings() (s settings.Settings, err error) {
	const query = `SELECT settings_revision, accepting_contracts, net_address, 
	contract_price, base_rpc_price, sector_access_price, collateral, 
	max_collateral, min_storage_price, min_egress_price, min_ingress_price, 
	max_account_balance, max_account_age, max_contract_duration, 
	ingress_limit, egress_limit
FROM host_settings;`
	err = ss.db.db.QueryRow(query).Scan(&s.Revision, &s.AcceptingContracts,
		&s.NetAddress, scanCurrency(&s.ContractPrice),
		scanCurrency(&s.BaseRPCPrice), scanCurrency(&s.SectorAccessPrice),
		scanCurrency(&s.Collateral), scanCurrency(&s.MaxCollateral),
		scanCurrency(&s.MinStoragePrice), scanCurrency(&s.MinEgressPrice),
		scanCurrency(&s.MinIngressPrice), scanCurrency(&s.MaxAccountBalance),
		&s.AccountExpiry, &s.MaxContractDuration, &s.IngressLimit,
		&s.EgressLimit)
	if errors.Is(err, sql.ErrNoRows) {
		return s, settings.ErrNoSettings
	}
	return
}

// UpdateSettings updates the host's stored settings.
func (ss *SettingsStore) UpdateSettings(settings settings.Settings) error {
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

	_, err := ss.db.db.Exec(query, settings.AcceptingContracts,
		settings.NetAddress, valueCurrency(settings.ContractPrice),
		valueCurrency(settings.BaseRPCPrice), valueCurrency(settings.SectorAccessPrice),
		valueCurrency(settings.Collateral), valueCurrency(settings.MaxCollateral),
		valueCurrency(settings.MinStoragePrice), valueCurrency(settings.MinEgressPrice),
		valueCurrency(settings.MinIngressPrice), valueCurrency(settings.MaxAccountBalance),
		settings.AccountExpiry, settings.MaxContractDuration,
		settings.IngressLimit, settings.EgressLimit)
	return err
}

// NewSettingsStore creates a new SettingsStore using the provided database.
func NewSettingsStore(db *Store) *SettingsStore {
	return &SettingsStore{
		db: db,
	}
}
