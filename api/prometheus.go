package api

import (
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/hostd/internal/prometheus"
)

// PrometheusMetric returns a Prometheus metric for the host state.
func (hs HostState) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_host_state",
			Labels: map[string]any{
				"name":           hs.Name,
				"public_key":     hs.PublicKey,
				"wallet_address": hs.WalletAddress,
				"network":        hs.Network,
				"version":        hs.Version,
				"commit":         hs.Commit,
				"os":             hs.OS,
				"build_time":     hs.BuildTime,
			},
		},
		{
			Name:      "hostd_start_time",
			Value:     1,
			Timestamp: hs.StartTime,
		},
		{
			Name:      "hostd_last_announcement",
			Labels:    map[string]any{"address": hs.LastAnnouncement.Address, "id": hs.LastAnnouncement.Index.ID},
			Value:     float64(hs.LastAnnouncement.Index.Height),
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns a Prometheus metric for the consensus state.
func (cs ConsensusState) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_consensus_state_synced",
			Value: func() float64 {
				if cs.Synced {
					return 1
				}
				return 0
			}(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_consensus_state_index",
			Labels:    map[string]any{"id": cs.ChainIndex.ID},
			Value:     float64(cs.ChainIndex.Height),
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host settings.
func (hs HostSettings) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_settings",
			Labels: map[string]any{
				"accepting_contracts": hs.AcceptingContracts,
			},
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_max_contract_duration",
			Value:     float64(hs.MaxContractDuration),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_contract_price",
			Value:     hs.ContractPrice.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_base_rpc_price",
			Value:     hs.BaseRPCPrice.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_sector_access_price",
			Value:     hs.SectorAccessPrice.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_storage_price",
			Value:     hs.StoragePrice.Mul64(1e12).Mul64(4320).Siacoins(), // price * 1TB * 4320 blocks per month
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_ingress_price",
			Value:     hs.IngressPrice.Mul64(1e12).Siacoins(), // price * 1TB
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_settings_egress_price",
			Value:     hs.EgressPrice.Mul64(1e12).Siacoins(), // price * 1TB
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host metrics.
func (m Metrics) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name:      "host_metrics_accounts_active",
			Value:     float64(m.Accounts.Active),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_accounts_balance",
			Value:     m.Accounts.Balance.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_potential_rpc",
			Value:     m.Revenue.Potential.RPC.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_potential_storage",
			Value:     m.Revenue.Potential.Storage.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_potential_ingress",
			Value:     m.Revenue.Potential.Ingress.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_potential_egress",
			Value:     m.Revenue.Potential.Egress.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_earned_rpc",
			Value:     m.Revenue.Earned.RPC.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_earned_storage",
			Value:     m.Revenue.Earned.Storage.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_earned_ingress",
			Value:     m.Revenue.Earned.Ingress.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_revenue_earned_egress",
			Value:     m.Revenue.Earned.Egress.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_active",
			Value:     float64(m.Contracts.Active),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_pending",
			Value:     float64(m.Contracts.Pending),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_rejected",
			Value:     float64(m.Contracts.Rejected),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_failed",
			Value:     float64(m.Contracts.Failed),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_successful",
			Value:     float64(m.Contracts.Successful),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_locked_collateral",
			Value:     m.Contracts.LockedCollateral.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_metrics_contracts_risked_collateral",
			Value:     m.Contracts.RiskedCollateral.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_total_bytes",
			Value:     float64(m.Storage.TotalSectors * rhp2.SectorSize),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_physical_used_bytes",
			Value:     float64(m.Storage.PhysicalSectors * rhp2.SectorSize),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_contract_used_bytes",
			Value:     float64(m.Storage.ContractSectors * rhp2.SectorSize),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_temp_used_bytes",
			Value:     float64(m.Storage.TempSectors * rhp2.SectorSize),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_reads",
			Value:     float64(m.Storage.Reads),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_storage_writes",
			Value:     float64(m.Storage.Writes),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_data_rhp_ingress_bytes",
			Value:     float64(m.Data.RHP.Ingress),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_data_rhp_egress_bytes",
			Value:     float64(m.Data.RHP.Egress),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_wallet_balance",
			Value:     m.Balance.Siacoins(),
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host wallet.
func (wr WalletResponse) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_wallet",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value:     float64(wr.ScanHeight),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_wallet_spendable",
			Value:     wr.Spendable.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_wallet_confirmed",
			Value:     wr.Confirmed.Siacoins(),
			Timestamp: time.Now(),
		},
		{
			Name:      "hostd_wallet_unconfirmed",
			Value:     wr.Unconfirmed.Siacoins(),
			Timestamp: time.Now(),
		},
	}
}
