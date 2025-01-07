package api

import (
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/prometheus"
)

// PrometheusMetric returns a Prometheus metric for the host state.
func (s State) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_host_state",
			Labels: map[string]any{
				"name":       s.Name,
				"public_key": s.PublicKey,
				"version":    s.Version,
				"commit":     s.Commit,
				"os":         s.OS,
				"build_time": s.BuildTime,
			},
			Value: 1,
		},
		{
			Name:  "hostd_start_time",
			Value: float64(s.StartTime.UTC().UnixMilli()),
		},
		{
			Name:      "hostd_runtime",
			Value:     float64(time.Since(s.StartTime).Milliseconds()),
			Timestamp: time.Now(),
		},
		{
			Name:   "hostd_last_announcement",
			Labels: map[string]any{"address": s.LastAnnouncement.Address, "id": s.LastAnnouncement.Index.ID},
			Value:  float64(s.LastAnnouncement.Index.Height),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host's consensus index.
func (cs ConsensusIndexResp) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_consensus_height",
			Labels: map[string]any{
				"block_id": cs.ID,
			},
			Value:     float64(cs.Height),
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host's index height.
func (cs IndexTipResp) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_index_height",
			Labels: map[string]any{
				"block_id": cs.ID,
			},
			Value:     float64(cs.Height),
			Timestamp: time.Now(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host's consensus state.
func (cs ConsensusStateResp) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_consensus_state_network",
			Labels: map[string]any{
				"name": cs.Network.Name,
			},
			Value: 1,
		},
		{
			Name: "hostd_consensus_state_height",
			Labels: map[string]any{
				"block_id": cs.Index.ID,
			},
			Value:     float64(cs.Index.Height),
			Timestamp: cs.PrevTimestamps[0],
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host settings.
func (hs HostSettings) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_settings",
			Labels: map[string]any{
				"net_address": hs.NetAddress,
			},
			Value: 1,
		},
		{
			Name: "hostd_settings_accepting_contracts",
			Value: func() float64 {
				if hs.AcceptingContracts {
					return 1
				}
				return 0
			}(),
		},
		{
			Name:  "hostd_settings_max_contract_duration",
			Value: float64(hs.MaxContractDuration),
		},
		{
			Name:  "hostd_settings_window_size",
			Value: float64(hs.WindowSize),
		},
		{
			Name:  "hostd_settings_collateral_multiplier",
			Value: hs.CollateralMultiplier,
		},
		{
			Name:  "hostd_settings_max_collateral",
			Value: hs.MaxCollateral.Siacoins(),
		},
		{
			Name:  "hostd_settings_pricetable_validity",
			Value: hs.PriceTableValidity.Seconds(),
		},
		{
			Name:  "hostd_settings_account_expiry",
			Value: hs.AccountExpiry.Seconds(),
		},
		{
			Name:  "hostd_settings_max_account_balance",
			Value: hs.MaxAccountBalance.Siacoins(),
		},
		{
			Name:  "hostd_settings_ingress_limit",
			Value: float64(hs.IngressLimit),
		},
		{
			Name:  "hostd_settings_egress_limit",
			Value: float64(hs.EgressLimit),
		},
		{
			Name:  "hostd_settings_sector_cache_size",
			Value: float64(hs.SectorCacheSize),
		},
		{
			Name:  "hostd_settings_revision",
			Value: float64(hs.Revision),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host metrics.
func (m Metrics) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name:  "hostd_metrics_accounts_active",
			Value: float64(m.Accounts.Active),
		},
		{
			Name:  "hostd_metrics_accounts_balance",
			Value: m.Accounts.Balance.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_potential_rpc",
			Value: m.Revenue.Potential.RPC.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_potential_storage",
			Value: m.Revenue.Potential.Storage.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_potential_ingress",
			Value: m.Revenue.Potential.Ingress.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_potential_egress",
			Value: m.Revenue.Potential.Egress.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_earned_rpc",
			Value: m.Revenue.Earned.RPC.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_earned_storage",
			Value: m.Revenue.Earned.Storage.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_earned_ingress",
			Value: m.Revenue.Earned.Ingress.Siacoins(),
		},
		{
			Name:  "hostd_metrics_revenue_earned_egress",
			Value: m.Revenue.Earned.Egress.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_contract_price",
			Value: m.Pricing.ContractPrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_ingress_price",
			Value: m.Pricing.IngressPrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_egress_price",
			Value: m.Pricing.EgressPrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_baserpc_price",
			Value: m.Pricing.BaseRPCPrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_sector_access_price",
			Value: m.Pricing.SectorAccessPrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_storage_price",
			Value: m.Pricing.StoragePrice.Siacoins(),
		},
		{
			Name:  "hostd_metrics_pricing_collateral_multiplier",
			Value: m.Pricing.CollateralMultiplier,
		},
		{
			Name:  "hostd_metrics_contracts_active",
			Value: float64(m.Contracts.Active),
		},
		{
			Name:  "hostd_metrics_contracts_rejected",
			Value: float64(m.Contracts.Rejected),
		},
		{
			Name:  "hostd_metrics_contracts_failed",
			Value: float64(m.Contracts.Failed),
		},
		{
			Name:  "hostd_metrics_contracts_successful",
			Value: float64(m.Contracts.Successful),
		},
		{
			Name:  "hostd_metrics_contracts_locked_collateral",
			Value: m.Contracts.LockedCollateral.Siacoins(),
		},
		{
			Name:  "hostd_metrics_contracts_risked_collateral",
			Value: m.Contracts.RiskedCollateral.Siacoins(),
		},
		{
			Name:  "hostd_metrics_storage_total_bytes",
			Value: float64(m.Storage.TotalSectors * rhp2.SectorSize),
		},
		{
			Name:  "hostd_metrics_storage_physical_used_bytes",
			Value: float64(m.Storage.PhysicalSectors * rhp2.SectorSize),
		},
		{
			Name:  "hostd_metrics_storage_contract_used_bytes",
			Value: float64(m.Storage.ContractSectors * rhp2.SectorSize),
		},
		{
			Name:  "hostd_metrics_storage_temp_used_bytes",
			Value: float64(m.Storage.TempSectors * rhp2.SectorSize),
		},
		{
			Name:  "hostd_metrics_storage_reads",
			Value: float64(m.Storage.Reads),
		},
		{
			Name:  "hostd_metrics_storage_writes",
			Value: float64(m.Storage.Writes),
		},
		{
			Name:  "hostd_metrics_storage_sector_cache_hits",
			Value: float64(m.Storage.SectorCacheHits),
		},
		{
			Name:  "hostd_metrics_storage_sector_cache_misses",
			Value: float64(m.Storage.SectorCacheMisses),
		},
		{
			Name:  "hostd_metrics_data_rhp_ingress",
			Value: float64(m.Data.RHP.Ingress),
		},
		{
			Name:  "hostd_metrics_data_rhp_egress",
			Value: float64(m.Data.RHP.Egress),
		},
		{
			Name:  "hostd_metrics_wallet_balance",
			Value: m.Wallet.Balance.Siacoins(),
		},
		{
			Name:  "hostd_metrics_wallet_immature_balance",
			Value: m.Wallet.ImmatureBalance.Siacoins(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host wallet.
func (wr WalletResponse) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_wallet_spendable",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value: wr.Spendable.Siacoins(),
		},
		{
			Name: "hostd_wallet_confirmed",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value: wr.Confirmed.Siacoins(),
		},
		{
			Name: "hostd_wallet_unconfirmed",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value: wr.Unconfirmed.Siacoins(),
		},
		{
			Name: "hostd_wallet_immature",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value: wr.Immature.Siacoins(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the hosts tpool fee.
func (t TPoolResp) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "hostd_tpool_fee",
			Value: types.Currency(t).Siacoins(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the hosts volumes.
func (v VolumeResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, volume := range v {
		labels := map[string]any{
			"id":         volume.ID,
			"local_path": volume.LocalPath,
		}
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_used_sectors",
			Labels: labels,
			Value:  float64(volume.UsedSectors),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_total_sectors",
			Labels: labels,
			Value:  float64(volume.TotalSectors),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_read_only",
			Labels: labels,
			Value: func() float64 {
				if volume.ReadOnly {
					return 1
				}
				return 0
			}(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_available",
			Labels: labels,
			Value: func() float64 {
				if volume.Available {
					return 1
				}
				return 0
			}(),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_failed_reads",
			Labels: labels,
			Value:  float64(volume.FailedReads),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_failed_writes",
			Labels: labels,
			Value:  float64(volume.FailedWrites),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_successful_reads",
			Labels: labels,
			Value:  float64(volume.SuccessfulReads),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_successful_writes",
			Labels: labels,
			Value:  float64(volume.SuccessfulWrites),
		})
		metrics = append(metrics, prometheus.Metric{
			Name:   "hostd_volume_status",
			Labels: labels,
			Value: func() float64 {
				if volume.Status == "creating" {
					return 1
				} else if volume.Status == "ready" {
					return 2
				}
				return 0
			}(),
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the hosts alerts.
func (a AlertResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, alert := range a {
		metrics = append(metrics, prometheus.Metric{
			Name: "hostd_alert",
			Labels: map[string]any{
				"id":       alert.ID,
				"severity": alert.Severity.String(),
				"message":  alert.Message,
			},
			Value:     1,
			Timestamp: alert.Timestamp,
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the hosts syncer peers.
func (p PeerResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, peer := range p {
		metrics = append(metrics, prometheus.Metric{
			Name: "hostd_syncer_peer",
			Labels: map[string]any{
				"address": string(peer.Address),
				"version": peer.Version,
			},
			Value: 1,
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the hosts syncer address.
func (s SyncerAddrResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metrics = append(metrics, prometheus.Metric{
		Name: "hostd_syncer_address",
		Labels: map[string]any{
			"address": string(s),
		},
		Value: 1,
	})
	return
}

// PrometheusMetric returns Prometheus samples for the hosts transactions.
func (w WalletTransactionsResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metricName := "hostd_wallet_event"
	for _, txn := range w {
		inflow, outflow := txn.SiacoinInflow(), txn.SiacoinOutflow()

		var value float64
		if inflow.Cmp(outflow) > 0 { // inflow > outflow = positive value
			value = inflow.Sub(outflow).Siacoins()
		} else { // inflow < outflow = negative value
			value = outflow.Sub(inflow).Siacoins() * -1
		}

		metrics = append(metrics, prometheus.Metric{
			Name: metricName,
			Labels: map[string]any{
				"txid": txn.ID.String(),
				"type": txn.Type,
			},
			Value: value,
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the host pending transactions.
func (w WalletPendingResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metricName := "hostd_wallet_event_pending"
	for _, txn := range w {
		inflow, outflow := txn.SiacoinInflow(), txn.SiacoinOutflow()
		var value float64
		if inflow.Cmp(outflow) > 0 { // inflow > outflow = positive value
			value = inflow.Sub(outflow).Siacoins()
		} else { // inflow < outflow = negative value
			value = outflow.Sub(inflow).Siacoins() * -1
		}

		metrics = append(metrics, prometheus.Metric{
			Name: metricName,
			Labels: map[string]any{
				"txid": txn.ID.String(),
				"type": txn.Type,
			},
			Value: value,
		})
	}
	return
}
