package api

import (
	"strings"
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
			Value: 1,
		},
		{
			Name:  "hostd_start_time",
			Value: float64(hs.StartTime.UTC().UnixMilli()),
		},
		{
			Name:      "hostd_runtime",
			Value:     float64(time.Since(hs.StartTime).Milliseconds()),
			Timestamp: time.Now(),
		},
		{
			Name:   "hostd_last_announcement",
			Labels: map[string]any{"address": hs.LastAnnouncement.Address, "id": hs.LastAnnouncement.Index.ID},
			Value:  float64(hs.LastAnnouncement.Index.Height),
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
		},
		{
			Name:  "hostd_consensus_state_chain_index_height",
			Value: float64(cs.ChainIndex.Height),
		},
		{
			Name: "hostd_consensus_state_chain_index_height_B", //TODO: grafana cannot simultaneously display block height and colorize based of this synced label. the bustedware/grafana repo is working on a forked grafana that will one day make this work
			Labels: map[string]any{"synced": func() float64 {
				if cs.Synced {
					return 1
				}
				return 0
			}()},
			Value: float64(cs.ChainIndex.Height),
		},
		{
			Name: "hostd_consensus_state_chain_index_height_C", //TODO: i don't believe including id is efficient use of labels. confirm
			Labels: map[string]any{"synced": func() float64 {
				if cs.Synced {
					return 1
				}
				return 0
			}(), "id": cs.ChainIndex.ID},
			Value: float64(cs.ChainIndex.Height),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host settings.
func (hs HostSettings) PrometheusMetric() []prometheus.Metric {
	labels := make(map[string]interface{}, 2)
	if hs.NetAddress != "" {
		labels["net_address"] = hs.NetAddress
	}
	if hs.DDNS.Provider != "" {
		labels["provaider"] = hs.DDNS.Provider
	}
	return []prometheus.Metric{
		{
			Name:   "hostd_settings_accepting_contracts",
			Labels: labels,
			Value: func() float64 {
				if hs.AcceptingContracts {
					return 1
				}
				return 0
			}(),
		},
		{
			Name:   "hostd_settings_max_contract_duration",
			Labels: labels,
			Value:  float64(hs.MaxContractDuration),
		},
		{
			Name:   "hostd_settings_window_size",
			Labels: labels,
			Value:  float64(hs.WindowSize),
		},
		{
			Name:   "hostd_settings_collateral_multiplier",
			Labels: labels,
			Value:  hs.CollateralMultiplier,
		},
		{
			Name:   "hostd_settings_max_collateral",
			Labels: labels,
			Value:  hs.MaxCollateral.Siacoins(),
		},
		{
			Name:   "hostd_settings_pricetable_validity",
			Labels: labels,
			Value:  hs.PriceTableValidity.Hours(),
		},
		{
			Name:   "hostd_settings_account_expiry",
			Labels: labels,
			Value:  hs.AccountExpiry.Hours(),
		},
		{
			Name:   "hostd_settings_max_account_balance",
			Labels: labels,
			Value:  hs.MaxAccountBalance.Siacoins(),
		},
		{
			Name:   "hostd_settings_ingress_limit",
			Labels: labels,
			Value:  float64(hs.IngressLimit),
		},
		{
			Name:   "hostd_settings_egress_limit",
			Labels: labels,
			Value:  float64(hs.EgressLimit),
		},
		{
			Name:   "hostd_settings_ddns_ipv4",
			Labels: labels,
			Value: func() float64 {
				if hs.DDNS.IPv4 {
					return 1
				}
				return 0
			}(),
		},
		{
			Name:   "hostd_settings_ddns_ipv6",
			Labels: labels,
			Value: func() float64 {
				if hs.DDNS.IPv6 {
					return 1
				}
				return 0
			}(),
		},
		{
			Name:   "hostd_settings_sector_cache_size",
			Labels: labels,
			Value:  float64(hs.SectorCacheSize),
		},
		{
			Name:   "hostd_settings_revision",
			Labels: labels,
			Value:  float64(hs.Revision),
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
			Name:  "hostd_metrics_contracts_pending",
			Value: float64(m.Contracts.Pending),
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
			Name:  "hostd_metrics_balance",
			Value: m.Balance.Siacoins(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the host wallet.
func (wr WalletResponse) PrometheusMetric() []prometheus.Metric {
	return []prometheus.Metric{
		{
			Name: "hostd_wallet_scan_height",
			Labels: map[string]any{
				"address": wr.Address,
			},
			Value: float64(wr.ScanHeight),
		},
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
	}
}

// PrometheusMetric returns Prometheus samples for the hosts tpool fee.
func (t TPoolResp) PrometheusMetric() (metrics []prometheus.Metric) {
	return []prometheus.Metric{
		{
			Name:  "hostd_tpool_fee",
			Value: t.RecommendedFee.Siacoins(),
		},
	}
}

// PrometheusMetric returns Prometheus samples for the hosts volumes.
func (v VolumeResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, volume := range v {
		labels := make(map[string]interface{}, 2)
		labels["id"] = volume.ID
		labels["local_path"] = volume.LocalPath
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
				} else {
					return 0
				}
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
				"severity": alert.Severity.String(),
				"message":  alert.Message,
			},
			Value: 1,
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
func (s SyncAddrResp) PrometheusMetric() (metrics []prometheus.Metric) {
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
	metricName := "hostd_wallet_transaction"
	for _, txn := range w {
		var value float64
		if txn.Inflow.Cmp(txn.Outflow) > 0 { // inflow > outflow = positive value
			value = txn.Inflow.Sub(txn.Outflow).Siacoins()
		} else { // inflow < outflow = negative value
			value = txn.Outflow.Sub(txn.Inflow).Siacoins() * -1
		}

		metrics = append(metrics, prometheus.Metric{
			Name: metricName,
			Labels: map[string]any{
				"txid": strings.Split(txn.ID.String(), ":")[1],
			},
			Value: value,
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the host pending transactions.
func (w WalletPendingResp) PrometheusMetric() (metrics []prometheus.Metric) {
	metricName := "hostd_wallet_transaction_pending"
	for _, txn := range w {
		var value float64
		if txn.Inflow.Cmp(txn.Outflow) > 0 { // inflow > outflow = positive value
			value = txn.Inflow.Sub(txn.Outflow).Siacoins()
		} else { // inflow < outflow = negative value
			value = txn.Outflow.Sub(txn.Inflow).Siacoins() * -1
		}

		metrics = append(metrics, prometheus.Metric{
			Name: metricName,
			Labels: map[string]any{
				"txid": strings.Split(txn.ID.String(), ":")[1],
			},
			Value: value,
		})
	}
	return
}

// PrometheusMetric returns Prometheus samples for the hosts sessions
func (s SessionResp) PrometheusMetric() (metrics []prometheus.Metric) {
	for _, session := range s {
		metrics = append(metrics, prometheus.Metric{
			Name: "hostd_session_ingress",
			Labels: map[string]any{
				"peer": session.PeerAddress,
			},
			Value: float64(session.Ingress),
		})
		metrics = append(metrics, prometheus.Metric{
			Name: "hostd_session_egress",
			Labels: map[string]any{
				"peer": session.PeerAddress,
			},
			Value: float64(session.Egress),
		})
	}
	return
}
