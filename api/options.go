package api

import "go.uber.org/zap"

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// ServerWithAlerts sets the alerts manager for the API server.
func ServerWithAlerts(al Alerts) ServerOption {
	return func(a *api) {
		a.alerts = al
	}
}

// ServerWithWebHooks sets the webhooks manager for the API server.
func ServerWithWebHooks(w WebHooks) ServerOption {
	return func(a *api) {
		a.webhooks = w
	}
}

// ServerWithSyncer sets the syncer for the API server.
func ServerWithSyncer(g Syncer) ServerOption {
	return func(a *api) {
		a.syncer = g
	}
}

// ServerWithChainManager sets the chain manager for the API server.
func ServerWithChainManager(chain ChainManager) ServerOption {
	return func(a *api) {
		a.chain = chain
	}
}

// ServerWithTransactionPool sets the transaction pool for the API server.
func ServerWithTransactionPool(tp TPool) ServerOption {
	return func(a *api) {
		a.tpool = tp
	}
}

// ServerWithContractManager sets the contract manager for the API server.
func ServerWithContractManager(cm ContractManager) ServerOption {
	return func(a *api) {
		a.contracts = cm
	}
}

// ServerWithAccountManager sets the account manager for the API server.
func ServerWithAccountManager(am AccountManager) ServerOption {
	return func(a *api) {
		a.accounts = am
	}
}

// ServerWithVolumeManager sets the volume manager for the API server.
func ServerWithVolumeManager(vm VolumeManager) ServerOption {
	return func(a *api) {
		a.volumes = vm
	}
}

// ServerWithMetricManager sets the metric manager for the API server.
func ServerWithMetricManager(m MetricManager) ServerOption {
	return func(a *api) {
		a.metrics = m
	}
}

// ServerWithPinnedSettings sets the pinned settings for the API server.
func ServerWithPinnedSettings(p PinnedSettings) ServerOption {
	return func(a *api) {
		a.pinned = p
	}
}

// ServerWithSettings sets the settings manager for the API server.
func ServerWithSettings(s Settings) ServerOption {
	return func(a *api) {
		a.settings = s
	}
}

// ServerWithRHPSessionReporter sets the RHP session reporter for the API server.
func ServerWithRHPSessionReporter(rsr RHPSessionReporter) ServerOption {
	return func(a *api) {
		a.sessions = rsr
	}
}

// ServerWithWallet sets the wallet for the API server.
func ServerWithWallet(w Wallet) ServerOption {
	return func(a *api) {
		a.wallet = w
	}
}

// ServerWithLogger sets the logger for the API server.
func ServerWithLogger(log *zap.Logger) ServerOption {
	return func(a *api) {
		a.log = log
	}
}
