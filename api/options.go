package api

import "go.uber.org/zap"

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

func ServerWithAlerts(al Alerts) ServerOption {
	return func(a *api) {
		a.alerts = al
	}
}

func ServerWithWebHooks(w WebHooks) ServerOption {
	return func(a *api) {
		a.webhooks = w
	}
}

func ServerWithSyncer(g Syncer) ServerOption {
	return func(a *api) {
		a.syncer = g
	}
}

func ServerWithChainManager(chain ChainManager) ServerOption {
	return func(a *api) {
		a.chain = chain
	}
}

func ServerWithTransactionPool(tp TPool) ServerOption {
	return func(a *api) {
		a.tpool = tp
	}
}

func ServerWithContractManager(cm ContractManager) ServerOption {
	return func(a *api) {
		a.contracts = cm
	}
}

func ServerWithAccountManager(am AccountManager) ServerOption {
	return func(a *api) {
		a.accounts = am
	}
}

func ServerWithVolumeManager(vm VolumeManager) ServerOption {
	return func(a *api) {
		a.volumes = vm
	}
}

func ServerWithMetricManager(m MetricManager) ServerOption {
	return func(a *api) {
		a.metrics = m
	}
}

func ServerWithPinnedSettings(p PinnedSettings) ServerOption {
	return func(a *api) {
		a.pinned = p
	}
}

func ServerWithSettings(s Settings) ServerOption {
	return func(a *api) {
		a.settings = s
	}
}

func ServerWithRHPSessionReporter(rsr RHPSessionReporter) ServerOption {
	return func(a *api) {
		a.sessions = rsr
	}
}

func ServerWithWallet(w Wallet) ServerOption {
	return func(a *api) {
		a.wallet = w
	}
}

func ServerWithLogger(log *zap.Logger) ServerOption {
	return func(a *api) {
		a.log = log
	}
}
