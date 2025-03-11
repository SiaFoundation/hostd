package api

import (
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/webhooks"
	"go.uber.org/zap"
)

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// WithAlerts sets the alerts manager for the API server.
func WithAlerts(al Alerts) ServerOption {
	return func(a *api) {
		a.alerts = al
	}
}

// WithSQLite3Store sets the SQLite3 store for the API server.
//
// This option is not required since it is only used for backups and there
// may be other stores in the future.
func WithSQLite3Store(s SQLite3Store) ServerOption {
	return func(a *api) {
		a.sqlite3Store = s
	}
}

// WithWebhooks sets the webhooks manager for the API server.
func WithWebhooks(w Webhooks) ServerOption {
	return func(a *api) {
		a.webhooks = w
	}
}

// WithPinnedSettings sets the pinned settings for the API server.
func WithPinnedSettings(p PinnedSettings) ServerOption {
	return func(a *api) {
		a.pinned = p
	}
}

// WithExplorer sets the explorer for the API server.
func WithExplorer(explorer *explorer.Explorer) ServerOption {
	return func(a *api) {
		a.explorerDisabled = false
		a.explorer = explorer
	}
}

// WithLogger sets the logger for the API server.
func WithLogger(log *zap.Logger) ServerOption {
	return func(a *api) {
		a.log = log
	}
}

type noopWebhooks struct{}

func (noopWebhooks) Webhooks() ([]webhooks.Webhook, error)                            { return nil, nil }
func (noopWebhooks) RemoveWebhook(id int64) error                                     { return nil }
func (noopWebhooks) BroadcastToWebhook(id int64, event, scope string, data any) error { return nil }
func (noopWebhooks) RegisterWebhook(callbackURL string, scopes []string) (webhooks.Webhook, error) {
	return webhooks.Webhook{}, nil
}
func (noopWebhooks) UpdateWebhook(id int64, callbackURL string, scopes []string) (webhooks.Webhook, error) {
	return webhooks.Webhook{}, nil
}

type noopAlerts struct{}

func (noopAlerts) Active() []alerts.Alert   { return nil }
func (noopAlerts) Dismiss(...types.Hash256) {}
