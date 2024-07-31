package api

import (
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/explorer"
	"go.sia.tech/hostd/rhp"
	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap"
)

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// ServerWithAlerts sets the alerts manager for the API server.
func ServerWithAlerts(al Alerts) ServerOption {
	return func(a *api) {
		a.alerts = al
	}
}

// ServerWithWebhooks sets the webhooks manager for the API server.
func ServerWithWebhooks(w Webhooks) ServerOption {
	return func(a *api) {
		a.webhooks = w
	}
}

// ServerWithPinnedSettings sets the pinned settings for the API server.
func ServerWithPinnedSettings(p PinnedSettings) ServerOption {
	return func(a *api) {
		a.pinned = p
	}
}

// ServerWithExplorer sets the explorer for the API server.
func ServerWithExplorer(explorer *explorer.Explorer) ServerOption {
	return func(a *api) {
		a.explorerDisabled = false
		a.explorer = explorer
	}
}

// ServerWithRHPSessionReporter sets the RHP session reporter for the API server.
func ServerWithRHPSessionReporter(rsr RHPSessionReporter) ServerOption {
	return func(a *api) {
		a.sessions = rsr
	}
}

// ServerWithLogger sets the logger for the API server.
func ServerWithLogger(log *zap.Logger) ServerOption {
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

type noopSessionReporter struct{}

func (noopSessionReporter) Subscribe(rhp.SessionSubscriber)   {}
func (noopSessionReporter) Unsubscribe(rhp.SessionSubscriber) {}
func (noopSessionReporter) Active() []rhp.Session             { return nil }
