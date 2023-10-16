package api

import (
	"fmt"
	"net/http"

	"go.sia.tech/hostd/host/webhooks"
	"go.sia.tech/jape"
)

func (a *api) handleGETWebhooks(c jape.Context) {
	webhooks, queueInfo := a.hooks.Info()
	c.Encode(WebHookResponse{
		Queues:   queueInfo,
		Webhooks: webhooks,
	})
}

func (a *api) handlePOSTWebhooks(c jape.Context) {
	var req webhooks.Webhook
	if c.Decode(&req) != nil {
		return
	}
	err := a.hooks.Register(webhooks.Webhook{
		Event:  req.Event,
		Module: req.Module,
		URL:    req.URL,
	})
	if err != nil {
		c.Error(fmt.Errorf("failed to add Webhook: %w", err), http.StatusInternalServerError)
		return
	}
}
