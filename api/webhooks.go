package api

import (
	"errors"
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

func (a *api) handlePOSTWebhookDelete(c jape.Context) {
	var wh webhooks.Webhook
	if c.Decode(&wh) != nil {
		return
	}
	err := a.hooks.Delete(wh)
	if errors.Is(err, webhooks.ErrWebhookNotFound) {
		c.Error(fmt.Errorf("webhook for URL %v and event %v.%v not found", wh.URL, wh.Module, wh.Event), http.StatusNotFound)
		return
	} else if c.Check("failed to delete webhook", err) != nil {
		return
	}
}

func (a *api) handlePOSTWebhooksAction(c jape.Context) {
	var action webhooks.Event
	if c.Check("failed to decode action", c.Decode(&action)) != nil {
		return
	}
	a.hooks.BroadcastAction(c.Request.Context(), action)
}
