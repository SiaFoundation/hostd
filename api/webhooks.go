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
		Scope: req.Scope,
		URL:   req.URL,
	})
	if err != nil {
		c.Error(fmt.Errorf("failed to add Webhook: %w", err), http.StatusInternalServerError)
		return
	}
}

func (a *api) handleDELETEWebhook(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	wh, err := a.hooks.Webhook(id)
	if err != nil {
		c.Error(fmt.Errorf("failed to delete Webhook: %w", err), http.StatusInternalServerError)
		return
	}

	err = a.hooks.Delete(wh)
	if errors.Is(err, webhooks.ErrWebhookNotFound) {
		c.Error(fmt.Errorf("webhook for URL %v and event %v not found", wh.URL, wh.Scope), http.StatusNotFound)
		return
	} else if c.Check("failed to delete webhook", err) != nil {
		return
	}
}
