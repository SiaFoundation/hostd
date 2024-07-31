package webhooks

// A WebhookBroadcaster broadcasts events to webhooks.
type WebhookBroadcaster interface {
	BroadcastEvent(event string, scope string, data any) error
	BroadcastToWebhook(hookID int64, event string, scope string, data any) error
}

// A NoOpBroadcaster is a WebhookBroadcaster that does nothing.
type NoOpBroadcaster struct{}

// BroadcastEvent implements WebhookBroadcaster.
func (NoOpBroadcaster) BroadcastEvent(event string, scope string, data any) error { return nil }

// BroadcastToWebhook implements WebhookBroadcaster.
func (NoOpBroadcaster) BroadcastToWebhook(hookID int64, event string, scope string, data any) error {
	return nil
}

// NewNop returns a new NoOpBroadcaster.
func NewNop() NoOpBroadcaster {
	return NoOpBroadcaster{}
}
