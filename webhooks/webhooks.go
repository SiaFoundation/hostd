package webhooks

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// event scope constants
const (
	ScopeAll = "all"

	ScopeAlerts         = "alerts"
	ScopeAlertsInfo     = "alerts/info"
	ScopeAlertsWarning  = "alerts/warning"
	ScopeAlertsError    = "alerts/error"
	ScopeAlertsCritical = "alerts/critical"

	ScopeWallet = "wallet"
	ScopeTest   = "test"
)

type (
	scope struct {
		children map[string]*scope
		hooks    map[int64]bool
	}

	// A Webhook is a callback that is invoked when an event occurs.
	Webhook struct {
		ID          int64    `json:"id"`
		CallbackURL string   `json:"callbackURL"`
		SecretKey   string   `json:"secretKey"`
		Scopes      []string `json:"scopes"`
	}

	// A UID is a unique identifier for an event.
	UID [32]byte

	// An Event is a notification sent to a Webhook callback.
	Event struct {
		ID    UID    `json:"id"`
		Event string `json:"event"`
		Scope string `json:"scope"`
		Data  any    `json:"data"`
	}

	// A Store stores and retrieves Webhooks.
	Store interface {
		RegisterWebhook(url, secret string, scopes []string) (int64, error)
		UpdateWebhook(id int64, url string, scopes []string) error
		RemoveWebhook(id int64) error
		Webhooks() ([]Webhook, error)
	}

	// A Manager manages Webhook subscribers and broadcasts events
	Manager struct {
		store Store
		log   *zap.Logger
		tg    *threadgroup.ThreadGroup

		mu     sync.Mutex
		hooks  map[int64]Webhook
		scopes *scope
	}
)

var _ WebhookBroadcaster = (*Manager)(nil)

// Close closes the Manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

func (m *Manager) findMatchingHooks(s string) (hooks []Webhook) {
	// recursively match hooks
	var match func(scopeParts []string, parent *scope)
	match = func(scopeParts []string, parent *scope) {
		for id := range parent.hooks {
			hook, ok := m.hooks[id]
			if !ok {
				panic("hook not found") // developer error
			}
			hooks = append(hooks, hook)
		}
		if len(scopeParts) == 0 {
			return
		}
		child, ok := parent.children[scopeParts[0]]
		if !ok {
			return
		}
		match(scopeParts[1:], child)
	}

	match(strings.Split(s, "/"), m.scopes)
	return
}

func (m *Manager) addHookScopes(id int64, scopes []string) {
	for _, s := range scopes {
		if s == "all" { // special case to register for all current and future scopes
			m.scopes.hooks[id] = true
			continue
		}

		parts := strings.Split(s, "/")
		parent := m.scopes
		for _, part := range parts {
			child, ok := parent.children[part]
			if !ok {
				child = &scope{children: make(map[string]*scope), hooks: make(map[int64]bool)}
				parent.children[part] = child
			}
			parent = child
		}
		parent.hooks[id] = true
	}
}

func (m *Manager) removeHookScopes(id int64) {
	var remove func(parent *scope)
	remove = func(parent *scope) {
		for _, child := range parent.children {
			remove(child)
		}
		delete(parent.hooks, id)
	}

	remove(m.scopes)
}

// Webhooks returns all registered Webhooks.
func (m *Manager) Webhooks() (hooks []Webhook, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, hook := range m.hooks {
		hooks = append(hooks, hook)
	}
	return
}

// RegisterWebhook registers a new Webhook.
func (m *Manager) RegisterWebhook(url string, scopes []string) (Webhook, error) {
	done, err := m.tg.Add()
	if err != nil {
		return Webhook{}, err
	}
	defer done()

	secret := hex.EncodeToString(frand.Bytes(16))

	// register the hook in the database
	id, err := m.store.RegisterWebhook(url, secret, scopes)
	if err != nil {
		return Webhook{}, fmt.Errorf("failed to register Webhook: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// add the hook to the in-memory map
	hook := Webhook{
		ID:          id,
		CallbackURL: url,
		SecretKey:   secret,
		Scopes:      scopes,
	}
	m.hooks[id] = hook
	// add the hook to the scope tree
	m.addHookScopes(id, scopes)
	return hook, nil
}

// RemoveWebhook removes a registered Webhook.
func (m *Manager) RemoveWebhook(id int64) error {
	done, err := m.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// remove the hook from the database
	if err := m.store.RemoveWebhook(id); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// remove the hook from the in-memory map and the scope tree
	delete(m.hooks, id)
	m.removeHookScopes(id)
	return nil
}

// UpdateWebhook updates the URL and scopes of a registered Webhook.
func (m *Manager) UpdateWebhook(id int64, url string, scopes []string) (Webhook, error) {
	done, err := m.tg.Add()
	if err != nil {
		return Webhook{}, err
	}
	defer done()

	// update the hook in the database
	err = m.store.UpdateWebhook(id, url, scopes)
	if err != nil {
		return Webhook{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// update the hook in the in-memory map
	hook, ok := m.hooks[id]
	if !ok {
		panic("UpdateWebhook called on nonexistent Webhook") // developer error
	}
	hook.CallbackURL = url
	hook.Scopes = scopes
	m.hooks[id] = hook
	// remove the hook from the scope tree
	m.removeHookScopes(id)
	// readd the new scopes to the scope tree
	m.addHookScopes(id, scopes)
	return hook, nil
}

func sendEventData(ctx context.Context, hook Webhook, buf []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", hook.CallbackURL, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("failed to create Webhook request: %w", err)
	}

	// set the secret key and content type
	req.SetBasicAuth("", hook.SecretKey)
	req.Header.Set("Content-Type", "application/json")

	// send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected response status code: %d", resp.StatusCode)
	}
	return nil
}

// BroadcastToWebhook sends an event to a specific Webhook subscriber.
func (m *Manager) BroadcastToWebhook(hookID int64, event string, scope string, data any) error {
	done, err := m.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	uid := UID(frand.Bytes(32))
	e := Event{
		ID:    uid,
		Event: event,
		Scope: scope,
		Data:  data,
	}

	buf, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	hook, ok := m.hooks[hookID]
	if !ok {
		return fmt.Errorf("Webhook not found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := m.log.With(zap.Int64("hook", hook.ID), zap.String("url", hook.CallbackURL), zap.String("scope", scope), zap.String("event", event))

	start := time.Now()
	if err := sendEventData(ctx, hook, buf); err != nil {
		return fmt.Errorf("failed to send Webhook event: %w", err)
	}
	log.Debug("sent Webhook event", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// BroadcastEvent sends an event to all registered Webhooks that match the
// event's scope.
func (m *Manager) BroadcastEvent(event string, scope string, data any) error {
	done, err := m.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	uid := UID(frand.Bytes(32))
	e := Event{
		ID:    uid,
		Event: event,
		Scope: scope,
		Data:  data,
	}

	buf, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// find matching hooks
	hooks := m.findMatchingHooks(scope)

	for _, hook := range hooks {
		go func(hook Webhook) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			log := m.log.With(zap.Int64("hook", hook.ID), zap.String("url", hook.CallbackURL), zap.String("scope", scope), zap.String("event", event))

			start := time.Now()
			if err := sendEventData(ctx, hook, buf); err != nil {
				log.Error("failed to send Webhook event", zap.Error(err))
				return
			}
			log.Debug("sent Webhook event", zap.Duration("elapsed", time.Since(start)))
		}(hook)
	}
	return nil
}

// NewManager creates a new Webhook Manager
func NewManager(store Store, log *zap.Logger) (*Manager, error) {
	m := &Manager{
		store: store,
		log:   log,
		tg:    threadgroup.New(),

		hooks:  make(map[int64]Webhook),
		scopes: &scope{children: make(map[string]*scope), hooks: make(map[int64]bool)},
	}

	_, err := store.Webhooks()
	if err != nil {
		return nil, fmt.Errorf("failed to load Webhooks: %w", err)
	}
	return m, nil
}
