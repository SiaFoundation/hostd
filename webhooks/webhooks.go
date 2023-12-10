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

	"go.sia.tech/hostd/internal/threadgroup"
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
)

type (
	scope struct {
		children map[string]*scope
		hooks    map[int64]bool
	}

	// A WebHook is a callback that is invoked when an event occurs.
	WebHook struct {
		ID          int64    `json:"id"`
		CallbackURL string   `json:"callbackURL"`
		SecretKey   string   `json:"secretKey"`
		Scopes      []string `json:"scopes"`
	}

	// A UID is a unique identifier for an event.
	UID [32]byte

	// An Event is a notification sent to a WebHook callback.
	Event struct {
		ID    UID    `json:"id"`
		Event string `json:"event"`
		Scope string `json:"scope"`
		Data  any    `json:"data"`
	}

	// A Store stores and retrieves WebHooks.
	Store interface {
		RegisterWebHook(url, secret string, scopes []string) (int64, error)
		UpdateWebHook(id int64, url string, scopes []string) error
		RemoveWebHook(id int64) error
		WebHooks() ([]WebHook, error)
	}

	// A Manager manages WebHook subscribers and broadcasts events
	Manager struct {
		store Store
		log   *zap.Logger
		tg    *threadgroup.ThreadGroup

		mu     sync.Mutex
		hooks  map[int64]WebHook
		scopes *scope
	}
)

// Close closes the Manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

func (m *Manager) findMatchingHooks(s string) (hooks []WebHook) {
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

// WebHooks returns all registered WebHooks.
func (m *Manager) WebHooks() (hooks []WebHook, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, hook := range m.hooks {
		hooks = append(hooks, hook)
	}
	return
}

// RegisterWebHook registers a new WebHook.
func (m *Manager) RegisterWebHook(url string, scopes []string) (WebHook, error) {
	done, err := m.tg.Add()
	if err != nil {
		return WebHook{}, err
	}
	defer done()

	secret := hex.EncodeToString(frand.Bytes(16))

	// register the hook in the database
	id, err := m.store.RegisterWebHook(url, secret, scopes)
	if err != nil {
		return WebHook{}, fmt.Errorf("failed to register WebHook: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// add the hook to the in-memory map
	hook := WebHook{
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

// RemoveWebHook removes a registered WebHook.
func (m *Manager) RemoveWebHook(id int64) error {
	done, err := m.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// remove the hook from the database
	if err := m.store.RemoveWebHook(id); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// remove the hook from the in-memory map and the scope tree
	delete(m.hooks, id)
	m.removeHookScopes(id)
	return nil
}

// UpdateWebHook updates the URL and scopes of a registered WebHook.
func (m *Manager) UpdateWebHook(id int64, url string, scopes []string) (WebHook, error) {
	done, err := m.tg.Add()
	if err != nil {
		return WebHook{}, err
	}
	defer done()

	// update the hook in the database
	err = m.store.UpdateWebHook(id, url, scopes)
	if err != nil {
		return WebHook{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// update the hook in the in-memory map
	hook, ok := m.hooks[id]
	if !ok {
		panic("UpdateWebHook called on nonexistent WebHook") // developer error
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

func sendEventData(ctx context.Context, hook WebHook, buf []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", hook.CallbackURL, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("failed to create WebHook request: %w", err)
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

// BroadcastEvent sends an event to all registered WebHooks that match the
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
		go func(hook WebHook) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			log := m.log.With(zap.Int64("hook", hook.ID), zap.String("url", hook.CallbackURL), zap.String("scope", scope), zap.String("event", event))

			start := time.Now()
			if err := sendEventData(ctx, hook, buf); err != nil {
				log.Error("failed to send webhook event", zap.Error(err))
				return
			}
			log.Debug("sent webhook event", zap.Duration("elapsed", time.Since(start)))
		}(hook)
	}
	return nil
}

// NewManager creates a new WebHook Manager
func NewManager(store Store, log *zap.Logger) (*Manager, error) {
	m := &Manager{
		store: store,
		log:   log,
		tg:    threadgroup.New(),

		hooks:  make(map[int64]WebHook),
		scopes: &scope{children: make(map[string]*scope), hooks: make(map[int64]bool)},
	}

	_, err := store.WebHooks()
	if err != nil {
		return nil, fmt.Errorf("failed to load WebHooks: %w", err)
	}
	return m, nil
}
