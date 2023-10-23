package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type (
	Webhook struct {
		Scope []string `json:"scope"`
		URL   string   `json:"url"`
	}

	WebhookQueueInfo struct {
		URL  string `json:"url"`
		Size int    `json:"size"`
	}

	// Event describes an event that has been triggered.
	Event struct {
		Scope   []string    `json:"scope"`
		Payload interface{} `json:"payload,omitempty"`
	}
)

var ErrWebhookNotFound = errors.New("Webhook not found")

type (
	WebhookStore interface {
		DeleteWebhook(wh Webhook) error
		AddWebhook(wh Webhook) error
		GetWebhook(id int) (Webhook, error)
		Webhooks() ([]Webhook, error)
	}

	Broadcaster interface {
		BroadcastAction(ctx context.Context, action Event) error
	}
)

const webhookTimeout = 10 * time.Second

var WebhookEventPing = []string{"ping"}

type Manager struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	store     WebhookStore

	mu       sync.Mutex
	queues   map[string]*eventQueue // URL -> queue
	webhooks map[string]Webhook
}

type eventQueue struct {
	ctx context.Context
	url string

	mu           sync.Mutex
	isDequeueing bool
	events       []Event
}

func (w *Manager) Close() error {
	w.ctxCancel()
	w.wg.Wait()
	return nil
}

func (w Webhook) String() string {
	return fmt.Sprintf("%v.%v", w.URL, w.Scope)
}

func (w *Manager) Delete(wh Webhook) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.store.DeleteWebhook(wh)
	if err != nil {
		return err
	}
	delete(w.webhooks, wh.String())
	return nil
}

func (w *Manager) Info() ([]Webhook, []WebhookQueueInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var hooks []Webhook
	for _, hook := range w.webhooks {
		hooks = append(hooks, Webhook{
			Scope: hook.Scope,
			URL:   hook.URL,
		})
	}
	var queueInfos []WebhookQueueInfo
	for _, queue := range w.queues {
		queue.mu.Lock()
		queueInfos = append(queueInfos, WebhookQueueInfo{
			URL:  queue.url,
			Size: len(queue.events),
		})
		queue.mu.Unlock()
	}
	return hooks, queueInfos
}

func (w *Manager) Webhook(id int) (Webhook, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	wh, err := w.store.GetWebhook(id)
	if err != nil {
		return Webhook{}, err
	}

	return wh, err
}

func (w Webhook) Matches(action Event) bool {
	scopeMap := make(map[string]bool)
	for _, s := range w.Scope {
		scopeMap[s] = true
	}
	for _, s := range action.Scope {
		if !scopeMap[s] {
			return false
		}
	}
	return true
}

func (w *Manager) BroadcastAction(_ context.Context, event Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, hook := range w.webhooks {

		if !hook.Matches(event) {
			continue
		}

		// Find queue or create one.
		queue, exists := w.queues[hook.URL]
		if !exists {
			queue = &eventQueue{
				ctx: w.ctx,
				url: hook.URL,
			}
			w.queues[hook.URL] = queue
		}

		// Add event and launch goroutine to start dequeueing if necessary.
		queue.mu.Lock()
		queue.events = append(queue.events, event)
		if !queue.isDequeueing {
			queue.isDequeueing = true
			w.wg.Add(1)
			go func() {
				queue.dequeue()
				w.wg.Done()
			}()
		}
		queue.mu.Unlock()
	}
	return nil
}

func (q *eventQueue) dequeue() {
	for {
		q.mu.Lock()
		if len(q.events) == 0 {
			q.isDequeueing = false
			q.mu.Unlock()
			return
		}
		next := q.events[0]
		q.events = q.events[1:]
		q.mu.Unlock()

		err := sendEvent(q.ctx, q.url, next)
		if err != nil {
			//fmt.Errorf("failed to send Webhook event %v to %v: %v", next.String(), q.url, err)
		}
	}
}

func (w *Manager) Register(wh Webhook) error {
	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()

	// Test URL.
	err := sendEvent(ctx, wh.URL, Event{
		Scope: WebhookEventPing,
	})
	if err != nil {
		return err
	}

	// Add Webhook.
	if err := w.store.AddWebhook(wh); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.webhooks[wh.String()] = wh
	return nil
}

func NewManager(store WebhookStore) (*Manager, error) {
	hooks, err := store.Webhooks()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		ctx:       ctx,
		ctxCancel: cancel,
		queues:    make(map[string]*eventQueue),
		store:     store,
		webhooks:  make(map[string]Webhook),
	}
	for _, hook := range hooks {
		m.webhooks[hook.String()] = hook
	}
	return m, nil
}

func sendEvent(ctx context.Context, url string, action Event) error {
	body, err := json.Marshal(action)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer io.ReadAll(req.Body) // always drain body

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		errStr, err := io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}
		return fmt.Errorf("Webhook returned unexpected status %v: %v", resp.StatusCode, string(errStr))
	}
	return nil
}
