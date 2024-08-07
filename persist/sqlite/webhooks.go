package sqlite

import (
	"strings"

	"go.sia.tech/hostd/webhooks"
)

// RegisterWebhook registers a new webhook.
func (s *Store) RegisterWebhook(url, secret string, scopes []string) (id int64, err error) {
	err = s.transaction(func(tx *txn) error {
		return tx.QueryRow("INSERT INTO webhooks (callback_url, secret_key, scopes) VALUES (?, ?, ?) RETURNING id", url, secret, strings.Join(scopes, ",")).Scan(&id)
	})
	return
}

// UpdateWebhook updates a webhook.
func (s *Store) UpdateWebhook(id int64, url string, scopes []string) error {
	return s.transaction(func(tx *txn) error {
		var dbID int64
		return tx.QueryRow(`UPDATE webhooks SET callback_url = ?, scopes = ? WHERE id = ? RETURNING id`, url, strings.Join(scopes, ","), id).Scan(&dbID)
	})
}

// RemoveWebhook removes a webhook.
func (s *Store) RemoveWebhook(id int64) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec("DELETE FROM webhooks WHERE id = ?", id)
		return err
	})
}

// Webhooks returns all webhooks.
func (s *Store) Webhooks() (hooks []webhooks.Webhook, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query("SELECT id, callback_url, secret_key, scopes FROM webhooks")
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var hook webhooks.Webhook
			var scopes string
			if err := rows.Scan(&hook.ID, &hook.CallbackURL, &hook.SecretKey, &scopes); err != nil {
				return err
			}
			hook.Scopes = strings.Split(scopes, ",")
			hooks = append(hooks, hook)
		}
		return rows.Err()
	})
	return
}
