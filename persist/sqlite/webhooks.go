package sqlite

import (
	"strings"

	"go.sia.tech/hostd/webhooks"
)

// RegisterWebHook registers a new webhook.
func (s *Store) RegisterWebHook(url, secret string, scopes []string) (id int64, err error) {
	err = s.queryRow("INSERT INTO webhooks (callback_url, secret_key, scopes) VALUES (?, ?, ?) RETURNING id", url, secret, strings.Join(scopes, ",")).Scan(&id)
	return
}

// UpdateWebHook updates a webhook.
func (s *Store) UpdateWebHook(id int64, url string, scopes []string) error {
	var dbID int64
	return s.queryRow(`UPDATE webhooks SET callback_url = ?, scopes = ? WHERE id = ? RETURNING id`, url, strings.Join(scopes, ","), id).Scan(&dbID)
}

// RemoveWebHook removes a webhook.
func (s *Store) RemoveWebHook(id int64) error {
	_, err := s.exec("DELETE FROM webhooks WHERE id = ?", id)
	return err
}

// WebHooks returns all webhooks.
func (s *Store) WebHooks() ([]webhooks.WebHook, error) {
	rows, err := s.query("SELECT id, callback_url, secret_key, scopes FROM webhooks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hooks []webhooks.WebHook
	for rows.Next() {
		var hook webhooks.WebHook
		var scopes string
		if err := rows.Scan(&hook.ID, &hook.CallbackURL, &hook.SecretKey, &scopes); err != nil {
			return nil, err
		}
		hook.Scopes = strings.Split(scopes, ",")
		hooks = append(hooks, hook)
	}
	return hooks, nil
}
