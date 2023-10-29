package sqlite

import (
	"errors"
	"fmt"
	"strings"

	"go.sia.tech/hostd/webhooks"
)

var ErrWebhookNotFound = errors.New("Webhook not found")

func (s *Store) DeleteWebhook(wb webhooks.Webhook) error {
	res, err := s.db.Exec("DELETE FROM webhooks WHERE hook_url = ?;", wb.URL)
	if err != nil {
		return err
	}

	count, err := res.RowsAffected()
	if count == 0 {
		return ErrWebhookNotFound
	}
	return err
}

func (s *Store) Webhooks() (whs []webhooks.Webhook, err error) {
	rows, err := s.db.Query(`SELECT id, scope, hook_url FROM webhooks;`)

	if err != nil {
		return nil, fmt.Errorf("failed to query webhooks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		wh := webhooks.Webhook{}
		var id int

		var tempScope string
		if err = rows.Scan(&id, &tempScope, &wh.URL); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		wh.Scope = strings.Split(tempScope, ",")

		whs = append(whs, wh)
	}

	return whs, err
}

func (s *Store) GetWebhook(id int) (wh webhooks.Webhook, err error) {
	var tempScope string
	err = s.db.QueryRow(`SELECT id, scope, hook_url FROM webhooks WHERE id = ?;`, id).Scan(&id, &tempScope, &wh.URL)
	wh.Scope = strings.Split(tempScope, ",")

	if err != nil {
		return webhooks.Webhook{}, err
	}

	return
}

func (s *Store) AddWebhook(wb webhooks.Webhook) (err error) {
	const query = `INSERT INTO webhooks (scope, hook_url) VALUES (?,?) ON CONFLICT (hook_url) DO UPDATE SET scope=excluded.scope;`
	_, err = s.exec(query, strings.Join(wb.Scope, ","), wb.URL)
	return err
}
