package sqlite

import (
	"errors"
	"fmt"

	"go.sia.tech/hostd/host/webhooks"
)

var ErrWebhookNotFound = errors.New("Webhook not found")

type (
	dbWebhook struct {
		Module string
		Event  string
		URL    string
	}
)

func (s *Store) DeleteWebhook(wb webhooks.Webhook) error {
	res, err := s.db.Exec("DELETE FROM webhooks WHERE module = ? AND event = ? AND url = ?", wb.Module, wb.Event, wb.URL)
	if err != nil {
		return err
	}

	count, err := res.RowsAffected()
	if count == 0 {
		return ErrWebhookNotFound
	}
	return err
}

func (s *Store) Webhooks() (wbs []webhooks.Webhook, err error) {
	rows, err := s.db.Query(`SELECT * FROM webhooks;`)

	if err != nil {
		return nil, fmt.Errorf("failed to query webhooks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		wb := webhooks.Webhook{}
		var id int

		if err = rows.Scan(&id, &wb.Module, &wb.Event, &wb.URL); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		wbs = append(wbs, wb)
	}

	return wbs, err
}

func (s *Store) AddWebhook(wb webhooks.Webhook) (err error) {
	var count int

	err = s.db.QueryRow(`SELECT COUNT(*) FROM webhooks WHERE module = ? AND event = ? AND url = ?;`, wb.Module, wb.Event, wb.URL).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		const query = `INSERT INTO webhooks (module, event, url) VALUES (?,?,?);`
		_, err = s.exec(query, wb.Module, wb.Event, wb.URL)
	}
	return err
}
