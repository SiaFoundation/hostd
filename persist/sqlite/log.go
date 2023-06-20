package sqlite

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/hostd/logging"
)

// AddEntries adds the entries to the database
func (s *Store) AddEntries(entries []logging.Entry) error {
	return s.transaction(func(tx txn) error {
		stmt, err := tx.Prepare(`INSERT INTO log_lines (date_created, log_level, log_name, log_caller, log_message, log_fields) VALUES (?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("failed to prepare log insert statement: %w", err)
		}
		defer stmt.Close()
		for _, entry := range entries {
			// insert the entry into the database
			if _, err := stmt.Exec(sqlTime(entry.Timestamp), int(entry.Level), entry.Name, entry.Caller, entry.Message, entry.Fields); err != nil {
				return fmt.Errorf("failed to insert log entry: %w", err)
			}
		}
		return nil
	})
}

// Prune deletes all log entries older than the given time.
func (s *Store) Prune(min time.Time) error {
	_, err := s.exec(`DELETE FROM log_lines WHERE date_created < ?`, sqlTime(min))
	return err
}

// LogEntries returns all log entries matching the given filter.
func (s *Store) LogEntries(filter logging.Filter) (entries []logging.Entry, count int, err error) {
	whereClause, queryParams, err := buildLogFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	selectQuery := fmt.Sprintf(`SELECT date_created, log_level, log_name, log_caller, log_message, log_fields FROM log_lines %s ORDER BY date_created DESC LIMIT ? OFFSET ?`, whereClause)
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM log_lines %s`, whereClause)

	// count the total number of entries matching the filter
	if err := s.queryRow(countQuery, queryParams...).Scan(&count); err != nil {
		return nil, 0, fmt.Errorf("failed to count log lines: %w", err)
	}

	// get the paginated log entries
	rows, err := s.query(selectQuery, append(queryParams, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query log lines: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry logging.Entry
		if err := rows.Scan((*sqlTime)(&entry.Timestamp), &entry.Level, &entry.Name, &entry.Caller, &entry.Message, &entry.Fields); err != nil {
			return nil, 0, fmt.Errorf("failed to scan log line: %w", err)
		}
		entries = append(entries, entry)
	}
	return
}

func buildLogFilter(filter logging.Filter) (string, []any, error) {
	if filter.After.Before(filter.Before) {
		return "", nil, errors.New("invalid time range")
	}

	var whereClause []string
	var queryParams []any

	if len(filter.Names) > 0 {
		whereClause = append(whereClause, "log_name IN ("+queryPlaceHolders(len(filter.Names))+")")
		queryParams = append(queryParams, queryArgs(filter.Names)...)
	}

	if len(filter.Levels) > 0 {
		whereClause = append(whereClause, "log_level IN ("+queryPlaceHolders(len(filter.Levels))+")")
		queryParams = append(queryParams, queryArgs(filter.Levels)...)
	}

	if len(filter.Callers) > 0 {
		whereClause = append(whereClause, "log_caller IN ("+queryPlaceHolders(len(filter.Callers))+")")
		queryParams = append(queryParams, queryArgs(filter.Callers)...)
	}

	switch {
	case filter.Before.IsZero() && !filter.After.IsZero():
		whereClause = append(whereClause, "date_created < ?")
		queryParams = append(queryParams, sqlTime(filter.After))
	case !filter.Before.IsZero() && filter.After.IsZero():
		whereClause = append(whereClause, "date_created > ?")
		queryParams = append(queryParams, sqlTime(filter.Before))
	case !filter.Before.IsZero() && !filter.After.IsZero():
		whereClause = append(whereClause, "date_created BETWEEN ? AND ?")
		queryParams = append(queryParams, sqlTime(filter.Before), sqlTime(filter.After))
	}
	if len(whereClause) == 0 {
		return "", nil, nil
	}
	return "WHERE " + strings.Join(whereClause, " AND "), queryParams, nil
}
