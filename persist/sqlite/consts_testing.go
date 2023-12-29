//go:build testing

package sqlite

import "time"

const (
	busyTimeout      = 100 // 100ms
	maxRetryAttempts = 10  // 10 attempts
	factor           = 2.0 // factor ^ retryAttempts = backoff time in milliseconds
	maxBackoff       = 15 * time.Second

	// the number of records to limit long-running sector queries to
	sqlSectorBatchSize = 5 // 20 MiB
)
