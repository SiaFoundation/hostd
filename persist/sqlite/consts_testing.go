//go:build testing

package sqlite

const (
	busyTimeout   = 5   // 5ms
	retryAttempts = 10  // 10 attempts
	factor        = 2.0 // factor ^ retryAttempts = backoff time in milliseconds

	// the number of records to limit long-running sector queries to
	sqlSectorBatchSize = 5 // 20 MiB
)
