//go:build !testing

package sqlite

const (
	busyTimeout   = 5000 // 5 seconds
	retryAttempts = 15   // 15 attempts
	factor        = 2.0  // factor ^ retryAttempts = backoff time in milliseconds

	// the number of records to limit long-running sector queries to
	sqlSectorBatchSize = 256 // 1 GiB
)
