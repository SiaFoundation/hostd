//go:build testing

package sqlite

const (
	busyTimeout   = 5   // 5ms
	retryAttempts = 10  // 10 attempts
	factor        = 2.0 // factor ^ retryAttempts = backoff time in milliseconds
)
