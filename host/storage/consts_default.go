//go:build !testing

package storage

import "time"

const (
	resizeBatchSize = 64 // 256 MiB

	cleanupInterval = 15 * time.Minute
)
