//go:build !testing

package storage

import "time"

const (
	cleanupInterval = 15 * time.Minute
)
