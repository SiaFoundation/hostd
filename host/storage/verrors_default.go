//go:build !windows

package storage

import (
	"errors"
	"syscall"
)

func isNotEnoughStorageErr(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
