//go:build windows

package storage

import (
	"errors"

	"golang.org/x/sys/windows"
)

func isNotEnoughStorageErr(err error) bool {
	return errors.Is(err, windows.ERROR_DISK_FULL)
}
