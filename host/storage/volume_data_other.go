//go:build !windows

package storage

import "os"

func openVolumeData(path string, flag int, perm os.FileMode) (volumeData, error) {
	return os.OpenFile(path, flag, perm)
}
