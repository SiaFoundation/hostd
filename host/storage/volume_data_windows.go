package storage

import (
	"os"

	"go.sia.tech/hostd/v2/internal/directio"
)

func openVolumeData(path string, flag int, perm os.FileMode) (volumeData, error) {
	return directio.OpenFile(path, flag, perm)
}
