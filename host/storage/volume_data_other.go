//go:build !windows

package storage

import (
	"os"

	proto4 "go.sia.tech/core/rhp/v4"
)

type volumeDataFile struct {
	*os.File
}

func (v volumeDataFile) ReadSector(p []byte, offset int64) (int, error) {
	return v.ReadAt(p, offset)
}

func (v volumeDataFile) WriteSector(data *[proto4.SectorSize]byte, offset int64) (int, error) {
	return v.WriteAt(data[:], offset)
}

func openVolumeData(path string, flag int, perm os.FileMode) (volumeData, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &volumeDataFile{f}, nil
}
