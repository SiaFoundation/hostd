package storage

import (
	"fmt"
	"io"
	"sync"

	rhpv2 "go.sia.tech/core/rhp/v2"
)

type (
	// volumeData wraps the methods needed to read and write sector data to a
	// volume.
	volumeData interface {
		io.ReaderAt
		io.WriterAt

		Sync() error
		Truncate(int64) error
		Close() error
	}

	// VolumeStats contains statistics about a volume
	VolumeStats struct {
		FailedReads      uint64  `json:"failedReads"`
		FailedWrites     uint64  `json:"failedWrites"`
		SuccessfulReads  uint64  `json:"successfulReads"`
		SuccessfulWrites uint64  `json:"successfulWrites"`
		Errors           []error `json:"errors"`
	}

	// A Volume stores and retrieves sector data
	Volume struct {
		ID           int    `json:"ID"`
		LocalPath    string `json:"localPath"`
		UsedSectors  uint64 `json:"usedSectors"`
		TotalSectors uint64 `json:"totalSectors"`
		ReadOnly     bool   `json:"readOnly"`
		Available    bool   `json:"available"`
	}

	// A volume stores and retrieves sector data
	volume struct {
		// data is a flatfile that stores the volume's sector data
		data volumeData

		mu    sync.Mutex // protects the fields below
		stats VolumeStats
		// busy must be set to true when the volume is being resized to prevent
		// conflicting operations.
		busy bool
	}
)

// ReadSector reads the sector at index from the volume
func (v *volume) ReadSector(index uint64) (*[rhpv2.SectorSize]byte, error) {
	var sector [rhpv2.SectorSize]byte
	_, err := v.data.ReadAt(sector[:], int64(index*rhpv2.SectorSize))
	v.mu.Lock()
	if err != nil {
		v.stats.FailedReads++
		v.stats.Errors = append(v.stats.Errors, fmt.Errorf("failed to read sector at index %v: %w", index, err))
		if len(v.stats.Errors) > 100 {
			v.stats.Errors = v.stats.Errors[len(v.stats.Errors)-100:]
		}
	} else {
		v.stats.SuccessfulReads++
	}
	v.mu.Unlock()
	return &sector, err
}

// WriteSector writes a sector to the volume at index
func (v *volume) WriteSector(data *[rhpv2.SectorSize]byte, index uint64) error {
	_, err := v.data.WriteAt(data[:], int64(index*rhpv2.SectorSize))
	v.mu.Lock()
	if err != nil {
		v.stats.FailedWrites++
		v.stats.Errors = append(v.stats.Errors, fmt.Errorf("failed to write sector to index %v: %w", index, err))
		if len(v.stats.Errors) > 100 {
			v.stats.Errors = v.stats.Errors[len(v.stats.Errors)-100:]
		}
	} else {
		v.stats.SuccessfulWrites++
	}
	v.mu.Unlock()
	return err
}

// Sync syncs the volume
func (v *volume) Sync() (err error) {
	err = v.data.Sync()
	if err != nil {
		v.mu.Lock()
		v.stats.Errors = append(v.stats.Errors, fmt.Errorf("failed to sync volume: %w", err))
		if len(v.stats.Errors) > 100 {
			v.stats.Errors = v.stats.Errors[len(v.stats.Errors)-100:]
		}
		v.mu.Unlock()
	}
	return
}

func (v *volume) Resize(sectors uint64) error {
	return v.data.Truncate(int64(sectors * rhpv2.SectorSize))
}

// Close closes the volume
func (v *volume) Close() error {
	return v.data.Close()
}
