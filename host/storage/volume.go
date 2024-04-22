package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
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

	// A volume stores and retrieves sector data from a local file
	volume struct {
		// when reading or writing to the volume, a read lock should be held.
		// When resizing or updating the volume's state, a write lock should be
		// held.
		mu sync.RWMutex

		location string     // location is the path to the volume's file
		data     volumeData // data is a flatfile that stores the volume's sector data
		stats    VolumeStats
	}

	// VolumeStats contains statistics about a volume
	VolumeStats struct {
		FailedReads      uint64  `json:"failedReads"`
		FailedWrites     uint64  `json:"failedWrites"`
		SuccessfulReads  uint64  `json:"successfulReads"`
		SuccessfulWrites uint64  `json:"successfulWrites"`
		Status           string  `json:"status"`
		Errors           []error `json:"errors"`
	}

	// A Volume stores and retrieves sector data
	Volume struct {
		ID           int64  `json:"id"`
		LocalPath    string `json:"localPath"`
		UsedSectors  uint64 `json:"usedSectors"`
		TotalSectors uint64 `json:"totalSectors"`
		ReadOnly     bool   `json:"readOnly"`
		Available    bool   `json:"available"`
	}

	// VolumeMeta contains the metadata of a volume.
	VolumeMeta struct {
		Volume
		VolumeStats
	}
)

// ErrVolumeNotAvailable is returned when a volume is not available
var ErrVolumeNotAvailable = errors.New("volume not available")

func (v *volume) incrementReadStats(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err != nil {
		v.stats.FailedReads++
		v.appendError(err)
	} else {
		v.stats.SuccessfulReads++
	}
}

func (v *volume) incrementWriteStats(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err != nil {
		v.stats.FailedWrites++
		v.appendError(err)
	} else {
		v.stats.SuccessfulWrites++
	}
}

func (v *volume) appendError(err error) {
	v.stats.Errors = append(v.stats.Errors, err)
	if len(v.stats.Errors) > 100 {
		v.stats.Errors = v.stats.Errors[len(v.stats.Errors)-100:]
	}
}

// Location returns the location of the volume
func (v *volume) Location() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.location
}

// alertID returns a deterministic alert ID for a volume and context
func (v *volume) alertID(context string) types.Hash256 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return types.HashBytes([]byte(v.location + context))
}

// OpenVolume opens the volume at localPath
func (v *volume) OpenVolume(localPath string, reload bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.data != nil && !reload {
		return nil
	}
	f, err := os.OpenFile(localPath, os.O_RDWR, 0700)
	if err != nil {
		return err
	}
	v.location = localPath
	v.data = f
	return nil
}

// SetStatus sets the status of the volume. If the new status is resizing, the
// volume must be ready. If the new status is removing, the volume must be ready
// or unavailable.
func (v *volume) SetStatus(status string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.stats.Status == status {
		return nil
	}

	switch status {
	case VolumeStatusRemoving:
		if v.stats.Status != VolumeStatusReady && v.stats.Status != VolumeStatusUnavailable {
			return fmt.Errorf("volume is %v", v.stats.Status)
		}
	case VolumeStatusResizing:
		if v.stats.Status != VolumeStatusReady {
			return fmt.Errorf("volume is %v", v.stats.Status)
		}
	case VolumeStatusReady, VolumeStatusUnavailable:
	default:
		panic("cannot set status to " + status) // developer error
	}
	v.stats.Status = status
	return nil
}

func (v *volume) Status() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.stats.Status
}

// ReadSector reads the sector at index from the volume
func (v *volume) ReadSector(index uint64) (*[rhp2.SectorSize]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.data == nil {
		return nil, ErrVolumeNotAvailable
	}

	var sector [rhp2.SectorSize]byte
	_, err := v.data.ReadAt(sector[:], int64(index*rhp2.SectorSize))

	if err != nil {
		err = fmt.Errorf("failed to read sector at index %v: %w", index, err)
	}
	go v.incrementReadStats(err)
	return &sector, err
}

// WriteSector writes a sector to the volume at index
func (v *volume) WriteSector(data *[rhp2.SectorSize]byte, index uint64) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.data == nil {
		panic("volume not open") // developer error
	}
	_, err := v.data.WriteAt(data[:], int64(index*rhp2.SectorSize))
	if err != nil {
		err = fmt.Errorf("failed to write sector to index %v: %w", index, err)
	}
	go v.incrementWriteStats(err)
	return err
}

// Sync syncs the volume
func (v *volume) Sync() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.data == nil {
		return nil
	}
	err := v.data.Sync()
	if err != nil {
		v.appendError(fmt.Errorf("failed to sync volume: %w", err))
	}
	return err
}

// Resize resizes the volume to the new number of sectors
func (v *volume) Resize(newSectors uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.data == nil {
		return ErrVolumeNotAvailable
	}
	return v.data.Truncate(int64(newSectors * rhp2.SectorSize))
}

func (v *volume) Stats() VolumeStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.stats
}

// Close closes the volume
func (v *volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.data == nil {
		return nil
	} else if err := v.data.Sync(); err != nil {
		return fmt.Errorf("failed to sync volume: %w", err)
	} else if err := v.data.Close(); err != nil {
		return fmt.Errorf("failed to close volume: %w", err)
	}
	v.data = nil
	v.stats.Status = VolumeStatusUnavailable
	return nil
}
