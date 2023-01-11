package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

type (
	// A SectorLocation is a location of a sector within a volume.
	SectorLocation struct {
		Volume VolumeID
		Index  uint64
	}

	Manager struct {
		vs VolumeStore
		ss SectorStore

		mu      sync.Mutex
		volumes map[VolumeID]*volume
	}
)

// lockVolume locks a volume for operations until release is called.
func (m *Manager) lockVolume(id VolumeID) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.volumes[id]
	if !ok {
		return nil, fmt.Errorf("volume %v not found", id)
	} else if v.busy {
		return nil, fmt.Errorf("volume %v is busy", id)
	}
	return func() {
		m.mu.Lock()
		m.volumes[id].busy = false
		m.mu.Unlock()
	}, nil
}

// migrateSector migrates a sector from one volume to another.
func (m *Manager) migrateSector(root SectorRoot, location SectorLocation) error {
	sector, err := m.ReadSector(root)
	if err != nil {
		return fmt.Errorf("failed to read sector %v: %w", root, err)
	}

	newVolume, err := m.volume(location.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume %v: %w", location.Volume, err)
	}

	// write the sector to the new volume
	if err := newVolume.WriteSector(sector, location.Index); err != nil {
		return fmt.Errorf("failed to write sector %v: %w", location, err)
	}
	return nil
}

// syncVolumes syncs the data files of changed volumes.
func (m *Manager) syncVolumes(volumes []VolumeID) error {
	for _, id := range volumes {
		v, err := m.volume(id)
		if err != nil {
			return fmt.Errorf("failed to get volume %v: %w", id, err)
		} else if err := v.Sync(); err != nil {
			return fmt.Errorf("failed to sync volume %v: %w", id, err)
		}
	}
	return nil
}

func (m *Manager) growVolume(id VolumeID, target, delta uint64) error {
	if delta < target {
		return errors.New("invalid sector range")
	}
	v, err := m.volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	start := target - delta
	n := uint64(256)
	for i := start; i < target; i += n {
		if i+n > target {
			n = target - i
		}
		// truncate the file and add the indices to the volume store. resize is
		// done in chunks to prevent holding the lock for too long and to allow
		// progress tracking.
		if v.Resize(i); err != nil {
			return fmt.Errorf("failed to expand volume data: %w", err)
		} else if err := m.vs.GrowVolume(id, n); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}
	}
	return nil
}

func (m *Manager) shrinkVolume(id VolumeID, target, delta uint64) error {
	if target < delta {
		return errors.New("invalid sector range")
	}

	volume, err := m.volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	// migrate any sectors outside of the new end of the volume
	err = m.vs.MigrateSectors(id, target, m.migrateSector, m.syncVolumes)
	if err != nil {
		return fmt.Errorf("failed to migrate sectors: %w", err)
	}

	current := target + delta
	n := uint64(256)
	for i := current; i > target; i -= n {
		if current > i-n {
			n = i - current
		}
		// shrink in chunks to prevent holding locks for too long and to
		// track progress.
		if err := m.vs.ShrinkVolume(id, n); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}
	}
	// resize the data file
	return volume.Resize(target)
}

// Volumes returns a list of all volumes in the storage manager.
func (m *Manager) Volumes() ([]Volume, error) {
	return m.vs.Volumes()
}

// AddVolume adds a new volume to the storage manager
func (m *Manager) AddVolume(localPath string, maxSectors uint64) (Volume, error) {
	f, err := os.Create(localPath)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to create volume file: %w", err)
	}

	v, err := m.vs.AddVolume(localPath, maxSectors, false)
	if err != nil {
		return v, fmt.Errorf("failed to add volume to store: %w", err)
	}

	// add the new volume to the volume map
	m.mu.Lock()
	m.volumes[v.ID] = &volume{
		data: f,
		stats: VolumeStats{
			Available: true,
		},
	}
	m.mu.Unlock()

	// lock the volume during grow operation
	release, err := m.lockVolume(v.ID)
	if err != nil {
		return v, fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// grow the volume to the desired size
	if err := m.growVolume(v.ID, v.MaxSectors, v.MaxSectors); err != nil {
		return v, fmt.Errorf("failed to grow volume: %w", err)
	}

	return v, nil
}

// SetReadOnly sets the read-only status of a volume.
func (m *Manager) SetVolumeReadOnly(id VolumeID, readOnly bool) error {
	release, err := m.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	if err := m.vs.SetReadOnly(id, readOnly); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	return nil
}

// RemoveVolume removes a volume from the manager.
func (m *Manager) RemoveVolume(id VolumeID, force bool) error {
	// lock the volume during removal to prevent concurrent operations
	release, err := m.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// set the volume to read-only to prevent new sectors from being added
	if err := m.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	// set the volume's max size to 0
	if _, err := m.vs.SetMaxSectors(id, 0); err != nil {
		return fmt.Errorf("failed to resize volume %v: %w", id, err)
	}

	// migrate sectors to other volumes
	err = m.vs.MigrateSectors(id, 0, m.migrateSector, m.syncVolumes)
	if err != nil {
		return fmt.Errorf("failed to migrate sector data: %w", err)
	}
	return m.vs.RemoveVolume(id, force)
}

// ResizeVolume resizes a volume to the specified size.
func (m *Manager) ResizeVolume(ctx context.Context, id VolumeID, maxSectors uint64) error {
	release, err := m.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// set the volume to read-only to prevent new sectors from being added
	if err := m.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	// set the volume's max size
	delta, err := m.vs.SetMaxSectors(id, maxSectors)
	if err != nil {
		return fmt.Errorf("failed to resize volume %v: %w", id, err)
	}

	// if delta is positive, add sectors to the volume. Otherwise, migrate then
	// remove sectors from the volume.
	if delta > 0 {
		// add sectors to the volume
		if err := m.growVolume(id, maxSectors, uint64(delta)); err != nil {
			return fmt.Errorf("failed to grow volume: %w", err)
		}
		return nil
	}
	if err := m.shrinkVolume(id, maxSectors, uint64(-delta)); err != nil {
		return fmt.Errorf("failed to shrink volume from volume: %w", err)
	}
	return nil
}

// RemoveSector deletes a sector's metadata and zeroes its data.
func (m *Manager) RemoveSector(root SectorRoot) error {
	loc, err := m.ss.RemoveSector(root)
	if err != nil {
		return fmt.Errorf("failed to locate sector %v: %w", root, err)
	}

	// get the volume from memory
	vol, err := m.volume(loc.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
	}

	// zero the sector, immediately sync the volume, then release the lock
	zeroes := make([]byte, sectorSize)
	if err := vol.WriteSector(zeroes, loc.Index); err != nil {
		return fmt.Errorf("failed to zero sector %v: %w", root, err)
	} else if err := vol.Sync(); err != nil {
		return fmt.Errorf("failed to sync volume %v: %w", loc.Volume, err)
	}
	return nil
}

// ReadSector reads the sector with the given root from a volume.
func (m *Manager) ReadSector(root SectorRoot) ([]byte, error) {
	loc, release, err := m.ss.SectorLocation(root)
	if err != nil {
		return nil, fmt.Errorf("failed to locate sector %v: %w", root, err)
	}
	defer release()

	m.mu.Lock()
	v, ok := m.volumes[loc.Volume]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("volume %v not found", loc.Volume)
	}
	m.mu.Unlock()
	return v.ReadSector(loc.Index)
}

// WriteSector writes a sector to a volume. The location is locked until release
// is called. If the sector already exists, the existing location is locked and
// returned. Metadata is not added to the store -- it is the callers
// responsibility to update the sector's metadata.
func (m *Manager) WriteSector(root SectorRoot, data []byte) (loc SectorLocation, release func() error, _ error) {
	loc, release, err := m.vs.StoreSector(root)
	if errors.Is(err, ErrSectorExists) {
		// if the sector already exists, keep the existing location locked and return it
		return loc, release, nil
	} else if err != nil {
		return SectorLocation{}, nil, fmt.Errorf("failed to store sector %v: %w", root, err)
	}

	// get the volume
	vol, err := m.volume(loc.Volume)
	if err != nil {
		return SectorLocation{}, nil, fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
	}

	// write the sector
	if err := vol.WriteSector(data, loc.Index); err != nil {
		return SectorLocation{}, nil, fmt.Errorf("failed to write sector %v: %w", root, err)
	}
	return loc, release, nil
}

func NewManager(vs VolumeStore, ss SectorStore) *Manager {
	return &Manager{
		vs: vs,
		ss: ss,

		volumes: make(map[VolumeID]*volume),
	}
}
