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
		ID     uint64
		Volume VolumeID
		Index  uint64
	}

	// A VolumeManager manages storage using local volumes.
	VolumeManager struct {
		vs VolumeStore

		mu      sync.Mutex // protects the following fields
		volumes map[VolumeID]*volume
		// changedVolumes tracks volumes that need to be fsynced
		changedVolumes map[VolumeID]bool
	}
)

// lockVolume locks a volume for operations until release is called. A locked
// volume cannot have its size or status changed and no new sectors can be
// written to it.
func (vm *VolumeManager) lockVolume(id VolumeID) (func(), error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	v, ok := vm.volumes[id]
	if !ok {
		return nil, fmt.Errorf("volume %v not found", id)
	} else if v.busy {
		return nil, fmt.Errorf("volume %v is busy", id)
	}
	return func() {
		vm.mu.Lock()
		vm.volumes[id].busy = false
		vm.mu.Unlock()
	}, nil
}

// migrateSector migrates a sector from one volume to another.
func (vm *VolumeManager) migrateSector(root SectorRoot, location SectorLocation) error {
	sector, err := vm.ReadSector(root)
	if err != nil {
		return fmt.Errorf("failed to read sector %v: %w", root, err)
	}

	newVolume, err := vm.volume(location.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume %v: %w", location.Volume, err)
	}

	// write the sector to the new volume
	if err := newVolume.WriteSector(sector, location.Index); err != nil {
		return fmt.Errorf("failed to write sector %v: %w", location, err)
	}
	return nil
}

func (vm *VolumeManager) growVolume(id VolumeID, oldMaxSectors, newMaxSectors uint64) error {
	if oldMaxSectors > newMaxSectors {
		return errors.New("old sectors must be less than new sectors")
	}

	const batchSize = 256
	v, err := vm.volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	for current := oldMaxSectors; current < newMaxSectors; current += batchSize {
		target := current + batchSize
		if target > newMaxSectors {
			target = newMaxSectors
		}
		// truncate the file and add the indices to the volume store. resize is
		// done in chunks to prevent holding a lock for too long and to allow
		// progress tracking.
		if v.Resize(current * sectorSize); err != nil {
			return fmt.Errorf("failed to expand volume data: %w", err)
		} else if err := vm.vs.GrowVolume(id, target); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}
	}
	return nil
}

func (vm *VolumeManager) shrinkVolume(id VolumeID, oldMaxSectors, newMaxSectors uint64) error {
	if oldMaxSectors <= newMaxSectors {
		return errors.New("old sectors must be greater than new sectors")
	}

	volume, err := vm.volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	// migrate any sectors outside of the new end of the volume
	err = vm.vs.MigrateSectors(id, newMaxSectors, vm.migrateSector, vm.Sync)
	if err != nil {
		return fmt.Errorf("failed to migrate sectors: %w", err)
	}

	current := oldMaxSectors - newMaxSectors
	n := uint64(256)
	for i := current; i > newMaxSectors; i -= n {
		if current > i-n {
			n = i - current
		}
		// shrink in chunks to prevent holding a lock for too long and to
		// track progress.
		if err := vm.vs.ShrinkVolume(id, n); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}
	}
	// resize the data file
	return volume.Resize(newMaxSectors * sectorSize)
}

// Volumes returns a list of all volumes in the storage manager.
func (vm *VolumeManager) Volumes() ([]Volume, error) {
	return vm.vs.Volumes()
}

// AddVolume adds a new volume to the storage manager
func (vm *VolumeManager) AddVolume(localPath string, maxSectors uint64) (Volume, error) {
	f, err := os.Create(localPath)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to create volume file: %w", err)
	}

	v, err := vm.vs.AddVolume(localPath, false)
	if err != nil {
		return v, fmt.Errorf("failed to add volume to store: %w", err)
	}

	// add the new volume to the volume map
	vm.mu.Lock()
	vm.volumes[v.ID] = &volume{
		data: f,
		stats: VolumeStats{
			Available: true,
		},
	}
	vm.mu.Unlock()

	// lock the volume during grow operation
	release, err := vm.lockVolume(v.ID)
	if err != nil {
		return v, fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// grow the volume to the desired size
	if err := vm.growVolume(v.ID, 0, maxSectors); err != nil {
		return v, fmt.Errorf("failed to grow volume: %w", err)
	}

	return v, nil
}

// SetReadOnly sets the read-only status of a volume.
func (vm *VolumeManager) SetReadOnly(id VolumeID, readOnly bool) error {
	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	if err := vm.vs.SetReadOnly(id, readOnly); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	return nil
}

// RemoveVolume removes a volume from the manager.
func (vm *VolumeManager) RemoveVolume(id VolumeID, force bool) error {
	// lock the volume during removal to prevent concurrent operations
	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	// migrate sectors to other volumes
	err = vm.vs.MigrateSectors(id, 0, vm.migrateSector, vm.Sync)
	if err != nil {
		return fmt.Errorf("failed to migrate sector data: %w", err)
	}
	return vm.vs.RemoveVolume(id, force)
}

// ResizeVolume resizes a volume to the specified size.
func (vm *VolumeManager) ResizeVolume(ctx context.Context, id VolumeID, maxSectors uint64) error {
	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}

	vol, err := vm.vs.Volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	if vol.TotalSectors == maxSectors {
		// volume is the same size, nothing to do
		return nil
	} else if vol.TotalSectors > maxSectors {
		// volume is shrinking
		if err := vm.shrinkVolume(id, vol.TotalSectors, maxSectors); err != nil {
			return fmt.Errorf("failed to shrink volume from volume: %w", err)
		}
		return nil
	}
	// volume is growing
	if err := vm.growVolume(id, vol.TotalSectors, maxSectors); err != nil {
		return fmt.Errorf("failed to grow volume: %w", err)
	}
	return nil
}

// RemoveSector deletes a sector's metadata and zeroes its data.
func (vm *VolumeManager) RemoveSector(root SectorRoot) error {
	loc, err := vm.vs.RemoveSector(root)
	if err != nil {
		return fmt.Errorf("failed to locate sector %v: %w", root, err)
	}

	// get the volume from memory
	vol, err := vm.volume(loc.Volume)
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
func (vm *VolumeManager) ReadSector(root SectorRoot) ([]byte, error) {
	loc, release, err := vm.vs.SectorLocation(root)
	if err != nil {
		return nil, fmt.Errorf("failed to locate sector %v: %w", root, err)
	}
	defer release()

	vm.mu.Lock()
	v, ok := vm.volumes[loc.Volume]
	if !ok {
		vm.mu.Unlock()
		return nil, fmt.Errorf("volume %v not found", loc.Volume)
	}
	vm.mu.Unlock()
	return v.ReadSector(loc.Index)
}

// Sync syncs the data files of changed volumes.
func (vm *VolumeManager) Sync() error {
	vm.mu.Lock()
	var toSync []VolumeID
	for id := range vm.changedVolumes {
		toSync = append(toSync, id)
	}
	vm.mu.Unlock()
	for _, id := range toSync {
		v, err := vm.volume(id)
		if err != nil {
			return fmt.Errorf("failed to get volume %v: %w", id, err)
		} else if err := v.Sync(); err != nil {
			return fmt.Errorf("failed to sync volume %v: %w", id, err)
		}
		vm.mu.Lock()
		delete(vm.changedVolumes, id)
		vm.mu.Unlock()
	}
	return nil
}

// WriteSector writes a sector to a volume. release should only be called after the
// contract roots have been committed to prevent the sector from being deleted.
func (vm *VolumeManager) WriteSector(root SectorRoot, data []byte) (release func() error, _ error) {
	return vm.vs.StoreSector(root, func(loc SectorLocation, exists bool) error {
		if exists {
			return nil
		}
		vol, err := vm.volume(loc.Volume)
		if err != nil {
			return fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
		} else if err := vol.WriteSector(data, loc.Index); err != nil {
			return fmt.Errorf("failed to write sector %v: %w", root, err)
		}
		return nil
	})
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(vs VolumeStore) *VolumeManager {
	return &VolumeManager{
		vs: vs,

		volumes:        make(map[VolumeID]*volume),
		changedVolumes: make(map[VolumeID]bool),
	}
}
