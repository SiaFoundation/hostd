package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
)

type (
	// A SectorLocation is a location of a sector within a volume.
	SectorLocation struct {
		ID     uint64
		Volume int
		Index  uint64
		Root   types.Hash256
	}

	// A VolumeManager manages storage using local volumes.
	VolumeManager struct {
		vs  VolumeStore
		log *zap.Logger

		tg *threadgroup.ThreadGroup

		mu      sync.Mutex // protects the following fields
		volumes map[int]*volume
		// changedVolumes tracks volumes that need to be fsynced
		changedVolumes map[int]bool
	}
)

// getVolume returns the volume with the given ID, or an error if the volume does
// not exist or is currently busy.
func (vm *VolumeManager) getVolume(v int) (*volume, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vol, ok := vm.volumes[v]
	if !ok {
		return nil, fmt.Errorf("volume %v not found", v)
	} else if vol.busy {
		return nil, fmt.Errorf("volume %v is currently busy", v)
	}
	return vol, nil
}

// lockVolume locks a volume for operations until release is called. A locked
// volume cannot have its size or status changed and no new sectors can be
// written to it.
func (vm *VolumeManager) lockVolume(id int) (func(), error) {
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

// writeSector writes a sector to a volume. The volume is not synced after the
// sector is written. The location is assumed to be empty and locked.
func (vm *VolumeManager) writeSector(data *[rhpv2.SectorSize]byte, loc SectorLocation) error {
	vol, err := vm.getVolume(loc.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	} else if err := vol.WriteSector(data, loc.Index); err != nil {
		return fmt.Errorf("failed to write sector data: %w", err)
	}
	vm.mu.Lock()
	vm.changedVolumes[loc.Volume] = true
	vm.mu.Unlock()
	return nil
}

// migrateSector migrates sectors to new locations. The sectors are read from
// their current locations and written to their new locations. Changed volumes
// are synced after all sectors have been written.
func (vm *VolumeManager) migrateSectors(locations []SectorLocation) error {
	for _, loc := range locations {
		// read the sector from the old location
		sector, err := vm.Read(loc.Root)
		if err != nil {
			return fmt.Errorf("failed to read sector %v: %w", loc.Root, err)
		} else if err := vm.writeSector(sector, loc); err != nil { // write the sector to the new location
			return fmt.Errorf("failed to write sector %v to %v:%v: %w", loc.Root, loc.Volume, loc.Index, err)
		}
	}
	return vm.Sync()
}

func (vm *VolumeManager) growVolume(ctx context.Context, id int, oldMaxSectors, newMaxSectors uint64) error {
	const batchSize = 256

	if oldMaxSectors > newMaxSectors {
		return errors.New("old sectors must be less than new sectors")
	}

	v, err := vm.getVolume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	for current := oldMaxSectors; current < newMaxSectors; current += batchSize {
		// stop early if the context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		target := current + batchSize
		if target > newMaxSectors {
			target = newMaxSectors
		}
		// truncate the file and add the indices to the volume store. resize is
		// done in chunks to prevent holding a lock for too long and to allow
		// progress tracking.
		if v.Resize(target); err != nil {
			return fmt.Errorf("failed to expand volume data: %w", err)
		} else if err := vm.vs.GrowVolume(id, target); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}
	}
	return nil
}

func (vm *VolumeManager) shrinkVolume(ctx context.Context, id int, oldMaxSectors, newMaxSectors uint64) error {
	if oldMaxSectors <= newMaxSectors {
		return errors.New("old sectors must be greater than new sectors")
	}

	volume, err := vm.getVolume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	// migrate any sectors outside of the target range. migrateSectors will be
	// called on chunks of 256 sectors
	err = vm.vs.MigrateSectors(id, newMaxSectors, vm.migrateSectors)
	if err != nil {
		return fmt.Errorf("failed to migrate sectors: %w", err)
	}

	var batchSize uint64 = 256
	for current := oldMaxSectors; current > newMaxSectors; current -= batchSize {
		// stop early if the context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var target uint64
		if current < batchSize {
			target = newMaxSectors
			batchSize = current - newMaxSectors
		} else {
			target = current - batchSize
		}
		// shrink in chunks to prevent holding a lock for too long and to
		// track progress.
		if err := vm.vs.ShrinkVolume(id, target); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		} else if err := volume.Resize(target); err != nil {
			return fmt.Errorf("failed to shrink volume data to %v sectors: %w", target, err)
		}
	}
	return nil
}

// Close gracefully shutsdown the volume manager.
func (vm *VolumeManager) Close() error {
	// wait for all operations to stop
	vm.tg.Stop()

	vm.mu.Lock()
	defer vm.mu.Unlock()
	// sync and close all open volumes
	for id, vol := range vm.volumes {
		if err := vol.Sync(); err != nil {
			vm.log.Error("failed to sync volume", zap.Int("id", id), zap.Error(err))
		} else if err := vol.Close(); err != nil {
			vm.log.Error("failed to close volume", zap.Int("id", id), zap.Error(err))
		}
		delete(vm.volumes, id)
	}
	return nil
}

// Usage returns the total and used storage space, in bytes, in the storage manager.
func (vm *VolumeManager) Usage() (usedBytes uint64, totalBytes uint64, err error) {
	done, err := vm.tg.Add()
	if err != nil {
		return 0, 0, err
	}
	defer done()
	return vm.vs.StorageUsage()
}

// Volumes returns a list of all volumes in the storage manager.
func (vm *VolumeManager) Volumes() ([]Volume, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()
	return vm.vs.Volumes()
}

// Volume returns a volume by its ID.
func (vm *VolumeManager) Volume(id int) (Volume, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return Volume{}, err
	}
	defer done()
	return vm.vs.Volume(id)
}

// AddVolume adds a new volume to the storage manager
func (vm *VolumeManager) AddVolume(localPath string, maxSectors uint64) (Volume, error) {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
	if err != nil {
		return Volume{}, err
	}
	defer cancel()

	f, err := os.Create(localPath)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to create volume file: %w", err)
	}

	volumeID, err := vm.vs.AddVolume(localPath, false)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to add volume to store: %w", err)
	}

	// add the new volume to the volume map
	vm.mu.Lock()
	vm.volumes[volumeID] = &volume{
		data: f,
	}
	vm.mu.Unlock()

	// lock the volume during grow operation
	release, err := vm.lockVolume(volumeID)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to lock volume: %w", err)
	}
	defer release()

	// grow the volume to the desired size
	if err := vm.growVolume(ctx, volumeID, 0, maxSectors); err != nil {
		return Volume{}, fmt.Errorf("failed to grow volume: %w", err)
	} else if err := vm.vs.SetAvailable(volumeID, true); err != nil {
		return Volume{}, fmt.Errorf("failed to set volume available: %w", err)
	}
	return vm.vs.Volume(volumeID)
}

// SetReadOnly sets the read-only status of a volume.
func (vm *VolumeManager) SetReadOnly(id int, readOnly bool) error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

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
func (vm *VolumeManager) RemoveVolume(id int, force bool) error {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	defer cancel()

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
	err = vm.vs.MigrateSectors(id, 0, func(locations []SectorLocation) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		return vm.migrateSectors(locations)
	})
	if err != nil {
		return fmt.Errorf("failed to migrate sector data: %w", err)
	}
	return vm.vs.RemoveVolume(id, force)
}

// ResizeVolume resizes a volume to the specified size.
func (vm *VolumeManager) ResizeVolume(id int, maxSectors uint64) error {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	defer cancel()

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
		if err := vm.shrinkVolume(ctx, id, vol.TotalSectors, maxSectors); err != nil {
			return fmt.Errorf("failed to shrink volume to %v sectors: %w", maxSectors, err)
		}
		return nil
	}
	// volume is growing
	if err := vm.growVolume(ctx, id, vol.TotalSectors, maxSectors); err != nil {
		return fmt.Errorf("failed to grow volume: %w", err)
	}
	return nil
}

// RemoveSector deletes a sector's metadata and zeroes its data.
func (vm *VolumeManager) RemoveSector(root types.Hash256) error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// get and lock the sector's current location
	loc, release, err := vm.vs.SectorLocation(root)
	if err != nil {
		return fmt.Errorf("failed to locate sector %v: %w", root, err)
	}
	defer release()

	// remove the sector from the volume store
	if err := vm.vs.RemoveSector(root); err != nil {
		return fmt.Errorf("failed to remove sector %v: %w", root, err)
	}

	// get the volume from memory
	vol, err := vm.getVolume(loc.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
	}

	// zero the sector and immediately sync the volume
	var zeroes [rhpv2.SectorSize]byte
	if err := vol.WriteSector(&zeroes, loc.Index); err != nil {
		return fmt.Errorf("failed to zero sector %v: %w", root, err)
	} else if err := vol.Sync(); err != nil {
		return fmt.Errorf("failed to sync volume %v: %w", loc.Volume, err)
	}
	return nil
}

// LockSector prevents the sector with the given root from being pruned. If the
// sector does not exist, an error is returned. Release must be called when the
// sector is no longer needed.
func (vm *VolumeManager) LockSector(root types.Hash256) (func() error, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()
	_, release, err := vm.vs.SectorLocation(root)
	return release, err
}

// Read reads the sector with the given root
func (vm *VolumeManager) Read(root types.Hash256) (*[rhpv2.SectorSize]byte, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

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
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	vm.mu.Lock()
	var toSync []int
	for id := range vm.changedVolumes {
		toSync = append(toSync, id)
	}
	vm.mu.Unlock()
	for _, id := range toSync {
		v, err := vm.getVolume(id)
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

// Write writes a sector to a volume. release should only be called after the
// contract roots have been committed to prevent the sector from being deleted.
func (vm *VolumeManager) Write(root types.Hash256, data *[rhpv2.SectorSize]byte) (release func() error, _ error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	return vm.vs.StoreSector(root, func(loc SectorLocation, exists bool) error {
		if exists {
			return nil
		}
		vol, err := vm.getVolume(loc.Volume)
		if err != nil {
			return fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
		} else if err := vol.WriteSector(data, loc.Index); err != nil {
			return fmt.Errorf("failed to write sector %v: %w", root, err)
		}
		return nil
	})
}

// loadVolumes opens all volumes. Volumes that are already loaded are skipped.
func (vm *VolumeManager) loadVolumes() error {
	done, err := vm.tg.Add()
	if err != nil {
		return fmt.Errorf("failed to add to threadgroup: %w", err)
	}
	defer done()

	volumes, err := vm.vs.Volumes()
	if err != nil {
		return fmt.Errorf("failed to load volumes: %w", err)
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	// load the volumes into memory
	for _, vol := range volumes {
		// skip volumes that are already loaded
		if vm.volumes[vol.ID] != nil {
			continue
		}

		// open the volume file
		f, err := os.Open(vol.LocalPath)
		if err != nil {
			// mark the volume as unavailable
			if err := vm.vs.SetAvailable(vol.ID, false); err != nil {
				return fmt.Errorf("failed to mark volume '%v' as unavailable: %w", vol.LocalPath, err)
			}
			vm.log.Error("unable to open volume", zap.Error(err), zap.Int("id", vol.ID), zap.String("path", vol.LocalPath))
		}
		// add the volume to the memory map
		vm.volumes[vol.ID] = &volume{
			data: f,
		}
		// mark the volume as available
		if err := vm.vs.SetAvailable(vol.ID, true); err != nil {
			return fmt.Errorf("failed to mark volume '%v' as available: %w", vol.LocalPath, err)
		}
	}
	return nil
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(vs VolumeStore, log *zap.Logger) (*VolumeManager, error) {
	vm := &VolumeManager{
		vs:  vs,
		log: log,

		volumes:        make(map[int]*volume),
		changedVolumes: make(map[int]bool),

		tg: threadgroup.New(),
	}
	if err := vm.loadVolumes(); err != nil {
		return nil, err
	}
	return vm, nil
}
