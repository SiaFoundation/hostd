package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
)

const (
	resizeBatchSize = (1 << 30) / rhpv2.SectorSize // 1 GiB

	cleanupInterval = 10 * time.Minute
)

// VolumeStatus is the status of a volume.
const (
	VolumeStatusUnavailable = "unavailable"
	VolumeStatusCreating    = "creating"
	VolumeStatusResizing    = "resizing"
	VolumeStatusRemoving    = "removing"
	VolumeStatusReady       = "ready"
)

type (
	// A ChainManager is used to get the current consensus state.
	ChainManager interface {
		TipState() consensus.State
	}

	// A SectorLocation is a location of a sector within a volume.
	SectorLocation struct {
		ID     int64
		Volume int
		Index  uint64
		Root   types.Hash256
	}

	// A TempSector is a stored sector that is not attached to a contract. It
	// will be deleted after the expiration height unless it is appended to a
	// contract.
	TempSector struct {
		Root       types.Hash256
		Expiration uint64
	}

	// A VolumeManager manages storage using local volumes.
	VolumeManager struct {
		vs       VolumeStore
		cm       ChainManager
		log      *zap.Logger
		recorder *sectorAccessRecorder

		tg         *threadgroup.ThreadGroup
		cleanTimer *time.Timer

		mu      sync.Mutex // protects the following fields
		volumes map[int]*volume
		// changedVolumes tracks volumes that need to be fsynced
		changedVolumes map[int]bool
	}

	// VolumeMeta contains the metadata of a volume.
	VolumeMeta struct {
		Volume
		VolumeStats
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
	var once sync.Once
	return func() {
		once.Do(func() {
			vm.mu.Lock()
			if vm.volumes[id] != nil {
				vm.volumes[id].busy = false
			}
			vm.mu.Unlock()
		})
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

// cleanup removes all sectors that are not referenced by a contract.
// This function is called periodically by the cleanup timer.
func (vm *VolumeManager) cleanup() {
	if err := vm.PruneSectors(); errors.Is(err, threadgroup.ErrClosed) {
		return
	} else if err != nil {
		vm.log.Error("failed to expire temp sectors", zap.Error(err))
	}
	// reset the timer
	vm.cleanTimer.Reset(cleanupInterval)
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
		f, err := os.OpenFile(vol.LocalPath, os.O_RDWR, 0700)
		if err != nil {
			vm.log.Error("unable to open volume", zap.Error(err), zap.Int("id", vol.ID), zap.String("path", vol.LocalPath))
			// mark the volume as unavailable
			if err := vm.vs.SetAvailable(vol.ID, false); err != nil {
				f.Close()
				return fmt.Errorf("failed to mark volume '%v' as unavailable: %w", vol.LocalPath, err)
			}
			continue
		}
		// add the volume to the memory map
		vm.volumes[vol.ID] = &volume{
			data: f,
			stats: VolumeStats{
				Status: VolumeStatusReady,
			},
		}
		// mark the volume as available
		if err := vm.vs.SetAvailable(vol.ID, true); err != nil {
			return fmt.Errorf("failed to mark volume '%v' as available: %w", vol.LocalPath, err)
		}
		vm.log.Debug("loaded volume", zap.Int("id", vol.ID), zap.String("path", vol.LocalPath))
	}
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

// growVolume grows a volume by adding sectors to the end of the volume.
func (vm *VolumeManager) growVolume(ctx context.Context, id int, oldMaxSectors, newMaxSectors uint64) error {
	if oldMaxSectors > newMaxSectors {
		return errors.New("old sectors must be less than new sectors")
	}

	v, err := vm.getVolume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	for current := oldMaxSectors; current < newMaxSectors; current += resizeBatchSize {
		// stop early if the context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		target := current + resizeBatchSize
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

// shrinkVolume shrinks a volume by removing sectors from the end of the volume.
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

	var batchSize uint64 = resizeBatchSize
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

func (vm *VolumeManager) volumeStats(id int) (vs VolumeStats) {
	v, ok := vm.volumes[id]
	if !ok {
		vs.Status = "unavailable"
	} else {
		vs = v.stats
	}
	return
}

func (vm *VolumeManager) setVolumeStatus(id int, status string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	v, ok := vm.volumes[id]
	if !ok {
		return
	}
	v.stats.Status = status
}

// Close gracefully shutsdown the volume manager.
func (vm *VolumeManager) Close() error {
	// wait for all operations to stop
	vm.tg.Stop()

	vm.mu.Lock()
	defer vm.mu.Unlock()
	// flush any pending metrics
	vm.recorder.Flush()
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

// Usage returns the total and used storage space, in sectors, from the storage manager.
func (vm *VolumeManager) Usage() (usedSectors uint64, totalSectors uint64, err error) {
	done, err := vm.tg.Add()
	if err != nil {
		return 0, 0, err
	}
	defer done()
	return vm.vs.StorageUsage()
}

// Volumes returns a list of all volumes in the storage manager.
func (vm *VolumeManager) Volumes() ([]VolumeMeta, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	volumes, err := vm.vs.Volumes()
	if err != nil {
		return nil, fmt.Errorf("failed to get volumes: %w", err)
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()
	var results []VolumeMeta
	for _, vol := range volumes {
		meta := VolumeMeta{
			Volume:      vol,
			VolumeStats: vm.volumeStats(vol.ID),
		}
		results = append(results, meta)
	}

	return results, nil
}

// Volume returns a volume by its ID.
func (vm *VolumeManager) Volume(id int) (VolumeMeta, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return VolumeMeta{}, err
	}
	defer done()

	vol, err := vm.vs.Volume(id)
	if err != nil {
		return VolumeMeta{}, fmt.Errorf("failed to get volume: %w", err)
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	return VolumeMeta{
		Volume:      vol,
		VolumeStats: vm.volumeStats(vol.ID),
	}, nil
}

// AddVolume adds a new volume to the storage manager
func (vm *VolumeManager) AddVolume(localPath string, maxSectors uint64) (Volume, error) {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
	if err != nil {
		return Volume{}, err
	}
	defer cancel()

	// check that the volume file does not already exist
	if _, err := os.Stat(localPath); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return Volume{}, fmt.Errorf("failed to stat volume file: %w", err)
		}
		return Volume{}, fmt.Errorf("volume file already exists: %s", localPath)
	}

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
		stats: VolumeStats{
			Status: VolumeStatusCreating,
		},
	}
	vm.mu.Unlock()
	defer vm.setVolumeStatus(volumeID, VolumeStatusReady)

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

	vol, err := vm.vs.Volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}
	vm.setVolumeStatus(id, VolumeStatusRemoving)
	defer vm.setVolumeStatus(id, VolumeStatusUnavailable)

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
	} else if err := vm.vs.RemoveVolume(id, force); err != nil {
		return fmt.Errorf("failed to remove volume: %w", err)
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	// close the volume
	vm.volumes[id].Close()
	// delete the volume from memory
	delete(vm.volumes, id)
	// remove the volume file
	if err := os.Remove(vol.LocalPath); err != nil {
		return fmt.Errorf("failed to remove volume file: %w", err)
	}
	return nil
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

	vol, err := vm.vs.Volume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	vm.setVolumeStatus(id, VolumeStatusResizing)
	defer vm.setVolumeStatus(id, VolumeStatusReady)

	oldReadonly := vol.ReadOnly
	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}

	defer func() {
		// restore the volume to its original read-only status
		if err := vm.vs.SetReadOnly(id, oldReadonly); err != nil {
			vm.log.Named("resize").Error("failed to restore volume read-only status", zap.Error(err), zap.Int("volumeID", id))
		}
	}()

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
	sector, err := v.ReadSector(loc.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to read sector %v: %w", root, err)
	}
	vm.recorder.AddRead()
	return sector, nil
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
func (vm *VolumeManager) Write(root types.Hash256, data *[rhpv2.SectorSize]byte) (func() error, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()
	release, err := vm.vs.StoreSector(root, func(loc SectorLocation, exists bool) error {
		if exists {
			return nil
		}
		start := time.Now()
		vol, err := vm.getVolume(loc.Volume)
		if err != nil {
			return fmt.Errorf("failed to get volume %v: %w", loc.Volume, err)
		} else if err := vol.WriteSector(data, loc.Index); err != nil {
			return fmt.Errorf("failed to write sector %v: %w", root, err)
		}
		vm.log.Debug("wrote sector", zap.String("root", root.String()), zap.Int("volume", loc.Volume), zap.Uint64("index", loc.Index), zap.Duration("elapsed", time.Since(start)))
		return nil
	})
	if err == nil {
		vm.recorder.AddWrite()
	}
	return release, err
}

// AddTemporarySectors adds sectors to the temporary store. The sectors are not
// referenced by a contract and will be removed at the expiration height.
func (vm *VolumeManager) AddTemporarySectors(sectors []TempSector) error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	return vm.vs.AddTemporarySectors(sectors)
}

// PruneSectors removes expired sectors from the volume store.
func (vm *VolumeManager) PruneSectors() error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()
	// expire temp sectors
	currentHeight := vm.cm.TipState().Index.Height
	if err := vm.vs.ExpireTempSectors(currentHeight); err != nil {
		return fmt.Errorf("failed to expire temp sectors: %w", err)
	}

	// prune sectors
	if err := vm.vs.PruneSectors(); err != nil {
		return fmt.Errorf("failed to prune sectors: %w", err)
	}
	return nil
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(vs VolumeStore, cm ChainManager, log *zap.Logger) (*VolumeManager, error) {
	vm := &VolumeManager{
		vs:  vs,
		cm:  cm,
		log: log,
		recorder: &sectorAccessRecorder{
			store: vs,
			log:   log.Named("recorder"),
		},

		volumes:        make(map[int]*volume),
		changedVolumes: make(map[int]bool),

		tg: threadgroup.New(),
	}
	if err := vm.loadVolumes(); err != nil {
		return nil, err
	}
	go vm.recorder.Run(vm.tg.Done())
	vm.cleanTimer = time.AfterFunc(cleanupInterval, vm.cleanup)
	return vm, nil
}
