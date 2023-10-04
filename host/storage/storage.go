package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	resizeBatchSize = 64 // 256 MiB

	// MaxTempSectorBlocks is the maximum number of blocks that a temp sector
	// can be stored for.
	MaxTempSectorBlocks = 144 * 7 // 7 days
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
	// Alerts can be used to register alerts.
	Alerts interface {
		Register(alerts.Alert)
		Dismiss(...types.Hash256)
	}

	// A ChainManager is used to get the current consensus state.
	ChainManager interface {
		TipState() consensus.State
		Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error
	}

	// A SectorLocation is a location of a sector within a volume.
	SectorLocation struct {
		ID     int64
		Volume int64
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
		cacheHits   uint64 // ensure 64-bit alignment on 32-bit systems
		cacheMisses uint64

		a        Alerts
		vs       VolumeStore
		cm       ChainManager
		log      *zap.Logger
		recorder *sectorAccessRecorder

		tg *threadgroup.ThreadGroup

		mu          sync.Mutex // protects the following fields
		lastCleanup time.Time
		volumes     map[int64]*volume
		// changedVolumes tracks volumes that need to be fsynced
		changedVolumes map[int64]bool
		cache          *lru.Cache[types.Hash256, *[rhp2.SectorSize]byte] // Added cache
	}
)

// getVolume returns the volume with the given ID, or an error if the volume does
// not exist or is currently busy.
func (vm *VolumeManager) getVolume(v int64) (*volume, error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vol, ok := vm.volumes[v]
	if !ok {
		return nil, fmt.Errorf("volume %v not found", v)
	}
	return vol, nil
}

// lockVolume locks a volume for operations until release is called. A locked
// volume cannot have its size or status changed and no new sectors can be
// written to it.
func (vm *VolumeManager) lockVolume(id int64) (func(), error) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	v, ok := vm.volumes[id]
	if !ok {
		return nil, fmt.Errorf("volume %v not found", id)
	} else if v.busy {
		return nil, fmt.Errorf("volume %v is busy", id)
	}
	v.busy = true
	var once sync.Once
	return func() {
		once.Do(func() {
			vm.mu.Lock()
			v, ok := vm.volumes[id]
			if ok {
				v.busy = false
			}
			vm.mu.Unlock()
		})
	}, nil
}

// writeSector writes a sector to a volume. The location is assumed to be empty
// and locked.
func (vm *VolumeManager) writeSector(data *[rhp2.SectorSize]byte, loc SectorLocation, sync bool) error {
	vol, err := vm.getVolume(loc.Volume)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	} else if err := vol.WriteSector(data, loc.Index); err != nil {
		return fmt.Errorf("failed to write sector data: %w", err)
	}

	if sync {
		return vm.Sync()
	}

	vm.mu.Lock()
	vm.changedVolumes[loc.Volume] = true
	vm.mu.Unlock()
	return nil
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
		// if the volume has not been loaded yet, create a new volume
		v := vm.volumes[vol.ID]
		if v == nil {
			v = &volume{
				stats: VolumeStats{
					Status: VolumeStatusUnavailable,
				},
			}
			vm.volumes[vol.ID] = v
		}

		if err := v.OpenVolume(vol.LocalPath, false); err != nil {
			v.appendError(fmt.Errorf("failed to open volume: %w", err))
			vm.log.Error("unable to open volume", zap.Error(err), zap.Int64("id", vol.ID), zap.String("path", vol.LocalPath))
			// mark the volume as unavailable
			if err := vm.vs.SetAvailable(vol.ID, false); err != nil {
				return fmt.Errorf("failed to mark volume '%v' as unavailable: %w", vol.LocalPath, err)
			}

			// register an alert
			vm.a.Register(alerts.Alert{
				ID:       frand.Entropy256(),
				Severity: alerts.SeverityError,
				Message:  "Failed to open volume",
				Data: map[string]any{
					"volume": vol.LocalPath,
					"error":  err.Error(),
				},
				Timestamp: time.Now(),
			})

			continue
		}
		// mark the volume as available
		if err := vm.vs.SetAvailable(vol.ID, true); err != nil {
			return fmt.Errorf("failed to mark volume '%v' as available: %w", vol.LocalPath, err)
		}
		v.SetStatus(VolumeStatusReady)
		vm.log.Debug("loaded volume", zap.Int64("id", vol.ID), zap.String("path", vol.LocalPath))
	}
	return nil
}

// migrateSector migrates a sector to a new location. The sector is read from
// its current location and written to its new location. The volume is
// immediately synced after the sector is written.
func (vm *VolumeManager) migrateSector(loc SectorLocation, log *zap.Logger) error {
	// read the sector from the old location
	sector, err := vm.Read(loc.Root)
	if err != nil {
		return fmt.Errorf("failed to read sector: %w", err)
	}
	// calculate the returned root
	root := rhp2.SectorRoot(sector)
	// verify the the sector is not corrupt
	if root != loc.Root {
		return fmt.Errorf("sector corrupt: %v != %v", loc.Root, root)
	}

	// write the sector to the new location
	return vm.writeSector(sector, loc, true)
}

// growVolume grows a volume by adding sectors to the end of the volume.
func (vm *VolumeManager) growVolume(ctx context.Context, id int64, volume *volume, oldMaxSectors, newMaxSectors uint64) error {
	if oldMaxSectors > newMaxSectors {
		return errors.New("old sectors must be less than new sectors")
	}

	// register an alert
	alert := alerts.Alert{
		ID:       frand.Entropy256(),
		Message:  "Growing volume",
		Severity: alerts.SeverityInfo,
		Data: map[string]any{
			"volumeID":       id,
			"oldSectors":     oldMaxSectors,
			"currentSectors": oldMaxSectors,
			"targetSectors":  newMaxSectors,
		},
		Timestamp: time.Now(),
	}
	vm.a.Register(alert)
	// dismiss the alert when the function returns. It is the caller's
	// responsibility to register a completion alert
	defer vm.a.Dismiss(alert.ID)

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
		if err := volume.Resize(current, target); err != nil {
			return fmt.Errorf("failed to expand volume data: %w", err)
		} else if err := vm.vs.GrowVolume(id, target); err != nil {
			return fmt.Errorf("failed to expand volume metadata: %w", err)
		}

		// update the alert
		alert.Data["currentSectors"] = target
		vm.a.Register(alert)
		// sleep to allow other operations to run
		time.Sleep(time.Millisecond)
	}
	return nil
}

// shrinkVolume shrinks a volume by removing sectors from the end of the volume.
func (vm *VolumeManager) shrinkVolume(ctx context.Context, id int64, volume *volume, oldMaxSectors, newMaxSectors uint64) error {
	log := vm.log.Named("shrinkVolume").With(zap.Int64("volumeID", id), zap.Uint64("oldMaxSectors", oldMaxSectors), zap.Uint64("newMaxSectors", newMaxSectors))
	if oldMaxSectors <= newMaxSectors {
		return errors.New("old sectors must be greater than new sectors")
	}

	// register the alert
	a := alerts.Alert{
		ID:       frand.Entropy256(),
		Message:  "Shrinking volume",
		Severity: alerts.SeverityInfo,
		Data: map[string]any{
			"volumeID":        id,
			"oldSectors":      oldMaxSectors,
			"currentSectors":  oldMaxSectors,
			"targetSectors":   newMaxSectors,
			"migratedSectors": 0,
		},
		Timestamp: time.Now(),
	}
	vm.a.Register(a)
	// dismiss the alert when the function returns. It is the caller's
	// responsibility to register a completion alert
	defer vm.a.Dismiss(a.ID)

	// migrate any sectors outside of the target range.
	var migrated int
	err := vm.vs.MigrateSectors(id, newMaxSectors, func(newLoc SectorLocation) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := vm.migrateSector(newLoc, log.Named("migrate")); err != nil {
			return err
		}
		migrated++
		// update the alert
		a.Data["migratedSectors"] = migrated
		vm.a.Register(a)
		return nil
	})
	log.Info("migrated sectors", zap.Int("count", migrated))
	if err != nil {
		return err
	}

	for current := oldMaxSectors; current > newMaxSectors; {
		// stop early if the context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var target uint64
		if current > resizeBatchSize {
			target = current - resizeBatchSize
			if target < newMaxSectors {
				target = newMaxSectors
			}
		} else {
			target = newMaxSectors
		}

		if err := vm.vs.ShrinkVolume(id, target); err != nil {
			return fmt.Errorf("failed to shrink volume metadata: %w", err)
		} else if err := volume.Resize(current, target); err != nil {
			return fmt.Errorf("failed to shrink volume data to %v sectors: %w", current, err)
		}

		current = target
		// update the alert
		a.Data["currentSectors"] = current
		vm.a.Register(a)
		// sleep to allow other operations to run
		time.Sleep(time.Millisecond)
	}
	return nil
}

func (vm *VolumeManager) volumeStats(id int64) (vs VolumeStats) {
	v, ok := vm.volumes[id]
	if !ok {
		vs.Status = "unavailable"
	} else {
		vs = v.stats
	}
	return
}

func (vm *VolumeManager) setVolumeStatus(id int64, status string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	v, ok := vm.volumes[id]
	if !ok {
		return
	}
	v.stats.Status = status
}

func (vm *VolumeManager) doResize(ctx context.Context, volumeID int64, vol *volume, current, target uint64) error {
	ctx, cancel, err := vm.tg.AddContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	switch {
	case current > target:
		// volume is shrinking
		return vm.shrinkVolume(ctx, volumeID, vol, current, target)
	case current < target:
		// volume is growing
		return vm.growVolume(ctx, volumeID, vol, current, target)
	}
	return nil
}

func (vm *VolumeManager) migrateForRemoval(ctx context.Context, id int64, localPath string, force bool, log *zap.Logger) (int, error) {
	ctx, cancel, err := vm.tg.AddContext(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()

	// add an alert for the migration
	a := alerts.Alert{
		ID:       frand.Entropy256(),
		Message:  "Migrating sectors",
		Severity: alerts.SeverityInfo,
		Data: map[string]interface{}{
			"volumeID": id,
			"migrated": 0,
			"force":    force,
		},
		Timestamp: time.Now(),
	}
	vm.a.Register(a)
	// dismiss the alert when the function returns. It is the caller's
	// responsibility to register a completion alert
	defer vm.a.Dismiss(a.ID)

	// migrate sectors to other volumes
	var migrated, failed int
	err = vm.vs.MigrateSectors(id, 0, func(newLoc SectorLocation) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := vm.migrateSector(newLoc, log.Named("migrate")); err != nil {
			log.Error("failed to migrate sector", zap.Stringer("sectorRoot", newLoc.Root), zap.Error(err))
			if force {
				failed++
				a.Data["failed"] = failed
				return nil
			}
			return err
		}
		migrated++
		// update the alert
		a.Data["migrated"] = migrated
		vm.a.Register(a)
		return nil
	})
	if err != nil {
		return migrated, fmt.Errorf("failed to migrate sector data: %w", err)
	} else if err := vm.vs.RemoveVolume(id); err != nil {
		return migrated, fmt.Errorf("failed to remove volume: %w", err)
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()
	// close the volume
	vm.volumes[id].Close()
	// delete the volume from memory
	delete(vm.volumes, id)
	// remove the volume file, ignore error if the file does not exist
	if err := os.Remove(localPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return migrated, fmt.Errorf("failed to remove volume file: %w", err)
	}
	return migrated, nil
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
			vm.log.Error("failed to sync volume", zap.Int64("id", id), zap.Error(err))
		} else if err := vol.Close(); err != nil {
			vm.log.Error("failed to close volume", zap.Int64("id", id), zap.Error(err))
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
func (vm *VolumeManager) Volume(id int64) (VolumeMeta, error) {
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
func (vm *VolumeManager) AddVolume(ctx context.Context, localPath string, maxSectors uint64, result chan<- error) (Volume, error) {
	if maxSectors == 0 {
		return Volume{}, errors.New("max sectors must be greater than 0")
	}

	done, err := vm.tg.Add()
	if err != nil {
		return Volume{}, err
	}
	defer done()

	// check that the volume file does not already exist
	if _, err := os.Stat(localPath); err == nil {
		return Volume{}, fmt.Errorf("volume file already exists: %s", localPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Volume{}, fmt.Errorf("failed to stat volume file: %w", err)
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
	vol := &volume{
		data: f,
		stats: VolumeStats{
			Status: VolumeStatusCreating,
		},
	}
	vm.volumes[volumeID] = vol
	vm.mu.Unlock()

	// lock the volume during grow operation
	release, err := vm.lockVolume(volumeID)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to lock volume: %w", err)
	}

	go func() {
		log := vm.log.Named("initialize").With(zap.Int64("volumeID", volumeID), zap.Uint64("maxSectors", maxSectors))
		start := time.Now()
		err := func() error {
			defer vm.vs.SetAvailable(volumeID, true)
			defer vm.setVolumeStatus(volumeID, VolumeStatusReady)
			defer release()
			return vm.doResize(ctx, volumeID, vol, 0, maxSectors)
		}()
		alert := alerts.Alert{
			ID: frand.Entropy256(),
			Data: map[string]interface{}{
				"volumeID": volumeID,
				"elapsed":  time.Since(start),
				"target":   maxSectors,
			},
			Timestamp: time.Now(),
		}
		if err != nil {
			log.Error("failed to initialize volume", zap.Error(err))
			alert.Message = "Failed to initialize volume"
			alert.Severity = alerts.SeverityError
			alert.Data["error"] = err.Error()
		} else {
			alert.Message = "Volume initialized"
			alert.Severity = alerts.SeverityInfo
		}
		vm.a.Register(alert)

		select {
		case result <- err:
		default:
		}
	}()
	return vm.vs.Volume(volumeID)
}

// SetReadOnly sets the read-only status of a volume.
func (vm *VolumeManager) SetReadOnly(id int64, readOnly bool) error {
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
func (vm *VolumeManager) RemoveVolume(ctx context.Context, id int64, force bool, result chan<- error) error {
	log := vm.log.Named("remove").With(zap.Int64("volumeID", id))
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	// lock the volume during removal to prevent concurrent operations
	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}

	vol, err := vm.vs.Volume(id)
	if err != nil {
		release()
		return fmt.Errorf("failed to get volume: %w", err)
	}

	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		release()
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}

	go func() {
		start := time.Now()
		migrated, err := func() (int, error) {
			vm.setVolumeStatus(id, VolumeStatusRemoving)
			defer vm.setVolumeStatus(id, VolumeStatusReady)
			defer release()
			return vm.migrateForRemoval(ctx, id, vol.LocalPath, force, log)
		}()

		alert := alerts.Alert{
			ID: frand.Entropy256(),
			Data: map[string]interface{}{
				"volumeID":        id,
				"elapsed":         time.Since(start),
				"migratedSectors": migrated,
			},
			Timestamp: time.Now(),
		}
		if err != nil {
			alert.Message = "Volume removal failed"
			alert.Severity = alerts.SeverityError
			alert.Data["error"] = err.Error()
		} else {
			alert.Message = "Volume removed"
			alert.Severity = alerts.SeverityInfo
		}
		vm.a.Register(alert)

		select {
		case result <- err:
		default:
		}
	}()

	return nil
}

// ResizeVolume resizes a volume to the specified size.
func (vm *VolumeManager) ResizeVolume(ctx context.Context, id int64, maxSectors uint64, result chan<- error) error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	vol, err := vm.getVolume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
	}

	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	vm.setVolumeStatus(id, VolumeStatusResizing)

	stat, err := vm.vs.Volume(id)
	if err != nil {
		release()
		return fmt.Errorf("failed to get volume: %w", err)
	}

	oldReadonly := stat.ReadOnly
	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		release()
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}

	go func() {
		log := vm.log.Named("resize").With(zap.Int64("volumeID", id))
		start := time.Now()
		err := func() error {
			defer func() {
				// restore the volume to its original read-only status
				if err := vm.vs.SetReadOnly(id, oldReadonly); err != nil {
					log.Error("failed to restore volume read-only status", zap.Error(err))
				}
				vm.setVolumeStatus(id, VolumeStatusReady)
			}()
			defer release()
			return vm.doResize(ctx, id, vol, stat.TotalSectors, maxSectors)
		}()
		alert := alerts.Alert{
			ID: frand.Entropy256(),
			Data: map[string]interface{}{
				"volumeID":      id,
				"elapsed":       time.Since(start),
				"targetSectors": maxSectors,
			},
			Timestamp: time.Now(),
		}
		if err != nil {
			log.Error("failed to resize volume", zap.Error(err))
			alert.Message = "Volume resize failed"
			alert.Severity = alerts.SeverityError
			alert.Data["error"] = err.Error()
		} else {
			alert.Message = "Volume resized"
			alert.Severity = alerts.SeverityInfo
		}
		vm.a.Register(alert)
		select {
		case result <- err:
		default:
		}
	}()
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
	var zeroes [rhp2.SectorSize]byte
	if err := vol.WriteSector(&zeroes, loc.Index); err != nil {
		return fmt.Errorf("failed to zero sector %v: %w", root, err)
	} else if err := vol.Sync(); err != nil {
		return fmt.Errorf("failed to sync volume %v: %w", loc.Volume, err)
	}

	// eject the sector from the cache
	vm.cache.Remove(root)
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

// CacheStats returns the number of cache hits and misses.
func (vm *VolumeManager) CacheStats() (hits, misses uint64) {
	return atomic.LoadUint64(&vm.cacheHits), atomic.LoadUint64(&vm.cacheMisses)
}

// Read reads the sector with the given root
func (vm *VolumeManager) Read(root types.Hash256) (*[rhp2.SectorSize]byte, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()

	// Check the cache first
	if sector, ok := vm.cache.Get(root); ok {
		vm.recorder.AddCacheHit()
		atomic.AddUint64(&vm.cacheHits, 1)
		return sector, nil
	}

	// Cache miss, read from disk
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

	// Add sector to cache
	vm.cache.Add(root, sector)
	vm.recorder.AddCacheMiss()
	atomic.AddUint64(&vm.cacheMisses, 1)
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
	var toSync []int64
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
func (vm *VolumeManager) Write(root types.Hash256, data *[rhp2.SectorSize]byte) (func() error, error) {
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
		if err := vm.writeSector(data, loc, false); err != nil {
			return err
		}
		vm.log.Debug("wrote sector", zap.String("root", root.String()), zap.Int64("volume", loc.Volume), zap.Uint64("index", loc.Index), zap.Duration("elapsed", time.Since(start)))
		// Add newly written sector to cache
		vm.cache.Add(root, data)
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
	if len(sectors) == 0 {
		return nil
	}

	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	return vm.vs.AddTemporarySectors(sectors)
}

// ResizeCache resizes the cache to the given size.
func (vm *VolumeManager) ResizeCache(size uint32) {
	// Resize the underlying cache data structure
	vm.cache.Resize(int(size))
}

// ProcessConsensusChange is called when the consensus set changes.
func (vm *VolumeManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	delta := time.Since(vm.lastCleanup)
	if delta < cleanupInterval {
		return
	}
	vm.lastCleanup = time.Now()

	go func() {
		log := vm.log.Named("cleanup").With(zap.Uint64("height", uint64(cc.BlockHeight)))
		if err := vm.vs.ExpireTempSectors(uint64(cc.BlockHeight)); err != nil {
			log.Error("failed to expire temp sectors", zap.Error(err))
		}
	}()
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(vs VolumeStore, a Alerts, cm ChainManager, log *zap.Logger, sectorCacheSize uint32) (*VolumeManager, error) {
	// Initialize cache with LRU eviction and a max capacity of 64
	cache, err := lru.New[types.Hash256, *[rhp2.SectorSize]byte](64)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}
	// resize the cache, prevents an error in lru.New when initializing the
	// cache to 0
	cache.Resize(int(sectorCacheSize))

	vm := &VolumeManager{
		vs:  vs,
		a:   a,
		cm:  cm,
		log: log,
		recorder: &sectorAccessRecorder{
			store: vs,
			log:   log.Named("recorder"),
		},

		volumes:        make(map[int64]*volume),
		changedVolumes: make(map[int64]bool),
		cache:          cache,
		tg:             threadgroup.New(),
	}
	if err := vm.loadVolumes(); err != nil {
		return nil, err
	} else if err := vm.cm.Subscribe(vm, modules.ConsensusChangeRecent, vm.tg.Done()); err != nil {
		return nil, fmt.Errorf("failed to subscribe to consensus set: %w", err)
	}
	go vm.recorder.Run(vm.tg.Done())
	return vm, nil
}
