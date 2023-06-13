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
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	resizeBatchSize = 64 // 256 MiB

	cleanupInterval = 5 * time.Minute
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
		a        Alerts
		vs       VolumeStore
		cm       ChainManager
		log      *zap.Logger
		recorder *sectorAccessRecorder

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
	t := time.NewTicker(cleanupInterval)
	defer t.Stop()
	for range t.C {
		count, err := vm.PruneSectors()
		if errors.Is(err, threadgroup.ErrClosed) {
			return
		} else if err != nil {
			vm.log.Error("failed to expire temp sectors", zap.Error(err))
		}
		vm.log.Named("cleanup").Debug("deleted unused sectors", zap.Int("deleted", count))
	}
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
			vm.log.Error("unable to open volume", zap.Error(err), zap.Int("id", vol.ID), zap.String("path", vol.LocalPath))
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
		vm.log.Debug("loaded volume", zap.Int("id", vol.ID), zap.String("path", vol.LocalPath))
	}
	return nil
}

// migrateSector migrates sectors to new locations. The sectors are read from
// their current locations and written to their new locations. Changed volumes
// are synced after all sectors have been written.
func (vm *VolumeManager) migrateSectors(locations []SectorLocation, force bool, log *zap.Logger) (migrated int, _ error) {
	for _, loc := range locations {
		err := func() error {
			// read the sector from the old location
			sector, err := vm.Read(loc.Root)
			if err != nil {
				return fmt.Errorf("failed to read sector: %w", err)
			}
			// calculate the returned root
			root := rhpv2.SectorRoot(sector)
			if root != loc.Root {
				return fmt.Errorf("sector corrupt: %v != %v", loc.Root, root)
			}
			if err := vm.writeSector(sector, loc); err != nil { // write the sector to the new location
				return fmt.Errorf("failed to write sector: %w", err)
			}
			return nil
		}()
		if err != nil {
			log.Error("failed to migrate sector", zap.Error(err), zap.Stringer("root", loc.Root), zap.Int("newVolumeID", loc.Volume), zap.Uint64("newIndex", loc.Index))
			if force {
				continue
			}
			return migrated, fmt.Errorf("failed to migrate sector %v: %w", loc.Root, err)
		}
		migrated++
	}
	return migrated, vm.Sync()
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
		if v.Resize(target); err != nil {
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
func (vm *VolumeManager) shrinkVolume(ctx context.Context, id int, oldMaxSectors, newMaxSectors uint64) error {
	log := vm.log.Named("shrinkVolume").With(zap.Int("volumeID", id), zap.Uint64("oldMaxSectors", oldMaxSectors), zap.Uint64("newMaxSectors", newMaxSectors))
	if oldMaxSectors <= newMaxSectors {
		return errors.New("old sectors must be greater than new sectors")
	}

	volume, err := vm.getVolume(id)
	if err != nil {
		return fmt.Errorf("failed to get volume: %w", err)
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

	// migrate any sectors outside of the target range. migrateSectors will be
	// called on chunks of 64 sectors
	var migrated int
	err = vm.vs.MigrateSectors(id, newMaxSectors, func(newLocations []SectorLocation) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := vm.migrateSectors(newLocations, false, log.Named("migrateSectors"))
		migrated += n
		// update the alert
		a.Data["migratedSectors"] = migrated
		vm.a.Register(a)
		return err
	})
	log.Info("migrated sectors", zap.Int("count", migrated))
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
		// update the alert
		a.Data["currentSectors"] = target
		vm.a.Register(a)
		// sleep to allow other operations to run
		time.Sleep(time.Millisecond)
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

func (vm *VolumeManager) doResize(volumeID int, current, target uint64) error {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	defer cancel()

	switch {
	case current > target:
		// volume is shrinking
		return vm.shrinkVolume(ctx, volumeID, current, target)
	case current < target:
		// volume is growing
		return vm.growVolume(ctx, volumeID, current, target)
	}
	return nil
}

func (vm *VolumeManager) migrateForRemoval(id int, localPath string, force bool, log *zap.Logger) (int, error) {
	ctx, cancel, err := vm.tg.AddContext(context.Background())
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
	var migrated int
	err = vm.vs.MigrateSectors(id, 0, func(locations []SectorLocation) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := vm.migrateSectors(locations, force, log.Named("migrateSectors"))
		migrated += n
		// update the alert
		a.Data["migrated"] = migrated
		vm.a.Register(a)
		return err
	})
	if err != nil && !force {
		return migrated, fmt.Errorf("failed to migrate sector data: %w", err)
	} else if err := vm.vs.RemoveVolume(id, force); err != nil {
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
func (vm *VolumeManager) AddVolume(localPath string, maxSectors uint64, result chan<- error) (Volume, error) {
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
	vm.volumes[volumeID] = &volume{
		data: f,
		stats: VolumeStats{
			Status: VolumeStatusCreating,
		},
	}
	vm.mu.Unlock()

	// lock the volume during grow operation
	release, err := vm.lockVolume(volumeID)
	if err != nil {
		return Volume{}, fmt.Errorf("failed to lock volume: %w", err)
	}

	go func() {
		log := vm.log.Named("initialize").With(zap.Int("volumeID", volumeID), zap.Uint64("maxSectors", maxSectors))
		start := time.Now()
		err := func() error {
			defer vm.vs.SetAvailable(volumeID, true)
			defer vm.setVolumeStatus(volumeID, VolumeStatusReady)
			defer release()
			return vm.doResize(volumeID, 0, maxSectors)
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
func (vm *VolumeManager) RemoveVolume(id int, force bool, result chan<- error) error {
	log := vm.log.Named("remove").With(zap.Int("volumeID", id))
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
			return vm.migrateForRemoval(id, vol.LocalPath, force, log)
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
func (vm *VolumeManager) ResizeVolume(id int, maxSectors uint64, result chan<- error) error {
	done, err := vm.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	release, err := vm.lockVolume(id)
	if err != nil {
		return fmt.Errorf("failed to lock volume: %w", err)
	}
	vm.setVolumeStatus(id, VolumeStatusResizing)

	vol, err := vm.vs.Volume(id)
	if err != nil {
		release()
		return fmt.Errorf("failed to get volume: %w", err)
	}

	oldReadonly := vol.ReadOnly
	// set the volume to read-only to prevent new sectors from being added
	if err := vm.vs.SetReadOnly(id, true); err != nil {
		release()
		return fmt.Errorf("failed to set volume %v to read-only: %w", id, err)
	}

	go func() {
		log := vm.log.Named("resize").With(zap.Int("volumeID", id))
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
			return vm.doResize(id, vol.TotalSectors, maxSectors)
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
func (vm *VolumeManager) PruneSectors() (int, error) {
	done, err := vm.tg.Add()
	if err != nil {
		return 0, err
	}
	defer done()
	// expire temp sectors
	currentHeight := vm.cm.TipState().Index.Height
	if err := vm.vs.ExpireTempSectors(currentHeight); err != nil {
		return 0, fmt.Errorf("failed to expire temp sectors: %w", err)
	}
	return vm.vs.PruneSectors()
}

// NewVolumeManager creates a new VolumeManager.
func NewVolumeManager(vs VolumeStore, a Alerts, cm ChainManager, log *zap.Logger) (*VolumeManager, error) {
	vm := &VolumeManager{
		vs:  vs,
		a:   a,
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
	go vm.cleanup()
	return vm, nil
}
