package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/jape"
)

type (
	volumeJobs struct {
		volumes VolumeManager

		mu   sync.Mutex // protects jobs
		jobs map[int]context.CancelFunc
	}
)

func (vj *volumeJobs) AddVolume(path string, maxSectors uint64) (storage.Volume, error) {
	ctx, cancel := context.WithCancel(context.Background())
	complete := make(chan error, 1)
	volume, err := vj.volumes.AddVolume(ctx, path, maxSectors, complete)
	if err != nil {
		cancel()
		return storage.Volume{}, err
	}

	vj.mu.Lock()
	defer vj.mu.Unlock()
	vj.jobs[volume.ID] = cancel

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case <-complete:
		}

		vj.mu.Lock()
		defer vj.mu.Unlock()
		delete(vj.jobs, volume.ID)
	}()
	return volume, nil
}

func (vj *volumeJobs) RemoveVolume(id int, force bool) error {
	vj.mu.Lock()
	defer vj.mu.Unlock()
	if _, exists := vj.jobs[id]; exists {
		return errors.New("volume is busy")
	}

	ctx, cancel := context.WithCancel(context.Background())
	complete := make(chan error, 1)
	err := vj.volumes.RemoveVolume(ctx, id, force, complete)
	if err != nil {
		cancel()
		return err
	}

	vj.jobs[id] = cancel
	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case <-complete:
		}

		vj.mu.Lock()
		defer vj.mu.Unlock()
		delete(vj.jobs, id)
	}()
	return nil
}

func (vj *volumeJobs) ResizeVolume(id int, newSize uint64) error {
	vj.mu.Lock()
	defer vj.mu.Unlock()
	if _, exists := vj.jobs[id]; exists {
		return errors.New("volume is busy")
	}

	ctx, cancel := context.WithCancel(context.Background())
	complete := make(chan error, 1)
	err := vj.volumes.ResizeVolume(ctx, id, newSize, complete)
	if err != nil {
		cancel()
		return err
	}

	vj.jobs[id] = cancel
	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
		case <-complete:
		}

		vj.mu.Lock()
		defer vj.mu.Unlock()
		delete(vj.jobs, id)
	}()
	return nil
}

func (vj *volumeJobs) Cancel(id int) error {
	vj.mu.Lock()
	defer vj.mu.Unlock()
	cancel, exists := vj.jobs[id]
	if !exists {
		return fmt.Errorf("no job for volume %d", id)
	}
	cancel()
	delete(vj.jobs, id)
	return nil
}

func (a *api) handleGETVolumes(c jape.Context) {
	volumes, err := a.volumes.Volumes()
	if !a.checkServerError(c, "failed to get volumes", err) {
		return
	}
	var jsonVolumes []VolumeMeta
	for _, volume := range volumes {
		jsonVolumes = append(jsonVolumes, toJSONVolume(volume))
	}
	c.Encode(jsonVolumes)
}

func (a *api) handlePOSTVolume(c jape.Context) {
	var req AddVolumeRequest
	if err := c.Decode(&req); err != nil {
		return
	} else if len(req.LocalPath) == 0 {
		c.Error(errors.New("local path is required"), http.StatusBadRequest)
		return
	} else if req.MaxSectors == 0 {
		c.Error(errors.New("max sectors is required"), http.StatusBadRequest)
		return
	}
	volume, err := a.volumeJobs.AddVolume(req.LocalPath, req.MaxSectors)
	if !a.checkServerError(c, "failed to add volume", err) {
		return
	}
	c.Encode(volume)
}

func (a *api) handleDeleteVolume(c jape.Context) {
	var id int
	var force bool
	if err := c.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	} else if err := c.DecodeForm("force", &force); err != nil {
		return
	}
	err := a.volumeJobs.RemoveVolume(id, force)
	a.checkServerError(c, "failed to remove volume", err)
}

func (a *api) handlePUTVolumeResize(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req ResizeVolumeRequest
	if err := c.Decode(&req); err != nil {
		return
	}

	err := a.volumeJobs.ResizeVolume(id, req.MaxSectors)
	a.checkServerError(c, "failed to resize volume", err)
}

func (a *api) handleDELETEVolumeCancelOp(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	err := a.volumeJobs.Cancel(id)
	a.checkServerError(c, "failed to cancel operation", err)
}
