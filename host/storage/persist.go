package storage

import (
	"errors"
)

type (
	// A TempSectorUpdateTransaction atomically updates the temporary sector
	// store.
	TempSectorUpdateTransaction interface {
		// AddTempSector adds a sector to the temporary sector store
		AddTempSector(root SectorRoot, expirationHeight uint64) error
	}

	// A VolumeStore stores and retrieves information about storage volumes.
	VolumeStore interface {
		// Volumes returns a list of all volumes in the volume store.
		Volumes() ([]Volume, error)
		// Volume returns a volume in the store by its id
		Volume(id int) (Volume, error)
		// AddVolume initializes a new storage volume and adds it to the volume
		// store. GrowVolume must be called afterwards to initialize the volume
		// to its desired size.
		AddVolume(localPath string, readOnly bool) (Volume, error)
		// RemoveVolume removes a storage volume from the volume store. If there
		// are used sectors in the volume, ErrVolumeNotEmpty is returned. If
		// force is true, the volume is removed even if it is not empty.
		RemoveVolume(volumeID int, force bool) error
		// GrowVolume grows a storage volume's metadata to maxSectors. If the
		// number of sectors in the volume is already greater than maxSectors,
		// nil is returned.
		GrowVolume(volumeID int, maxSectors uint64) error
		// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If
		// there are used sectors in the shrink range, an error is returned.
		ShrinkVolume(volumeID int, maxSectors uint64) error

		// SetReadOnly sets the read-only flag on a volume.
		SetReadOnly(volumeID int, readOnly bool) error

		// MigrateSectors returns a new location for each occupied sector of a volume
		// starting at min. The sector data should be copied to the new volume and
		// synced to disk during migrateFn. Iteration is stopped if migrateFn returns an
		// error.
		MigrateSectors(volumeID int, min uint64, migrateFn func(newLocations []SectorLocation) error) error
		// StoreSector calls fn with an empty location in a writeable volume. If
		// the sector root already exists, fn is called with the existing
		// location and exists is true. Unless exists is true, The sector must
		// be written to disk within fn. If fn returns an error, the metadata is
		// rolled back. If no space is available, ErrNotEnoughStorage is
		// returned. The location is locked until release is called.
		//
		// The sector should be referenced by either a contract or temp store
		// before release is called to prevent Prune() from removing it.
		StoreSector(root SectorRoot, fn func(loc SectorLocation, exists bool) error) (release func() error, err error)
		// RemoveSector removes the metadata of a sector and returns its
		// location in the volume.
		RemoveSector(root SectorRoot) error
		// SectorLocation returns the location of a sector or an error if the
		// sector is not found. The location is locked until release is
		// called.
		SectorLocation(root SectorRoot) (loc SectorLocation, release func() error, err error)
		// Prune removes the metadata of all sectors that are no longer
		// referenced by either a contract or temporary storage.
		Prune() error
	}
)

var (
	// ErrNotEnoughStorage is returned when there is not enough storage space to
	// store a sector.
	ErrNotEnoughStorage = errors.New("not enough storage")
	// ErrSectorNotFound is returned when a sector is not found.
	ErrSectorNotFound = errors.New("sector not found")
	// ErrVolumeNotEmpty is returned when trying to remove or shrink a volume
	// that has not been emptied.
	ErrVolumeNotEmpty = errors.New("volume is not empty")
	// ErrVolumeNotFound is returned when a volume is not found.
	ErrVolumeNotFound = errors.New("volume not found")
)
