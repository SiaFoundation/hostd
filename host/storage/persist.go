package storage

import (
	"errors"
)

type (
	// A SectorUpdateTransaction atomically updates the sector store
	SectorUpdateTransaction interface {
		// AddSectorMetadata adds a sector's metadata to the sector store.
		AddSectorMetadata(root SectorRoot, location SectorLocation) error
	}

	// A SectorStore stores and retrieves sector metadata.
	SectorStore interface {
		// RemoveSector removes the metadata of a sector and returns its
		// location in the volume.
		RemoveSector(root SectorRoot) (loc SectorLocation, err error)
		// SectorLocation returns the location of a sector or an error if the
		// sector is not found. The location is locked until release is
		// called.
		SectorLocation(SectorRoot) (loc SectorLocation, release func() error, err error)
		// Prune removes the metadata of all sectors that are no longer
		// referenced by either a contract or temporary storage.
		Prune() error

		// Update atomically updates the sector store.
		Update(func(SectorUpdateTransaction) error) error
	}

	// A TempSectorUpdateTransaction atomically updates the temporary sector
	// store.
	TempSectorUpdateTransaction interface {
		SectorUpdateTransaction
		// AddTempSector adds a sector to the temporary sector store
		AddTempSector(root SectorRoot, expirationHeight uint64) error
	}

	// A TempSectorStore stores and retrieves sectors uploaded to temporary
	// storage.
	TempSectorStore interface {
		// Update atomically updates the temp sector store.
		Update(func(TempSectorUpdateTransaction) error) error
	}

	// A VolumeStore stores and retrieves information about storage volumes.
	VolumeStore interface {
		// Volumes returns a list of all volumes in the volume store.
		Volumes() ([]Volume, error)
		// AddVolume initializes a new storage volume and adds it to the volume
		// store. GrowVolume must be called afterwards to initialize the volume
		// to its desired size.
		AddVolume(localPath string, maxSectors uint64, readOnly bool) (Volume, error)
		// RemoveVolume removes a storage volume from the volume store. If there
		// are used sectors in the volume, ErrVolumeNotEmpty is returned. If
		// force is true, the volume is removed even if it is not empty.
		RemoveVolume(id VolumeID, force bool) error
		// GrowVolume grows a storage volume's metadata by n sectors.
		GrowVolume(id VolumeID, n uint64) error
		// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If
		// there are used sectors in the shrink range, an error is returned.
		ShrinkVolume(id VolumeID, maxSectors uint64) error

		// SetReadOnly sets the read-only flag on a volume.
		SetReadOnly(id VolumeID, readOnly bool) error
		// SetMaxSectors sets the maximum number of sectors in a volume,
		// returning the difference between the old and new values. It does not
		// change the actual size of the volume or its metadata. GrowVolume or
		// ShrinkVolume should be called after updating the maximum size.
		SetMaxSectors(id VolumeID, maxSectors uint64) (int64, error)

		// StoreSector returns an empty location in a writeable volume. The
		// returned location is locked until release is called. If no space is
		// available, ErrNotEnoughStorage is returned.
		//
		// If the sector already exists in the store, the existing location and
		// ErrSectorExists are returned. The location is still locked until
		// release is called.
		StoreSector(root SectorRoot) (loc SectorLocation, release func() error, err error)
		// MigrateSectors returns a new location for each occupied sector of a volume
		// starting at min. The sector data should be copied to the new volume and
		// synced to disk during migrateFn. Iteration is stopped if migrateFn returns an
		// error.
		MigrateSectors(id VolumeID, min uint64, migrateFn func(root SectorRoot, newLoc SectorLocation) error, commitFn func([]VolumeID) error) error
	}
)

var (
	ErrNotEnoughStorage = errors.New("not enough storage")
	ErrSectorExists     = errors.New("sector already exists")
	ErrSectorNotFound   = errors.New("sector not found")
	ErrVolumeNotEmpty   = errors.New("volume is not empty")
)
