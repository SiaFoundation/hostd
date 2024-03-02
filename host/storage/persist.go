package storage

import (
	"context"
	"errors"

	"go.sia.tech/core/types"
)

type (
	// MigrateFunc is a callback function that is called for each sector that
	// needs to be migrated If the function returns an error, the sector should
	// be skipped and migration should continue.
	MigrateFunc func(location SectorLocation) error

	// A VolumeStore stores and retrieves information about storage volumes.
	VolumeStore interface {
		// StorageUsage returns the number of used and total bytes in all volumes
		StorageUsage() (usedBytes, totalBytes uint64, _ error)
		// Volumes returns a list of all volumes in the volume store.
		Volumes() ([]Volume, error)
		// Volume returns a volume in the store by its id
		Volume(id int64) (Volume, error)
		// AddVolume initializes a new storage volume and adds it to the volume
		// store. GrowVolume must be called afterwards to initialize the volume
		// to its desired size.
		AddVolume(localPath string, readOnly bool) (int64, error)
		// RemoveVolume removes a storage volume from the volume store. If there
		// are used sectors in the volume, ErrVolumeNotEmpty is returned. If
		// force is true, the volume is removed even if it is not empty.
		RemoveVolume(volumeID int64, force bool) error
		// GrowVolume grows a storage volume's metadata to maxSectors. If the
		// number of sectors in the volume is already greater than maxSectors,
		// nil is returned.
		GrowVolume(volumeID int64, maxSectors uint64) error
		// ShrinkVolume shrinks a storage volume's metadata to maxSectors. If
		// there are used sectors in the shrink range, an error is returned.
		ShrinkVolume(volumeID int64, maxSectors uint64) error

		// SetReadOnly sets the read-only flag on a volume.
		SetReadOnly(volumeID int64, readOnly bool) error
		// SetAvailable sets the available flag on a volume.
		SetAvailable(volumeID int64, available bool) error

		// MigrateSectors returns a new location for each occupied sector of a
		// volume starting at min. The sector data should be copied to the new
		// location and synced to disk during migrateFn. If migrateFn returns an
		// error, migration will continue, but that sector is not migrated.
		MigrateSectors(ctx context.Context, volumeID int64, min uint64, migrateFn MigrateFunc) (migrated, failed int, err error)
		// StoreSector calls fn with an empty location in a writable volume. If
		// the sector root already exists, fn is called with the existing
		// location and exists is true. Unless exists is true, The sector must
		// be written to disk within fn. If fn returns an error, the metadata is
		// rolled back. If no space is available, ErrNotEnoughStorage is
		// returned. The location is locked until release is called.
		//
		// The sector should be referenced by either a contract or temp store
		// before release is called to prevent Prune() from removing it.
		StoreSector(root types.Hash256, fn func(loc SectorLocation, exists bool) error) (release func() error, err error)
		// RemoveSector removes the metadata of a sector and returns its
		// location in the volume.
		RemoveSector(root types.Hash256) error
		// SectorLocation returns the location of a sector or an error if the
		// sector is not found. The location is locked until release is
		// called.
		SectorLocation(root types.Hash256) (loc SectorLocation, release func() error, err error)
		// AddTemporarySectors adds a list of sectors to the temporary store.
		// The sectors are not referenced by a contract and will be removed
		// at the expiration height.
		AddTemporarySectors(sectors []TempSector) error
		// ExpireTempSectors removes all temporary sectors that expired before
		// the given height.
		ExpireTempSectors(height uint64) error
		// IncrementSectorStats increments sector stats
		IncrementSectorStats(reads, writes, cacheHit, cacheMiss uint64) error
		// SectorReferences returns the references to a sector
		SectorReferences(types.Hash256) (SectorReference, error)
	}
)

var (
	// ErrMigrationFailed is returned when a volume fails to migrate all
	// of its sectors.
	ErrMigrationFailed = errors.New("migration failed")
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
