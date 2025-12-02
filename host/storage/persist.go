package storage

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/core/types"
)

type (
	// StoreFunc is called for every sector that needs written
	// to disk.
	StoreFunc func(loc SectorLocation) error
	// MigrateFunc is called for every sector that needs migration.
	// The sector should be migrated from 'from' to 'to' during
	// migrate func.
	MigrateFunc func(from, to SectorLocation) error

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

		// PruneSectors removes all sectors that have not been accessed since
		// lastAccess and are no longer referenced by a contract or temp storage.
		// If the context is canceled, pruning is stopped and the function returns
		// with the error.
		PruneSectors(ctx context.Context, lastAccess time.Time) error

		// MigrateSectors returns a new location for each occupied sector of a
		// volume starting at min. The sector data should be copied to the new
		// location and synced to disk during migrateFn. If migrateFn returns an
		// error, migration will continue, but that sector is not migrated.
		MigrateSectors(ctx context.Context, volumeID int64, min uint64, fn MigrateFunc) (migrated, failed int, err error)
		// StoreSector calls fn with an empty location in a writable volume. If
		// the sector root already exists, nil is returned. The sector should be
		// written to disk within fn. If fn returns an error, the metadata is
		// rolled back and the error is returned. If no space is available,
		// ErrNotEnoughStorage is returned.
		StoreSector(root types.Hash256, fn StoreFunc) error
		// RemoveSector removes the metadata of a sector and returns its
		// location in the volume.
		RemoveSector(root types.Hash256) error
		// HasSector returns true if the sector is stored by the host.
		HasSector(root types.Hash256) (bool, error)
		// SectorLocation returns the location of a sector or an error if the
		// sector is not found.
		SectorLocation(root types.Hash256) (loc SectorLocation, err error)

		// CacheSubtrees stores the cached subtree roots for a sector
		CacheSubtrees(root types.Hash256, subtrees []types.Hash256) error
		// SectorMetadata returns the metadata of a sector or an error if the
		// sector is not found.
		SectorMetadata(types.Hash256) (SectorMetadata, error)
		// AddTempSector adds a sector to temporary storage. The sectors will be deleted
		// after the expiration height
		AddTempSector(root types.Hash256, expiration uint64) error
		// AddTemporarySectors adds a list of sectors to the temporary store.
		// The sectors are not referenced by a contract and will be removed
		// at the expiration height.
		//
		// Deprecated: use AddTempSector
		AddTemporarySectors(sectors []TempSector) error
		// ExpireTempSectors removes all temporary sectors that expired before
		// the given height.
		ExpireTempSectors(height uint64) error
		// IncrementSectorMetrics increments sector metrics
		IncrementSectorMetrics(SectorMetrics) error
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
	// ErrSectorCorrupt is returned when a sector is found to be corrupt.
	ErrSectorCorrupt = errors.New("sector is corrupt")
	// ErrVolumeNotEmpty is returned when trying to remove or shrink a volume
	// that has not been emptied.
	ErrVolumeNotEmpty = errors.New("volume is not empty")
	// ErrVolumeNotFound is returned when a volume is not found.
	ErrVolumeNotFound = errors.New("volume not found")
)
