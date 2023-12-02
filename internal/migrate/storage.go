package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type (
	storageFolder struct {
		Index uint16
		Path  string
		Usage []uint64
	}
	storageSettings struct {
		SectorSalt     types.Hash256
		StorageFolders []storageFolder
	}
)

// ErrMigrateSectorStored is return if a sector is already stored on the host
var ErrMigrateSectorStored = errors.New("sector already stored")

func readStorageSettings(ctx context.Context, dir string) (storageSettings, error) {
	f, err := os.Open(filepath.Join(dir, "host", "contractmanager", "contractmanager.json"))
	if err != nil {
		return storageSettings{}, fmt.Errorf("failed to open contractmanager.json: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	// decode the header, but ignore it
	var header string
	if err := dec.Decode(&header); err != nil {
		return storageSettings{}, fmt.Errorf("failed to decode header: %w", err)
	} else if err := dec.Decode(&header); err != nil {
		return storageSettings{}, fmt.Errorf("failed to decode header: %w", err)
	}

	var settings storageSettings
	if err := dec.Decode(&settings); err != nil {
		return storageSettings{}, fmt.Errorf("failed to decode settings: %w", err)
	}
	return settings, nil
}

func migrateFolderSectors(ctx context.Context, db Store, volumeID int64, maxSectors uint64, newPath, oldPath string, destructive bool, log *zap.Logger) error {
	log = log.With(zap.String("newVolume", newPath), zap.String("oldFolder", oldPath))

	// create the volume file and immediately close it to ensure it exists
	dataFile, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to create storage folder: %w", err)
	}
	defer dataFile.Close()

	oldDataFile, err := os.OpenFile(oldPath, os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open old storage folder: %w", err)
	}
	defer oldDataFile.Close()

	var sector [rhp2.SectorSize]byte
	// read each sector from the file
	for i := maxSectors; i > 0; i-- {
		select {
		case <-ctx.Done():
			return ctx.Err() // abort
		default:
		}

		startOffset := int64((i - 1) * rhp2.SectorSize)
		log.Debug("migrating sector", zap.Uint64("index", i), zap.Uint64("startOffset", uint64(startOffset)))
		if _, err := oldDataFile.ReadAt(sector[:], startOffset); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("failed to check sector: %w", err)
		}

		root := rhp2.SectorRoot(&sector)
		log.Debug("read sector", zap.Stringer("root", root))

		// check if the sector is stored on the host
		if ok, err := db.HasSector(root); err != nil {
			return fmt.Errorf("failed to check sector: %w", err)
		} else if !ok {
			// sector is not in use, skip it
			//
			// note: it would be better to check siad's usage array instead of
			// reading everything from disk, but some sectors on our test node
			// were marked as free when they were still being used, causing
			// contract failures.
			log.Debug("skipping unused sector", zap.Stringer("root", root))
			continue
		}

		log.Debug("sector is in use", zap.Stringer("root", root))

		// get the next available volume index
		volumeIndex, err := db.SiadMigrateNextVolumeIndex(volumeID)
		if err != nil {
			return fmt.Errorf("failed to get sector index: %w", err)
		}

		log.Debug("new sector index", zap.Int64("volumeIndex", volumeIndex))

		// write the sector to the volume file
		if _, err := dataFile.WriteAt(sector[:], volumeIndex*rhp2.SectorSize); err != nil {
			return fmt.Errorf("failed to write sector: %w", err)
		} else if err := dataFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync volume file: %w", err)
		}

		log.Debug("wrote sector to volume file", zap.Stringer("root", root))

		// update the sector index
		err = db.SiadMigrateStoredSector(volumeID, volumeIndex, root)
		if err != nil && !errors.Is(err, ErrMigrateSectorStored) {
			return fmt.Errorf("failed to update sector index: %w", err)
		}

		if destructive {
			// shrink the old storage folder
			if err := oldDataFile.Truncate(startOffset); err != nil {
				return fmt.Errorf("failed to truncate old storage folder to %d: %w", startOffset, err)
			}
		}
		log.Info("migrated sector", zap.Stringer("root", root))
	}
	return nil
}

func migrateStorageFolders(ctx context.Context, db Store, dir string, destructive bool, log *zap.Logger) error {
	settings, err := readStorageSettings(ctx, dir)
	if err != nil {
		return fmt.Errorf("failed to read storage settings: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// add all storage folders to the database
	for _, folder := range settings.StorageFolders {
		newPath := filepath.Join(folder.Path, "data.dat")

		maxSectors := uint64(len(folder.Usage)) * 64 // 1 sector per bit

		volumeID, err := db.SiadMigrateVolume(newPath, maxSectors)
		if err != nil {
			return fmt.Errorf("failed to add storage folder: %w", err)
		}
		log.Info("migrated storage folder", zap.String("path", newPath), zap.Uint64("maxSectors", maxSectors))

		wg.Add(1)
		// migrate folder data
		go func(ctx context.Context, volumeID int64, newPath, oldPath string) {
			defer wg.Done()

			err := migrateFolderSectors(ctx, db, volumeID, maxSectors, newPath, oldPath, destructive, log)
			if err != nil {
				errCh <- fmt.Errorf("failed to migrate storage data %q: %w", oldPath, err)
			}
		}(ctx, volumeID, newPath, filepath.Join(folder.Path, "siahostdata.dat"))
	}

	go func() {
		// wait for all migrations to complete, then close the error channel
		wg.Wait()
		close(errCh)
	}()

	// check for errors
	for err := range errCh {
		return err
	}

	if destructive {
		for _, folder := range settings.StorageFolders {
			oldDataPath := filepath.Join(folder.Path, "siahostdata.dat")
			oldMetaPath := filepath.Join(folder.Path, "siahostmetadata.dat")
			// delete the old storage folders
			if err := os.Remove(oldDataPath); err != nil {
				log.Warn("failed to remove old data file", zap.Error(err))
			} else if err := os.Remove(oldMetaPath); err != nil {
				log.Warn("failed to remove old metadata file", zap.Error(err))
			}
		}
	}
	return nil
}
