package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

type (
	jsonHostSettings struct {
		BlockHeight      uint64                       `json:"blockheight"`
		SecretKey        crypto.SecretKey             `json:"secretkey"`
		InternalSettings modules.HostInternalSettings `json:"settings"`
	}

	// A Store is used to migrate a siad host to hostd's database format.
	Store interface {
		// Sectors
		HasSector(root types.Hash256) (bool, error)

		// Config
		UpdateSettings(settings settings.Settings) error

		// Migration
		SiadMigrateHostKey(pk types.PrivateKey) error
		SiadMigrateContract(revision contracts.SignedRevision, negotiationHeight uint64, formationSet []types.Transaction, sectors []types.Hash256) error
		SiadMigrateNextVolumeIndex(volumeID int64) (volumeIndex int64, err error)
		SiadMigrateStoredSector(volumeID, volumeIndex int64, sectorID types.Hash256) (err error)
		SiadMigrateVolume(localPath string, maxSectors uint64) (volumeID int64, err error)
		SiadUpdateMetrics() error
	}
)

func readHostConfig(ctx context.Context, dir string) (jsonHostSettings, error) {
	f, err := os.Open(filepath.Join(dir, "host", "host.json"))
	if err != nil {
		return jsonHostSettings{}, fmt.Errorf("failed to open host.json: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	// decode the header, but ignore it
	var header string
	if err := dec.Decode(&header); err != nil {
		return jsonHostSettings{}, fmt.Errorf("failed to decode header: %w", err)
	} else if err := dec.Decode(&header); err != nil {
		return jsonHostSettings{}, fmt.Errorf("failed to decode header: %w", err)
	} else if err := dec.Decode(&header); err != nil {
		return jsonHostSettings{}, fmt.Errorf("failed to decode header: %w", err)
	}

	var settings jsonHostSettings
	if err := dec.Decode(&settings); err != nil {
		return jsonHostSettings{}, fmt.Errorf("failed to decode settings: %w", err)
	}
	return settings, nil
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// Siad migrates a siad host to hostd.
func Siad(ctx context.Context, db Store, dir string, destructive bool, log *zap.Logger) error {
	oldConfig, err := readHostConfig(ctx, dir)
	if err != nil {
		return fmt.Errorf("failed to read host config: %w", err)
	}

	// migrate host key
	privateKey := types.PrivateKey(oldConfig.SecretKey[:])
	if err := db.SiadMigrateHostKey(privateKey); err != nil {
		return fmt.Errorf("failed to set host key: %w", err)
	}

	log.Info("Migrated host key", zap.Stringer("key", privateKey.PublicKey()))

	newSettings := settings.DefaultSettings
	newSettings.AcceptingContracts = oldConfig.InternalSettings.AcceptingContracts
	newSettings.NetAddress = string(oldConfig.InternalSettings.NetAddress)
	// pricing
	convertToCore(&oldConfig.InternalSettings.MinContractPrice, &newSettings.ContractPrice)
	convertToCore(&oldConfig.InternalSettings.MinBaseRPCPrice, &newSettings.BaseRPCPrice)
	convertToCore(&oldConfig.InternalSettings.MinSectorAccessPrice, &newSettings.SectorAccessPrice)
	convertToCore(&oldConfig.InternalSettings.MinStoragePrice, &newSettings.StoragePrice)
	convertToCore(&oldConfig.InternalSettings.MinUploadBandwidthPrice, &newSettings.IngressPrice)
	convertToCore(&oldConfig.InternalSettings.MinDownloadBandwidthPrice, &newSettings.EgressPrice)
	newSettings.CollateralMultiplier = 2
	// limits
	convertToCore(&oldConfig.InternalSettings.MaxCollateral, &newSettings.MaxCollateral)
	newSettings.MaxContractDuration = uint64(oldConfig.InternalSettings.MaxDuration)

	// migrate settings
	if err := db.UpdateSettings(newSettings); err != nil {
		return fmt.Errorf("failed to set settings: %w", err)
	}

	log.Info("Migrated settings")

	// migrate contracts
	success, skipped, failures, err := migrateContracts(ctx, db, dir, oldConfig.BlockHeight, log.Named("contracts"))
	if err != nil {
		return fmt.Errorf("failed to migrate contracts: %w", err)
	}
	log.Info("Migrated contracts", zap.Int("failed", failures), zap.Int("successful", success), zap.Int("skipped", skipped))

	// migrate storage folders
	if err := migrateStorageFolders(ctx, db, dir, destructive, log.Named("storage")); err != nil {
		return fmt.Errorf("failed to migrate storage folders: %w", err)
	}
	log.Info("Migrated storage folders")

	if err := db.SiadUpdateMetrics(); err != nil {
		return fmt.Errorf("failed to update metrics: %w", err)
	}

	log.Info("Migration complete")
	return nil
}
