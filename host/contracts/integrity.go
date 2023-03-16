package contracts

import (
	"context"
	"errors"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type (
	// An IntegrityResult contains the result of an integrity check for a
	// contract sector.
	IntegrityResult struct {
		Root       types.Hash256 `json:"root"`
		ActualRoot types.Hash256 `json:"actualRoot"`
		Error      error         `json:"error"`
	}
)

// CheckIntegrity checks the integrity of a contract's sector roots on disk. The
// result of every check is sent on the returned channel. The channel is closed
// when all checks are complete.
func (cm *ContractManager) CheckIntegrity(ctx context.Context, contractID types.FileContractID) (<-chan IntegrityResult, uint64, error) {
	// lock the contract to ensure it doesn't get modified before the sector
	// roots are retrieved.
	contract, err := cm.Lock(ctx, contractID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to lock contract: %w", err)
	}
	defer cm.Unlock(contractID)

	expectedRoots := contract.Revision.Filesize / rhpv2.SectorSize

	roots, err := cm.store.SectorRoots(contractID, 0, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get sector roots: %w", err)
	} else if uint64(len(roots)) != expectedRoots {
		return nil, 0, fmt.Errorf("expected %v sector roots, got %v", expectedRoots, len(roots))
	} else if calculated := rhpv2.MetaRoot(roots); contract.Revision.FileMerkleRoot != calculated {
		return nil, 0, fmt.Errorf("expected Merkle root %v, got %v", contract.Revision.FileMerkleRoot, calculated)
	}

	results := make(chan IntegrityResult, 1)
	// start a goroutine to check each sector
	go func() {
		defer close(results)

		ctx, done, err := cm.tg.AddContext(ctx)
		if err != nil {
			return
		}
		defer done()

		log := cm.log.Named("integrityCheck").With(zap.String("contractID", contractID.String()))
		for _, root := range roots {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// read each sector from disk and verify its Merkle root
			sector, err := cm.storage.Read(root)
			if err != nil { // sector read failed
				log.Error("missing sector", zap.String("root", root.String()), zap.Error(err))
				results <- IntegrityResult{Root: root, Error: err}
			} else if calculated := rhpv2.SectorRoot(sector); root != calculated { // sector data corrupt
				log.Error("corrupt sector", zap.String("root", root.String()), zap.String("actual", calculated.String()))
				results <- IntegrityResult{Root: root, ActualRoot: calculated, Error: errors.New("sector data corrupt")}
			} else { // sector is valid
				results <- IntegrityResult{Root: root, ActualRoot: calculated}
			}
		}
	}()
	return results, uint64(len(roots)), nil
}
