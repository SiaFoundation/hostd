package contracts

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/alerts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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
// result of every checked sector is sent on the returned channel. The channel is closed
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

	// register an alert to track progress
	alert := alerts.Alert{
		ID:       frand.Entropy256(),
		Severity: alerts.SeverityInfo,
		Message:  "Checking contract integrity",
		Data: map[string]any{
			"contractID": contractID,
			"checked":    0,
			"missing":    0,
			"corrupt":    0,
			"total":      len(roots),
		},
		Timestamp: time.Now(),
	}
	cm.alerts.Register(alert)

	results := make(chan IntegrityResult, 1)
	// start a goroutine to check each sector
	go func() {
		defer close(results)

		ctx, done, err := cm.tg.AddContext(ctx)
		if err != nil {
			return
		}
		defer done()

		var missing, corrupt int
		log := cm.log.Named("integrityCheck").With(zap.String("contractID", contractID.String()))
		for i, root := range roots {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// read each sector from disk and verify its Merkle root
			sector, err := cm.storage.Read(root)
			if err != nil { // sector read failed
				log.Error("missing sector", zap.String("root", root.String()), zap.Error(err))
				missing++
				results <- IntegrityResult{Root: root, Error: err}
			} else if calculated := rhpv2.SectorRoot(sector); root != calculated { // sector data corrupt
				log.Error("corrupt sector", zap.String("root", root.String()), zap.String("actual", calculated.String()))
				corrupt++
				results <- IntegrityResult{Root: root, ActualRoot: calculated, Error: errors.New("sector data corrupt")}
			} else { // sector is valid
				results <- IntegrityResult{Root: root, ActualRoot: calculated}
			}

			// update alert
			alert.Data["checked"] = i + 1
			alert.Data["missing"] = missing
			alert.Data["corrupt"] = corrupt
			cm.alerts.Register(alert)
		}

		log.Info("integrity check complete", zap.Int("missing", missing), zap.Int("corrupt", corrupt))
		// update the alert with the final results
		alert.Message = "Integrity check complete"
		if corrupt > 0 || missing > 0 {
			alert.Severity = alerts.SeverityError
		}
		cm.alerts.Register(alert)
	}()
	return results, uint64(len(roots)), nil
}
