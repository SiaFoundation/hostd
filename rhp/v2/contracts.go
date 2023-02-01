package rhp

import (
	"errors"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

var (
	// ErrNoContractLocked is returned when a contract revision is attempted
	// without a contract being locked.
	ErrNoContractLocked = errors.New("no contract locked")
	// ErrContractRevisionLimit is returned when a contract revision would
	// exceed the maximum revision number.
	ErrContractRevisionLimit = errors.New("max revision number reached")
	// ErrContractProofWindowStarted is returned when a contract revision is
	// attempted after the proof window has started.
	ErrContractProofWindowStarted = errors.New("proof window has started")
	// ErrContractExpired is returned when a contract revision is attempted
	// after the contract has expired.
	ErrContractExpired = errors.New("contract has expired")

	// ErrInvalidSectorLength is returned when a sector is not the correct
	// length.
	ErrInvalidSectorLength = errors.New("length of sector data must be exactly 4MiB")

	// ErrTrimOutOfBounds is returned when a trim operation exceeds the total
	// number of sectors
	ErrTrimOutOfBounds = errors.New("trim size exceeds number of sectors")
	// ErrSwapOutOfBounds is returned when one of the swap indices exceeds the
	// total number of sectors
	ErrSwapOutOfBounds = errors.New("swap index is out of bounds")
	// ErrUpdateOutOfBounds is returned when the update index exceeds the total
	// number of sectors
	ErrUpdateOutOfBounds = errors.New("update index is out of bounds")
	// ErrOffsetOutOfBounds is returned when the offset exceeds and length
	// exceed the sector size.
	ErrOffsetOutOfBounds = errors.New("update section is out of bounds")
	// ErrUpdateProofSize is returned when a proof is requested for an update
	// operation that is not a multiple of 64 bytes.
	ErrUpdateProofSize = errors.New("update section is not a multiple of the segment size")
)

// validateWriteActions validates the actions received by the renter for the
// write RPC and estimates the cost of completing the actions.
func validateWriteActions(actions []rhpv2.RPCWriteAction, oldSectors uint64, proof bool, remainingDuration uint64, settings rhpv2.HostSettings) (cost, collateral types.Currency, _ error) {
	var uploadBytes uint64
	newSectors := oldSectors
	for _, action := range actions {
		switch action.Type {
		case rhpv2.RPCWriteActionAppend:
			if len(action.Data) != rhpv2.SectorSize {
				return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("invalid sector size: %v: %w", len(action.Data), ErrInvalidSectorLength)
			}
			newSectors++
			uploadBytes += rhpv2.SectorSize
		case rhpv2.RPCWriteActionTrim:
			if action.A > newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrTrimOutOfBounds
			}
			newSectors -= action.A
		case rhpv2.RPCWriteActionSwap:
			if action.A >= newSectors || action.B >= newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrSwapOutOfBounds
			}
		case rhpv2.RPCWriteActionUpdate:
			idx, offset := action.A, action.B
			if idx >= newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrUpdateOutOfBounds
			} else if offset+uint64(len(action.Data)) > rhpv2.SectorSize {
				return types.ZeroCurrency, types.ZeroCurrency, ErrOffsetOutOfBounds
			} else if proof && (offset%rhpv2.LeafSize != 0) || len(action.Data)%rhpv2.LeafSize != 0 {
				return types.ZeroCurrency, types.ZeroCurrency, ErrUpdateProofSize
			}
		default:
			return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("unknown write action type '%v'", action.Type)
		}
	}

	// calculate the cost of the write actions
	cost = settings.BaseRPCPrice.Add(settings.UploadBandwidthPrice.Mul64(uploadBytes)) // cost of uploading the new sectors
	if newSectors > oldSectors {
		additionalSectors := (newSectors - oldSectors)
		cost = cost.Add(settings.StoragePrice.Mul64(rhpv2.SectorSize * additionalSectors * remainingDuration)) // cost of storing the new sectors
		collateral = settings.Collateral.Mul64(rhpv2.SectorSize * additionalSectors)                           // collateral for the new sectors
	}

	if proof {
		// estimate cost of Merkle proof
		proofSize := 32 * (128 + len(actions))
		cost = cost.Add(settings.DownloadBandwidthPrice.Mul64(uint64(proofSize)))
	}
	return cost, collateral, nil
}
