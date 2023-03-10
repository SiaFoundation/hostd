package rhp

import (
	"errors"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
)

type rpcCost struct {
	Base       types.Currency
	Storage    types.Currency
	Ingress    types.Currency
	Egress     types.Currency
	Collateral types.Currency
}

func (c rpcCost) Add(o rpcCost) rpcCost {
	return rpcCost{
		Base:       c.Base.Add(o.Base),
		Storage:    c.Storage.Add(o.Storage),
		Ingress:    c.Ingress.Add(o.Ingress),
		Egress:     c.Egress.Add(o.Egress),
		Collateral: c.Collateral.Add(o.Collateral),
	}
}

func (c rpcCost) Total() (cost, collateral types.Currency) {
	return c.Base.Add(c.Storage).Add(c.Ingress).Add(c.Egress), c.Collateral
}

func (c rpcCost) ToContractRevenue() contracts.Revenue {
	return contracts.Revenue{
		RPC:     c.Base,
		Storage: c.Storage,
		Egress:  c.Egress,
		Ingress: c.Ingress,
	}
}

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
func validateWriteActions(actions []rhpv2.RPCWriteAction, oldSectors uint64, proof bool, remainingDuration uint64, settings rhpv2.HostSettings) (rpcCost, error) {
	var uploadBytes uint64
	newSectors := oldSectors
	for _, action := range actions {
		switch action.Type {
		case rhpv2.RPCWriteActionAppend:
			if len(action.Data) != rhpv2.SectorSize {
				return rpcCost{}, fmt.Errorf("invalid sector size: %v: %w", len(action.Data), ErrInvalidSectorLength)
			}
			newSectors++
			uploadBytes += rhpv2.SectorSize
		case rhpv2.RPCWriteActionTrim:
			if action.A > newSectors {
				return rpcCost{}, ErrTrimOutOfBounds
			}
			newSectors -= action.A
		case rhpv2.RPCWriteActionSwap:
			if action.A >= newSectors || action.B >= newSectors {
				return rpcCost{}, ErrSwapOutOfBounds
			}
		case rhpv2.RPCWriteActionUpdate:
			idx, offset := action.A, action.B
			if idx >= newSectors {
				return rpcCost{}, ErrUpdateOutOfBounds
			} else if offset+uint64(len(action.Data)) > rhpv2.SectorSize {
				return rpcCost{}, ErrOffsetOutOfBounds
			} else if proof && (offset%rhpv2.LeafSize != 0) || len(action.Data)%rhpv2.LeafSize != 0 {
				return rpcCost{}, ErrUpdateProofSize
			}
		default:
			return rpcCost{}, fmt.Errorf("unknown write action type '%v'", action.Type)
		}
	}

	cost := rpcCost{
		Base:    settings.BaseRPCPrice,                            // base cost of the RPC
		Ingress: settings.UploadBandwidthPrice.Mul64(uploadBytes), // cost of uploading the new sectors
	}

	if newSectors > oldSectors {
		additionalSectors := (newSectors - oldSectors)
		cost.Storage = settings.StoragePrice.Mul64(rhpv2.SectorSize * additionalSectors * remainingDuration)  // cost of storing the new sectors
		cost.Collateral = settings.Collateral.Mul64(rhpv2.SectorSize * additionalSectors * remainingDuration) // collateral for the new sectors
	}

	if proof {
		// estimate cost of Merkle proof
		proofSize := 32 * (128 + len(actions))
		cost.Egress = settings.DownloadBandwidthPrice.Mul64(uint64(proofSize))
	}
	return cost, nil
}

func validateReadActions(sections []rhpv2.RPCReadRequestSection, proof bool, settings rhpv2.HostSettings) (rpcCost, error) {
	// validate the request sections and calculate the cost
	var bandwidth uint64
	for _, sec := range sections {
		switch {
		case uint64(sec.Offset)+uint64(sec.Length) > rhpv2.SectorSize:
			return rpcCost{}, ErrOffsetOutOfBounds
		case sec.Length == 0:
			return rpcCost{}, errors.New("length cannot be zero")
		case proof && (sec.Offset%rhpv2.LeafSize != 0 || sec.Length%rhpv2.LeafSize != 0):
			return rpcCost{}, errors.New("offset and length must be multiples of SegmentSize when requesting a Merkle proof")
		}

		bandwidth += uint64(sec.Length)
		if proof {
			start := sec.Offset / rhpv2.LeafSize
			end := (sec.Offset + sec.Length) / rhpv2.LeafSize
			proofSize := rhpv2.RangeProofSize(rhpv2.LeavesPerSector, start, end)
			bandwidth += proofSize * 32
		}
	}

	return rpcCost{
		Base:   settings.BaseRPCPrice.Add(settings.SectorAccessPrice.Mul64(uint64(len(sections)))),
		Egress: settings.DownloadBandwidthPrice.Mul64(bandwidth),
	}, nil
}

func rpcSectorRootsCost(count, offset uint64, settings rhpv2.HostSettings) rpcCost {
	proofSize := rhpv2.RangeProofSize(rhpv2.LeavesPerSector, offset, offset+count)
	return rpcCost{
		Base:   settings.BaseRPCPrice,
		Egress: settings.DownloadBandwidthPrice.Mul64((count + proofSize) * 32),
	}
}
