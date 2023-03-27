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

func (c rpcCost) ToUsage() contracts.Usage {
	return contracts.Usage{
		RPCRevenue:       c.Base,
		StorageRevenue:   c.Storage,
		EgressRevenue:    c.Egress,
		IngressRevenue:   c.Ingress,
		RiskedCollateral: c.Collateral,
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

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// validateContractFormation verifies that the new contract is valid given the
// host's settings.
func validateContractFormation(fc types.FileContract, hostKey, renterKey types.UnlockKey, currentHeight uint64, settings rhpv2.HostSettings) (types.Currency, error) {
	switch {
	case fc.Filesize != 0:
		return types.ZeroCurrency, errors.New("initial filesize should be 0")
	case fc.RevisionNumber != 0:
		return types.ZeroCurrency, errors.New("initial revision number should be 0")
	case fc.FileMerkleRoot != types.Hash256{}:
		return types.ZeroCurrency, errors.New("initial Merkle root should be empty")
	case fc.WindowStart < currentHeight+settings.WindowSize:
		return types.ZeroCurrency, errors.New("contract ends too soon to safely submit the contract transaction")
	case fc.WindowStart > currentHeight+settings.MaxDuration:
		return types.ZeroCurrency, errors.New("contract duration is too long")
	case fc.WindowEnd < fc.WindowStart+settings.WindowSize:
		return types.ZeroCurrency, errors.New("proof window is too small")
	case len(fc.ValidProofOutputs) != 2:
		return types.ZeroCurrency, errors.New("wrong number of valid proof outputs")
	case len(fc.MissedProofOutputs) != 3:
		return types.ZeroCurrency, errors.New("wrong number of missed proof outputs")
	case fc.ValidHostOutput().Address != settings.Address:
		return types.ZeroCurrency, errors.New("wrong address for host valid output")
	case fc.MissedHostOutput().Address != settings.Address:
		return types.ZeroCurrency, errors.New("wrong address for host missed output")
	case fc.MissedProofOutputs[2].Address != types.VoidAddress:
		return types.ZeroCurrency, errors.New("wrong address for void output")
	case fc.MissedProofOutputs[2].Value != types.ZeroCurrency:
		return types.ZeroCurrency, errors.New("void output should have value 0")
	case fc.ValidHostPayout().Cmp(settings.ContractPrice) < 0:
		return types.ZeroCurrency, errors.New("host valid payout is too small")
	case !fc.ValidHostPayout().Equals(fc.MissedHostPayout()):
		return types.ZeroCurrency, errors.New("host valid and missed outputs must be equal")
	case fc.ValidHostPayout().Cmp(settings.MaxCollateral) > 0:
		return types.ZeroCurrency, errors.New("excessive initial collateral")
	case fc.UnlockHash != types.Hash256(contractUnlockConditions(hostKey, renterKey).UnlockHash()):
		return types.ZeroCurrency, errors.New("incorrect unlock hash")
	}
	return fc.ValidHostPayout().Sub(settings.ContractPrice), nil
}

// validateContractRenewal verifies that the renewed contract is valid given the
// old contract. A renewal is valid if the contract fields match and the
// revision number is 0.
func validateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.UnlockKey, baseHostRevenue, baseRiskedCollateral types.Currency, currentHeight uint64, settings rhpv2.HostSettings) (storageRevenue, riskedCollateral, lockedCollateral types.Currency, err error) {
	switch {
	case renewal.RevisionNumber != 0:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("revision number must be zero")
	case renewal.Filesize != existing.Filesize:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("filesize must not change")
	case renewal.FileMerkleRoot != existing.FileMerkleRoot:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("file Merkle root must not change")
	case renewal.WindowEnd < existing.WindowEnd:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("renewal window must not end before current window")
	case renewal.WindowStart < currentHeight+settings.WindowSize:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("contract ends too soon to safely submit the contract transaction")
	case renewal.WindowStart > currentHeight+settings.MaxDuration:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("contract duration is too long")
	case renewal.WindowEnd < renewal.WindowStart+settings.WindowSize:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("proof window is too small")
	case len(renewal.ValidProofOutputs) != 2:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong number of valid proof outputs")
	case len(renewal.MissedProofOutputs) != 3:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong number of missed proof outputs")
	case renewal.ValidProofOutputs[1].Address != settings.Address:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for valid host output")
	case renewal.MissedProofOutputs[1].Address != settings.Address:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for missed host output")
	case renewal.MissedProofOutputs[2].Address != types.VoidAddress:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for void output")
	}

	expectedBurn := baseHostRevenue.Add(baseRiskedCollateral)
	hostBurn, underflow := renewal.ValidHostPayout().SubWithUnderflow(renewal.MissedHostPayout())
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("host valid payout must be greater than host missed payout")
	} else if hostBurn.Cmp(expectedBurn) > 0 {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("excessive host burn: expected at most %d got %d", baseRiskedCollateral, riskedCollateral)
	} else if !renewal.MissedProofOutputs[2].Value.Equals(hostBurn) {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("risked collateral must be sent to void output")
	}

	// calculate the host's risked collateral as the difference between the burn
	// and base revenue
	riskedCollateral, underflow = hostBurn.SubWithUnderflow(baseHostRevenue)
	if underflow {
		riskedCollateral = types.ZeroCurrency
	}

	// calculate the locked collateral as the difference between the valid host
	// payout and the base revenue
	lockedCollateral, underflow = renewal.ValidHostPayout().SubWithUnderflow(baseHostRevenue)
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("valid host output must be more than base storage cost")
	} else if lockedCollateral.Cmp(settings.MaxCollateral) > 0 {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("collateral exceeds maximum: expected at most %d got %d", settings.MaxCollateral, lockedCollateral)
	}

	return baseHostRevenue, riskedCollateral, lockedCollateral, nil
}
