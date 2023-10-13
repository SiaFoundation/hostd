package rhp

import (
	"errors"
	"fmt"

	rhp2 "go.sia.tech/core/rhp/v2"
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
)

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// validateContractFormation verifies that the new contract is valid given the
// host's settings.
func validateContractFormation(fc types.FileContract, hostKey, renterKey types.UnlockKey, currentHeight uint64, settings rhp2.HostSettings) (types.Currency, error) {
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
func validateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.UnlockKey, baseHostRevenue, baseRiskedCollateral types.Currency, currentHeight uint64, settings rhp2.HostSettings) (storageRevenue, riskedCollateral, lockedCollateral types.Currency, err error) {
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
	case renewal.ValidHostOutput().Address != settings.Address:
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for valid host output")
	case renewal.MissedHostOutput().Address != settings.Address:
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
