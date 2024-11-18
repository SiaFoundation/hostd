package rhp

import (
	"errors"
	"fmt"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

// hashFinalRevision returns the hash of the final revision during contract renewal
func hashFinalRevision(clearing types.FileContractRevision, renewal types.FileContract) types.Hash256 {
	h := types.NewHasher()
	renewal.EncodeTo(h.E)
	clearing.EncodeTo(h.E)
	return h.Sum()
}

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// validateContractRenewal verifies that the renewed contract is valid given the
// old contract. A renewal is valid if the contract fields match and the
// revision number is 0.
func validateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.UnlockKey, walletAddress types.Address, baseStorageRevenue, baseRiskedCollateral types.Currency, pt rhp3.HostPriceTable) (riskedCollateral, lockedCollateral types.Currency, err error) {
	switch {
	case renewal.RevisionNumber != 0:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("revision number must be zero")
	case renewal.Filesize != existing.Filesize:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("filesize must not change")
	case renewal.FileMerkleRoot != existing.FileMerkleRoot:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("file Merkle root must not change")
	case renewal.WindowEnd < existing.WindowEnd:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("renewal window must not end before current window")
	case renewal.WindowStart < pt.HostBlockHeight+pt.WindowSize:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("contract ends too soon to safely submit the contract transaction")
	case renewal.WindowStart > pt.HostBlockHeight+pt.MaxDuration:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("contract duration is too long")
	case renewal.WindowEnd < renewal.WindowStart+pt.WindowSize:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("proof window is too small")
	case len(renewal.ValidProofOutputs) != 2:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong number of valid proof outputs")
	case len(renewal.MissedProofOutputs) != 3:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong number of missed proof outputs")
	case renewal.ValidHostOutput().Address != walletAddress:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for valid host output")
	case renewal.MissedHostOutput().Address != walletAddress:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for missed host output")
	case renewal.MissedProofOutputs[2].Address != types.VoidAddress:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("wrong address for void output")
	case renewal.UnlockHash != contractUnlockConditions(hostKey, renterKey).UnlockHash():
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("incorrect unlock hash")
	}

	expectedBurn := baseStorageRevenue.Add(baseRiskedCollateral)
	hostBurn, underflow := renewal.ValidHostPayout().SubWithUnderflow(renewal.MissedHostPayout())
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("host valid payout must be greater than host missed payout")
	} else if hostBurn.Cmp(expectedBurn) > 0 {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("excessive host burn: expected at most %d got %d", expectedBurn, hostBurn)
	} else if !renewal.MissedProofOutputs[2].Value.Equals(hostBurn) {
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("risked collateral must be sent to void output")
	}

	// calculate the host's risked collateral as the difference between the burn
	// and base revenue
	riskedCollateral, underflow = hostBurn.SubWithUnderflow(baseStorageRevenue)
	if underflow {
		riskedCollateral = types.ZeroCurrency
	}

	// calculate the locked collateral as the difference between the valid host
	// payout and the base revenue
	minValidPayout := pt.ContractPrice.Add(baseStorageRevenue)
	lockedCollateral, underflow = renewal.ValidHostPayout().SubWithUnderflow(minValidPayout)
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("insufficient host valid payout: expected at least %d got %d", minValidPayout, renewal.ValidHostPayout())
	} else if lockedCollateral.Cmp(pt.MaxCollateral) > 0 {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("collateral exceeds maximum: expected at most %d got %d", pt.MaxCollateral, lockedCollateral)
	}
	// validate that the host's missed payout returns at least the locked
	// collateral and contract price
	minMissedPayout := pt.ContractPrice.Add(lockedCollateral).Sub(riskedCollateral)
	if renewal.MissedHostPayout().Cmp(minMissedPayout) < 0 {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("insufficient host missed payout: expected at least %d got %d", minMissedPayout, renewal.MissedHostPayout())
	}
	return riskedCollateral, lockedCollateral, nil
}
