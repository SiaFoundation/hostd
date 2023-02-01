package rhp

import (
	"errors"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

// HashRevision returns the hash of rev.
func HashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// InitialRevision returns the first revision of a file contract formation
// transaction.
func InitialRevision(formationTxn *types.Transaction, hostPubKey, renterPubKey types.UnlockKey) types.FileContractRevision {
	fc := formationTxn.FileContracts[0]
	return types.FileContractRevision{
		ParentID:         formationTxn.FileContractID(0),
		UnlockConditions: contractUnlockConditions(hostPubKey, renterPubKey),
		FileContract: types.FileContract{
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
			RevisionNumber:     1,
		},
	}
}

// Revise updates the contract revision with the provided values.
func Revise(revision types.FileContractRevision, revisionNumber uint64, validOutputs, missedOutputs []types.Currency) (types.FileContractRevision, error) {
	if len(validOutputs) != len(revision.ValidProofOutputs) || len(missedOutputs) != len(revision.MissedProofOutputs) {
		return types.FileContractRevision{}, errors.New("incorrect number of outputs")
	}
	revision.RevisionNumber = revisionNumber
	oldValid, oldMissed := revision.ValidProofOutputs, revision.MissedProofOutputs
	revision.ValidProofOutputs = make([]types.SiacoinOutput, len(validOutputs))
	revision.MissedProofOutputs = make([]types.SiacoinOutput, len(missedOutputs))
	for i := range validOutputs {
		revision.ValidProofOutputs[i].Address = oldValid[i].Address
		revision.ValidProofOutputs[i].Value = validOutputs[i]
	}
	for i := range missedOutputs {
		revision.MissedProofOutputs[i].Address = oldMissed[i].Address
		revision.MissedProofOutputs[i].Value = missedOutputs[i]
	}
	return revision, nil
}

// ValidateContractFormation verifies that the new contract is valid given the
// host's settings.
func ValidateContractFormation(fc types.FileContract, hostKey, renterKey types.UnlockKey, currentHeight uint64, settings rhpv2.HostSettings) error {
	switch {
	case fc.Filesize != 0:
		return errors.New("initial filesize should be 0")
	case fc.RevisionNumber != 0:
		return errors.New("initial revision number should be 0")
	case fc.FileMerkleRoot != types.Hash256{}:
		return errors.New("initial Merkle root should be empty")
	case fc.WindowStart < currentHeight+settings.WindowSize:
		return errors.New("contract ends too soon to safely submit the contract transaction")
	case fc.WindowStart > currentHeight+settings.MaxDuration:
		return errors.New("contract duration is too long")
	case fc.WindowEnd < fc.WindowStart+settings.WindowSize:
		return errors.New("proof window is too small")
	case len(fc.ValidProofOutputs) != 2:
		return errors.New("wrong number of valid proof outputs")
	case len(fc.MissedProofOutputs) != 3:
		return errors.New("wrong number of missed proof outputs")
	case fc.ValidProofOutputs[1].Address != settings.Address:
		return errors.New("wrong address for host valid output")
	case fc.MissedProofOutputs[1].Address != settings.Address:
		return errors.New("wrong address for host missed output")
	case fc.ValidProofOutputs[1].Value.Cmp(settings.ContractPrice) < 0:
		return errors.New("host valid payout is too small")
	case fc.ValidProofOutputs[1].Value.Cmp(fc.MissedProofOutputs[1].Value) != 0:
		return errors.New("host valid and missed outputs must be equal")
	case fc.ValidProofOutputs[1].Value.Cmp(settings.MaxCollateral) > 0:
		return errors.New("excessive initial collateral")
	case fc.UnlockHash != types.Hash256(contractUnlockConditions(hostKey, renterKey).UnlockHash()):
		return errors.New("incorrect unlock hash")
	}
	return nil
}

// ValidateContractRenewal verifies that the renewed contract is valid given the
// old contract. A renewal is valid if the contract fields match and the
// revision number is 0.
func ValidateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.UnlockKey, renterCost, hostBurn types.Currency, currentHeight uint64, settings rhpv2.HostSettings) error {
	switch {
	case renewal.RevisionNumber != 0:
		return errors.New("revision number must be zero")
	case renewal.Filesize != existing.Filesize:
		return errors.New("filesize must not change")
	case renewal.FileMerkleRoot != existing.FileMerkleRoot:
		return errors.New("file Merkle root must not change")
	case renewal.WindowEnd < existing.WindowEnd:
		return errors.New("renewal window must not end before current window")
	case renewal.WindowStart < currentHeight+settings.WindowSize:
		return errors.New("contract ends too soon to safely submit the contract transaction")
	case renewal.WindowStart > currentHeight+settings.MaxDuration:
		return errors.New("contract duration is too long")
	case renewal.WindowEnd < renewal.WindowStart+settings.WindowSize:
		return errors.New("proof window is too small")
	case renewal.ValidProofOutputs[1].Address != settings.Address:
		return errors.New("wrong address for host output")
	case renewal.ValidProofOutputs[1].Value.Cmp(renterCost) < 0:
		return errors.New("insufficient initial host valid payout")
	case renewal.MissedProofOutputs[1].Value.Cmp(renterCost) < 0:
		return errors.New("insufficient initial host missed payout")
	case renewal.ValidProofOutputs[1].Value.Sub(renterCost).Cmp(settings.MaxCollateral) > 0:
		return errors.New("excessive initial collateral")
	case renewal.MissedProofOutputs[1].Value.Cmp(renewal.ValidProofOutputs[1].Value) < 0:
		return errors.New("host valid payout must be greater than host missed payout")
	case renewal.MissedProofOutputs[2].Value.Cmp(hostBurn) < 0:
		return errors.New("excessive initial void output")
	case renewal.MissedProofOutputs[1].Value.Cmp(renewal.ValidProofOutputs[1].Value.Sub(hostBurn)) < 0:
		return errors.New("insufficient host missed payout")
	case renewal.UnlockHash != types.Hash256(contractUnlockConditions(hostKey, renterKey).UnlockHash()):
		return errors.New("incorrect unlock hash")
	}
	return nil
}

// ValidateClearingRevision verifies that the revision locks the current
// contract by setting its revision number to the maximum value and the valid
// and missed proof outputs are the same.
func ValidateClearingRevision(current, final types.FileContractRevision) error {
	switch {
	case current.Filesize != final.Filesize:
		return errors.New("file size must not change")
	case current.FileMerkleRoot != final.FileMerkleRoot:
		return errors.New("file merkle root must not change")
	case current.WindowStart != final.WindowStart:
		return errors.New("window start must not change")
	case current.WindowEnd != final.WindowEnd:
		return errors.New("window end must not change")
	case len(final.ValidProofOutputs) != len(final.MissedProofOutputs):
		return errors.New("wrong number of proof outputs")
	case final.RevisionNumber != types.MaxRevisionNumber:
		return errors.New("revision number must be max value")
	case final.UnlockHash != current.UnlockHash:
		return errors.New("unlock hash must not change")
	case final.UnlockConditions.UnlockHash() != current.UnlockConditions.UnlockHash():
		return errors.New("unlock conditions must not change")
	}

	// validate both valid and missed outputs are equal to the current valid
	// proof outputs.
	for i := range final.ValidProofOutputs {
		current, valid, missed := current.ValidProofOutputs[i], final.ValidProofOutputs[i], final.MissedProofOutputs[i]
		switch {
		case valid.Value.Cmp(current.Value) != 0:
			return fmt.Errorf("valid proof output value %v must not change", i)
		case valid.Address != current.Address:
			return fmt.Errorf("valid proof output address %v must not change", i)
		case valid.Value.Cmp(missed.Value) != 0:
			return fmt.Errorf("missed proof output %v must equal valid proof output", i)
		case valid.Address != missed.Address:
			return fmt.Errorf("missed proof output address %v must equal valid proof output", i)
		}
	}
	return nil
}

// ValidateRevision verifies that a new revision is valid given the current
// revision. Only the revision number and proof output values are allowed to
// change
func ValidateRevision(current, revision types.FileContractRevision, payment, collateral types.Currency) error {
	var oldPayout, validPayout, missedPayout types.Currency
	for _, o := range current.ValidProofOutputs {
		oldPayout = oldPayout.Add(o.Value)
	}
	for i := range revision.ValidProofOutputs {
		if revision.ValidProofOutputs[i].Address != current.ValidProofOutputs[i].Address {
			return fmt.Errorf("valid proof output %v address should not change", i)
		}
		validPayout = validPayout.Add(revision.ValidProofOutputs[i].Value)
	}
	for i := range revision.MissedProofOutputs {
		if revision.MissedProofOutputs[i].Address != current.MissedProofOutputs[i].Address {
			return fmt.Errorf("missed proof output %v address should not change", i)
		}
		missedPayout = missedPayout.Add(revision.MissedProofOutputs[i].Value)
	}

	minValid := current.ValidProofOutputs[1].Value.Add(payment)
	maxMissed := current.MissedProofOutputs[1].Value.Sub(collateral)

	switch {
	case validPayout.Cmp(oldPayout) != 0:
		return errors.New("valid proof output sum must not change")
	case missedPayout.Cmp(oldPayout) != 0:
		return errors.New("missed proof output sum must not change")
	case revision.UnlockHash != current.UnlockHash:
		return errors.New("unlock hash must not change")
	case revision.UnlockConditions.UnlockHash() != current.UnlockConditions.UnlockHash():
		return errors.New("unlock conditions must not change")
	case revision.RevisionNumber <= current.RevisionNumber:
		return errors.New("revision number must increase")
	case revision.WindowStart != current.WindowStart:
		return errors.New("window start must not change")
	case revision.WindowEnd != current.WindowEnd:
		return errors.New("window end must not change")
	case len(revision.ValidProofOutputs) != len(current.ValidProofOutputs):
		return errors.New("valid proof outputs must not change")
	case len(revision.MissedProofOutputs) != len(current.MissedProofOutputs):
		return errors.New("missed proof outputs must not change")
	case revision.ValidProofOutputs[0].Value.Cmp(current.ValidProofOutputs[0].Value) > 0:
		return errors.New("renter valid proof output must not increase")
	case revision.MissedProofOutputs[0].Value.Cmp(current.MissedProofOutputs[0].Value) > 0:
		return errors.New("renter missed proof output must not increase")
	case revision.ValidProofOutputs[1].Value.Cmp(minValid) < 0:
		return fmt.Errorf("insufficient host valid payment: expected value at least %v, got %v", minValid, revision.ValidProofOutputs[1].Value)
	case revision.MissedProofOutputs[1].Value.Cmp(maxMissed) > 0:
		return fmt.Errorf("too much collateral transfer: expected value at most %v, got %v", maxMissed, revision.MissedProofOutputs[1].Value)
	}
	return nil
}
