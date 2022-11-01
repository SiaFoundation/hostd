package rhp

import (
	"errors"
	"fmt"
	"math"

	rhpv2 "go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

const maxRevisionNumber = math.MaxUint64

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

func hashRevision(rev types.FileContractRevision) (hash [32]byte) {
	h, _ := blake2b.New256(nil)
	enc := newEncoder(h)
	(*objFileContractRevision)(&rev).encodeTo(enc)
	enc.Flush()
	h.Sum(hash[:0])
	return
}

// contractUnlockConditions returns the unlock conditions for a contract with
// the given host and renter public keys.
func contractUnlockConditions(hostKey, renterKey types.SiaPublicKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys: []types.SiaPublicKey{
			renterKey,
			hostKey,
		},
		SignaturesRequired: 2,
	}
}

// initialRevision returns the first revision of a file contract formation
// transaction.
func initialRevision(formationTxn *types.Transaction, hostPubKey, renterPubKey types.SiaPublicKey) types.FileContractRevision {
	fc := formationTxn.FileContracts[0]
	return types.FileContractRevision{
		ParentID:              formationTxn.FileContractID(0),
		NewRevisionNumber:     1,
		UnlockConditions:      contractUnlockConditions(hostPubKey, renterPubKey),
		NewFileSize:           fc.FileSize,
		NewFileMerkleRoot:     fc.FileMerkleRoot,
		NewWindowStart:        fc.WindowStart,
		NewWindowEnd:          fc.WindowEnd,
		NewValidProofOutputs:  fc.ValidProofOutputs,
		NewMissedProofOutputs: fc.MissedProofOutputs,
		NewUnlockHash:         fc.UnlockHash,
	}
}

// revise updates a contract revision with the new revision number and proof
// output values. The returned revision is a deep copy to prevent aliasing.
func revise(revision types.FileContractRevision, revisionNumber uint64, validOutputs, missedOutputs []types.Currency) (types.FileContractRevision, error) {
	if len(validOutputs) != len(revision.NewValidProofOutputs) || len(missedOutputs) != len(revision.NewMissedProofOutputs) {
		return types.FileContractRevision{}, errors.New("incorrect number of outputs")
	}
	revision.NewRevisionNumber = revisionNumber
	revision.NewValidProofOutputs = append([]types.SiacoinOutput(nil), revision.NewValidProofOutputs...)
	revision.NewMissedProofOutputs = append([]types.SiacoinOutput(nil), revision.NewMissedProofOutputs...)
	for i := range validOutputs {
		revision.NewValidProofOutputs[i].Value = validOutputs[i]
	}
	for i := range missedOutputs {
		revision.NewMissedProofOutputs[i].Value = missedOutputs[i]
	}
	return revision, nil
}

// validateContractRenewal verifies that the renewed contract is valid given the
// old contract. A renewal is valid if the contract fields match and the
// revision number is 0.
func validateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.SiaPublicKey, renterCost, hostBurn types.Currency, currentHeight uint64, settings rhpv2.HostSettings) error {
	switch {
	case renewal.RevisionNumber != 0:
		return errors.New("revision number must be zero")
	case renewal.FileSize != existing.NewFileSize:
		return errors.New("filesize must not change")
	case renewal.FileMerkleRoot != existing.NewFileMerkleRoot:
		return errors.New("file Merkle root must not change")
	case renewal.WindowEnd < existing.NewWindowEnd:
		return errors.New("renewal window must not end before current window")
	case renewal.WindowStart < types.BlockHeight(currentHeight+settings.WindowSize):
		return errors.New("contract ends too soon to safely submit the contract transaction")
	case renewal.WindowStart > types.BlockHeight(currentHeight+settings.MaxDuration):
		return errors.New("contract duration is too long")
	case renewal.WindowEnd < renewal.WindowStart+types.BlockHeight(settings.WindowSize):
		return errors.New("proof window is too small")
	case renewal.ValidProofOutputs[1].UnlockHash != settings.UnlockHash:
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
	case renewal.UnlockHash != contractUnlockConditions(hostKey, renterKey).UnlockHash():
		return errors.New("incorrect unlock hash")
	}
	return nil
}

// validateClearingRevision verifies that the revision locks the current
// contract by setting its revision number to the maximum value and the valid
// and missed proof outputs are the same.
func validateClearingRevision(current, final types.FileContractRevision) error {
	switch {
	case current.NewFileSize != final.NewFileSize:
		return errors.New("file size must not change")
	case current.NewFileMerkleRoot != final.NewFileMerkleRoot:
		return errors.New("file merkle root must not change")
	case current.NewWindowStart != final.NewWindowStart:
		return errors.New("window start must not change")
	case current.NewWindowEnd != final.NewWindowEnd:
		return errors.New("window end must not change")
	case len(final.NewValidProofOutputs) != len(final.NewMissedProofOutputs):
		return errors.New("wrong number of proof outputs")
	case final.NewRevisionNumber != maxRevisionNumber:
		return errors.New("revision number must be max value")
	}

	// validate both valid and missed outputs are equal to the current valid
	// proof outputs.
	for i := range final.NewValidProofOutputs {
		current, valid, missed := current.NewValidProofOutputs[i], final.NewValidProofOutputs[i], final.NewMissedProofOutputs[i]
		switch {
		case valid.Value.Cmp(current.Value) != 0:
			return fmt.Errorf("valid proof output value %v must not change", i)
		case valid.UnlockHash != current.UnlockHash:
			return fmt.Errorf("valid proof output address %v must not change", i)
		case valid.Value.Cmp(missed.Value) != 0:
			return fmt.Errorf("missed proof output %v must equal valid proof output", i)
		case valid.UnlockHash != missed.UnlockHash:
			return fmt.Errorf("missed proof output address %v must equal valid proof output", i)
		}
	}
	return nil
}

// validateRevision verifies that a new revision is valid given the current
// revision. Only the revision number and proof output values are allowed to
// change
func validateStdRevision(current, revision types.FileContractRevision) error {
	var oldPayout, validPayout, missedPayout types.Currency
	for _, o := range current.NewValidProofOutputs {
		oldPayout = oldPayout.Add(o.Value)
	}
	for i := range revision.NewValidProofOutputs {
		if revision.NewValidProofOutputs[i].UnlockHash != current.NewValidProofOutputs[i].UnlockHash {
			return fmt.Errorf("valid proof output %v unlockhash should not change", i)
		}
		validPayout = validPayout.Add(revision.NewValidProofOutputs[i].Value)
	}
	for i := range revision.NewMissedProofOutputs {
		if revision.NewMissedProofOutputs[i].UnlockHash != current.NewMissedProofOutputs[i].UnlockHash {
			return fmt.Errorf("missed proof output %v unlockhash should not change", i)
		}
		missedPayout = missedPayout.Add(revision.NewMissedProofOutputs[i].Value)
	}

	switch {
	case validPayout.Cmp(oldPayout) != 0:
		return errors.New("valid proof output sum must not change")
	case missedPayout.Cmp(oldPayout) != 0:
		return errors.New("missed proof output sum must not change")
	case revision.NewRevisionNumber <= current.NewRevisionNumber:
		return errors.New("revision number must increase")
	case revision.NewWindowStart != current.NewWindowStart:
		return errors.New("window start must not change")
	case revision.NewWindowEnd != current.NewWindowEnd:
		return errors.New("window end must not change")
	case len(revision.NewValidProofOutputs) != len(current.NewValidProofOutputs):
		return errors.New("valid proof outputs must not change")
	case len(revision.NewMissedProofOutputs) != len(current.NewMissedProofOutputs):
		return errors.New("missed proof outputs must not change")
	case revision.NewValidProofOutputs[0].Value.Cmp(current.NewValidProofOutputs[0].Value) > 0:
		return errors.New("renter valid proof output must not increase")
	case revision.NewMissedProofOutputs[0].Value.Cmp(current.NewMissedProofOutputs[0].Value) > 0:
		return errors.New("renter missed proof output must not increase")
	}
	return nil
}

// validateProgramRevision verifies that a contract program revision is valid
// and only the missed host output value is modified by the expected burn amount
// all other usage will have been paid for by the RPC budget. Signatures are not
// validated.
func validateProgramRevision(current, revision types.FileContractRevision, additionalStorage, additionalCollateral types.Currency) error {
	// verify the new revision is valid given the existing revision and the
	// public keys have not changed
	if err := validateStdRevision(current, revision); err != nil {
		return err
	}

	expectedBurn := additionalStorage.Add(additionalCollateral)
	if expectedBurn.Cmp(current.NewMissedProofOutputs[1].Value) > 0 {
		return errors.New("expected burn amount is greater than the missed host output value")
	}
	// validate that the burn amount was subtracted from the host's missed proof
	// output. There is no need to validate the burn output value since it is
	// already checked by validateStdRevision.
	missedHostValue := current.NewMissedProofOutputs[1].Value.Sub(expectedBurn)
	if revision.NewMissedProofOutputs[1].Value.Cmp(missedHostValue) < 0 {
		return errors.New("host burning more collateral than expected")
	}
	return nil
}

// validatePaymentRevision verifies that a payment revision is valid and the
// amount is properly deducted from both renter outputs and added to both host
// outputs. Signatures are not validated.
func validatePaymentRevision(current, revision types.FileContractRevision) (types.Currency, error) {
	// verify the new revision is valid given the existing revision and the
	// public keys have not changed.
	if err := validateStdRevision(current, revision); err != nil {
		return types.ZeroCurrency, err
	}

	transfer := current.NewValidProofOutputs[0].Value.Sub(revision.NewValidProofOutputs[0].Value)

	// validate that all outputs are consistent with only transferring the
	// amount from the renter payouts to the host payouts.
	switch {
	case revision.NewValidProofOutputs[0].Value.Cmp(current.NewValidProofOutputs[0].Value.Sub(transfer)) != 0:
		return types.ZeroCurrency, errors.New("renter valid proof output is not reduced by the payment amount")
	case revision.NewMissedProofOutputs[0].Value.Cmp(current.NewMissedProofOutputs[0].Value.Sub(transfer)) != 0:
		return types.ZeroCurrency, errors.New("renter missed proof output is not reduced by the payment amount")
	case revision.NewValidProofOutputs[1].Value.Cmp(current.NewValidProofOutputs[1].Value.Add(transfer)) != 0:
		return types.ZeroCurrency, errors.New("host valid proof output is not increased by the payment amount")
	case revision.NewMissedProofOutputs[1].Value.Cmp(current.NewMissedProofOutputs[1].Value.Add(transfer)) != 0:
		return types.ZeroCurrency, errors.New("host missed proof output is not increased by the payment amount")
	}
	return transfer, nil
}
