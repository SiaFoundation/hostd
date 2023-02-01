package rhp

import (
	"errors"
	"fmt"

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

// validateRevision verifies that a new revision is valid given the current
// revision. Only the revision number and proof output values are allowed to
// change
func validateStdRevision(current, revision types.FileContractRevision) error {
	var oldPayout, validPayout, missedPayout types.Currency
	for _, o := range current.ValidProofOutputs {
		oldPayout = oldPayout.Add(o.Value)
	}
	for i := range revision.ValidProofOutputs {
		if revision.ValidProofOutputs[i].Address != current.ValidProofOutputs[i].Address {
			return fmt.Errorf("valid proof output %v unlockhash should not change", i)
		}
		validPayout = validPayout.Add(revision.ValidProofOutputs[i].Value)
	}
	for i := range revision.MissedProofOutputs {
		if revision.MissedProofOutputs[i].Address != current.MissedProofOutputs[i].Address {
			return fmt.Errorf("missed proof output %v unlockhash should not change", i)
		}
		missedPayout = missedPayout.Add(revision.MissedProofOutputs[i].Value)
	}

	switch {
	case validPayout.Cmp(oldPayout) != 0:
		return errors.New("valid proof output sum must not change")
	case missedPayout.Cmp(oldPayout) != 0:
		return errors.New("missed proof output sum must not change")
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
	if expectedBurn.Cmp(current.MissedProofOutputs[1].Value) > 0 {
		return errors.New("expected burn amount is greater than the missed host output value")
	}
	// validate that the burn amount was subtracted from the host's missed proof
	// output. There is no need to validate the burn output value since it is
	// already checked by validateStdRevision.
	missedHostValue := current.MissedProofOutputs[1].Value.Sub(expectedBurn)
	if revision.MissedProofOutputs[1].Value.Cmp(missedHostValue) < 0 {
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

	transfer := current.ValidProofOutputs[0].Value.Sub(revision.ValidProofOutputs[0].Value)

	// validate that all outputs are consistent with only transferring the
	// amount from the renter payouts to the host payouts.
	switch {
	case revision.ValidProofOutputs[0].Value.Cmp(current.ValidProofOutputs[0].Value.Sub(transfer)) != 0:
		return types.ZeroCurrency, errors.New("renter valid proof output is not reduced by the payment amount")
	case revision.MissedProofOutputs[0].Value.Cmp(current.MissedProofOutputs[0].Value.Sub(transfer)) != 0:
		return types.ZeroCurrency, errors.New("renter missed proof output is not reduced by the payment amount")
	case revision.ValidProofOutputs[1].Value.Cmp(current.ValidProofOutputs[1].Value.Add(transfer)) != 0:
		return types.ZeroCurrency, errors.New("host valid proof output is not increased by the payment amount")
	case revision.MissedProofOutputs[1].Value.Cmp(current.MissedProofOutputs[1].Value.Add(transfer)) != 0:
		return types.ZeroCurrency, errors.New("host missed proof output is not increased by the payment amount")
	}
	return transfer, nil
}
