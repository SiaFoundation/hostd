package rhp

import (
	"errors"
	"fmt"
	"math"

	"go.sia.tech/core/types"
)

func contractUnlockConditions(hostKey, renterKey types.UnlockKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys:         []types.UnlockKey{renterKey, hostKey},
		SignaturesRequired: 2,
	}
}

// validateStdRevision verifies that a new revision is valid given the current
// revision. Only the revision number and proof output values are allowed to
// change
func validateStdRevision(current, revision types.FileContractRevision) error {
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

	switch {
	case !validPayout.Equals(oldPayout):
		return errors.New("valid proof output sum must not change")
	case !missedPayout.Equals(oldPayout):
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
	case revision.ValidRenterPayout().Cmp(current.ValidRenterPayout()) > 0:
		return errors.New("renter valid proof output must not increase")
	case revision.MissedRenterPayout().Cmp(current.MissedRenterPayout()) > 0:
		return errors.New("renter missed proof output must not increase")
	case !revision.ValidRenterPayout().Equals(revision.MissedRenterPayout()):
		return errors.New("renter payouts must be equal")
	}
	return nil
}

// HashRevision returns the hash of rev.
func HashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

// InitialRevision returns the first revision of a file contract formation
// transaction.
func InitialRevision(formationTxn types.Transaction, hostPubKey, renterPubKey types.UnlockKey) types.FileContractRevision {
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
	switch {
	case revision.RevisionNumber == math.MaxUint64:
		return types.FileContractRevision{}, errors.New("contract is locked")
	case revisionNumber <= revision.RevisionNumber:
		return types.FileContractRevision{}, fmt.Errorf("revision number must be greater than %v", revision.RevisionNumber)
	case len(validOutputs) != len(revision.ValidProofOutputs):
		return types.FileContractRevision{}, errors.New("incorrect number of valid outputs")
	case len(missedOutputs) != len(revision.MissedProofOutputs):
		return types.FileContractRevision{}, errors.New("incorrect number of missed outputs")
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

// ClearingRevision returns a revision that locks a contract and sets the missed
// proof outputs to the valid proof outputs.
func ClearingRevision(revision types.FileContractRevision, outputValues []types.Currency) (types.FileContractRevision, error) {
	if revision.RevisionNumber == math.MaxUint64 {
		return types.FileContractRevision{}, errors.New("contract is locked")
	} else if len(outputValues) != len(revision.ValidProofOutputs) {
		return types.FileContractRevision{}, errors.New("incorrect number of outputs")
	}

	oldValid := revision.ValidProofOutputs
	revision.ValidProofOutputs = make([]types.SiacoinOutput, len(outputValues))
	for i := range outputValues {
		revision.ValidProofOutputs[i].Address = oldValid[i].Address
		revision.ValidProofOutputs[i].Value = outputValues[i]
	}
	revision.MissedProofOutputs = revision.ValidProofOutputs
	revision.RevisionNumber = math.MaxUint64
	revision.Filesize = 0
	revision.FileMerkleRoot = types.Hash256{}
	return revision, nil
}

// ValidateClearingRevision verifies that the revision locks the current
// contract by setting its revision number to the maximum value and the valid
// and missed proof outputs are the same.
func ValidateClearingRevision(current, final types.FileContractRevision, finalPayment types.Currency) (types.Currency, error) {
	switch {
	case final.Filesize != 0:
		return types.ZeroCurrency, errors.New("filesize must be 0")
	case final.FileMerkleRoot != types.Hash256{}:
		return types.ZeroCurrency, errors.New("file merkle root must be empty")
	case current.WindowStart != final.WindowStart:
		return types.ZeroCurrency, errors.New("window start must not change")
	case current.WindowEnd != final.WindowEnd:
		return types.ZeroCurrency, errors.New("window end must not change")
	case len(final.MissedProofOutputs) != 2:
		return types.ZeroCurrency, errors.New("wrong number of proof outputs")
	case len(final.ValidProofOutputs) != len(final.MissedProofOutputs):
		return types.ZeroCurrency, errors.New("valid proof outputs must equal missed proof outputs")
	case final.RevisionNumber != types.MaxRevisionNumber:
		return types.ZeroCurrency, errors.New("revision number must be max value")
	case final.UnlockHash != current.UnlockHash:
		return types.ZeroCurrency, errors.New("unlock hash must not change")
	case final.UnlockConditions.UnlockHash() != current.UnlockConditions.UnlockHash():
		return types.ZeroCurrency, errors.New("unlock conditions must not change")
	}

	fromRenter, underflow := current.ValidRenterPayout().SubWithUnderflow(final.MissedRenterPayout())
	if underflow {
		return types.ZeroCurrency, errors.New("renter valid payout must not increase")
	}

	toHost, underflow := final.ValidHostPayout().SubWithUnderflow(current.ValidHostPayout())
	if underflow {
		return types.ZeroCurrency, errors.New("host valid payout must not decrease")
	} else if !fromRenter.Equals(toHost) {
		return types.ZeroCurrency, fmt.Errorf("expected host to receive %v, but got %v", fromRenter, toHost)
	} else if fromRenter.Cmp(finalPayment) < 0 {
		return types.ZeroCurrency, fmt.Errorf("expected host payment of at least %v, but got %v", finalPayment, fromRenter)
	}

	// validate both valid and missed outputs are equal to the current valid
	// proof outputs.
	for i := range final.ValidProofOutputs {
		current, valid, missed := current.ValidProofOutputs[i], final.ValidProofOutputs[i], final.MissedProofOutputs[i]
		switch {
		case valid.Address != current.Address: // address doesn't change
			return types.ZeroCurrency, fmt.Errorf("valid proof output address %v must not change", i)
		case valid.Address != missed.Address: // valid and missed address are the same
			return types.ZeroCurrency, fmt.Errorf("missed proof output address %v must equal valid proof output", i)
		case !valid.Value.Equals(missed.Value): // valid and missed value are the same
			return types.ZeroCurrency, fmt.Errorf("missed proof output %v must equal valid proof output", i)
		}
	}
	return toHost, nil
}

// ValidateRevision verifies that a new revision is valid given the current
// revision. Only the revision number and proof output values are allowed to
// change
func ValidateRevision(current, revision types.FileContractRevision, payment, collateral types.Currency) (transfer, burn types.Currency, err error) {
	if err := validateStdRevision(current, revision); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	// validate the current revision has enough funds
	switch {
	case current.ValidRenterPayout().Cmp(payment) < 0:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("renter valid proof output must be greater than the payment amount")
	case current.MissedRenterPayout().Cmp(payment) < 0:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("renter missed proof output must be greater than the payment amount")
	case current.MissedHostPayout().Cmp(collateral) < 0:
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("host missed proof output must be greater than the collateral amount")
	}

	fromRenter, underflow := current.ValidRenterPayout().SubWithUnderflow(revision.ValidRenterPayout())
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("renter valid payout must decrease")
	}

	toHost, overflow := revision.ValidHostPayout().SubWithUnderflow(current.ValidHostPayout())
	if overflow {
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("host valid payout must increase")
	}

	hostBurn, underflow := current.MissedHostPayout().SubWithUnderflow(revision.MissedHostPayout())
	if underflow {
		return types.ZeroCurrency, types.ZeroCurrency, errors.New("host missed payout must decrease")
	}

	switch {
	case !fromRenter.Equals(toHost):
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("expected %d to be transferred from renter to host, got %d", fromRenter, toHost)
	case toHost.Cmp(payment) < 0:
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("insufficient host transfer: expected at least %d, got %d", payment, toHost)
	case hostBurn.Cmp(collateral) > 0:
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("excessive collateral transfer: expected at most %d, got %d", collateral, hostBurn)
	}
	return toHost, hostBurn, nil
}

// ValidateProgramRevision verifies that a contract program revision is valid
// and only the missed host value and burn value are modified by the expected
// burn amount. All other usage will have been paid for by the RPC budget.
func ValidateProgramRevision(current, revision types.FileContractRevision, storage, collateral types.Currency) (burn types.Currency, _ error) {
	if err := validateStdRevision(current, revision); err != nil {
		return types.ZeroCurrency, err
	}

	// calculate the amount of SC that the host is expected to burn
	hostBurn, underflow := current.MissedHostPayout().SubWithUnderflow(revision.MissedHostPayout())
	if underflow {
		return types.ZeroCurrency, errors.New("host missed payout must decrease")
	}

	// validate that the host is not burning more than the expected amount
	expectedBurn := storage.Add(collateral)
	if hostBurn.Cmp(expectedBurn) > 0 {
		return types.ZeroCurrency, fmt.Errorf("host expected to burn at most %d, but burned %d", expectedBurn, hostBurn)
	}

	// validate that the void burn value is equal to the host burn value
	// note: this check is covered by validateStdRevision, but it's
	// good to be explicit.
	voidBurn, underflow := revision.MissedProofOutputs[2].Value.SubWithUnderflow(current.MissedProofOutputs[2].Value)
	if underflow {
		return types.ZeroCurrency, errors.New("void output value must increase")
	} else if !voidBurn.Equals(hostBurn) {
		return types.ZeroCurrency, fmt.Errorf("host burn value %d should match void burn value %d", hostBurn, voidBurn)
	}

	// validate no other values have changed
	switch {
	case !current.ValidRenterPayout().Equals(revision.ValidRenterPayout()):
		return types.ZeroCurrency, errors.New("renter valid proof output must not change")
	case !current.ValidHostPayout().Equals(revision.ValidHostPayout()):
		return types.ZeroCurrency, errors.New("host valid proof output must not change")
	case !current.MissedRenterPayout().Equals(revision.MissedRenterPayout()):
		return types.ZeroCurrency, errors.New("renter missed proof output must not change")
	}
	return hostBurn, nil
}

// ValidatePaymentRevision verifies that a payment revision is valid and the
// amount is properly deducted from both renter outputs and added to both host
// outputs. Signatures are not validated.
func ValidatePaymentRevision(current, revision types.FileContractRevision, payment types.Currency) error {
	if err := validateStdRevision(current, revision); err != nil {
		return err
	}
	// validate that all outputs are consistent with only transferring the
	// payment from the renter payouts to the host payouts.
	switch {
	case revision.ValidRenterPayout().Cmp(current.ValidRenterPayout().Sub(payment)) != 0:
		return errors.New("renter valid proof output is not reduced by the payment amount")
	case revision.MissedRenterPayout().Cmp(current.MissedRenterPayout().Sub(payment)) != 0:
		return errors.New("renter missed proof output is not reduced by the payment amount")
	case revision.ValidHostPayout().Cmp(current.ValidHostPayout().Add(payment)) != 0:
		return errors.New("host valid proof output is not increased by the payment amount")
	case revision.MissedHostPayout().Cmp(current.MissedHostPayout().Add(payment)) != 0:
		return errors.New("host missed proof output is not increased by the payment amount")
	}
	return nil
}
