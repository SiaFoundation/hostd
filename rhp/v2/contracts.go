package rhp

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/siad/crypto"
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

func hashRevision(rev types.FileContractRevision) [32]byte {
	or := (*objFileContractRevision)(&rev)
	var b objBuffer
	b.buf = *bytes.NewBuffer(make([]byte, 0, 1024))
	b.grow(or.marshalledSize()) // just in case 1024 is too small
	or.marshalBuffer(&b)
	return blake2b.Sum256(b.bytes())
}

func hashChallenge(challenge [16]byte) [32]byte {
	c := make([]byte, 32)
	copy(c[:16], "challenge")
	copy(c[16:], challenge[:])
	return blake2b.Sum256(c)
}

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

// revise updates the contract revision with the provided values
func revise(revision types.FileContractRevision, revisionNumber uint64, validOutputs, missedOutputs []types.Currency) (types.FileContractRevision, error) {
	if len(validOutputs) != len(revision.NewValidProofOutputs) || len(missedOutputs) != len(revision.NewMissedProofOutputs) {
		return types.FileContractRevision{}, errors.New("incorrect number of outputs")
	}
	revision.NewRevisionNumber = revisionNumber
	oldValid, oldMissed := revision.NewValidProofOutputs, revision.NewMissedProofOutputs
	revision.NewValidProofOutputs = make([]types.SiacoinOutput, len(validOutputs))
	revision.NewMissedProofOutputs = make([]types.SiacoinOutput, len(missedOutputs))
	for i := range validOutputs {
		revision.NewValidProofOutputs[i].UnlockHash = oldValid[i].UnlockHash
		revision.NewValidProofOutputs[i].Value = validOutputs[i]
	}
	for i := range missedOutputs {
		revision.NewMissedProofOutputs[i].UnlockHash = oldMissed[i].UnlockHash
		revision.NewMissedProofOutputs[i].Value = missedOutputs[i]
	}
	return revision, nil
}

// validateContractFormation verifies that the new contract is valid given the
// host's settings.
func validateContractFormation(fc types.FileContract, hostKey, renterKey types.SiaPublicKey, currentHeight uint64, settings HostSettings) error {
	switch {
	case fc.FileSize != 0:
		return errors.New("initial filesize should be 0")
	case fc.RevisionNumber != 0:
		return errors.New("initial revision number should be 0")
	case fc.FileMerkleRoot != crypto.Hash{}:
		return errors.New("initial Merkle root should be empty")
	case fc.WindowStart < types.BlockHeight(currentHeight+settings.WindowSize):
		return errors.New("contract ends too soon to safely submit the contract transaction")
	case fc.WindowStart > types.BlockHeight(currentHeight+settings.MaxDuration):
		return errors.New("contract duration is too long")
	case fc.WindowEnd < fc.WindowStart+types.BlockHeight(settings.WindowSize):
		return errors.New("proof window is too small")
	case len(fc.ValidProofOutputs) != 2:
		return errors.New("wrong number of valid proof outputs")
	case len(fc.MissedProofOutputs) != 3:
		return errors.New("wrong number of missed proof outputs")
	case fc.ValidProofOutputs[1].UnlockHash != settings.UnlockHash:
		return errors.New("wrong address for host valid output")
	case fc.MissedProofOutputs[1].UnlockHash != settings.UnlockHash:
		return errors.New("wrong address for host missed output")
	case fc.ValidProofOutputs[1].Value.Cmp(settings.ContractPrice) < 0:
		return errors.New("host valid payout is too small")
	case fc.ValidProofOutputs[1].Value.Cmp(fc.MissedProofOutputs[1].Value) != 0:
		return errors.New("host valid and missed outputs must be equal")
	case fc.ValidProofOutputs[1].Value.Cmp(settings.MaxCollateral) > 0:
		return errors.New("excessive initial collateral")
	case fc.UnlockHash != contractUnlockConditions(hostKey, renterKey).UnlockHash():
		return errors.New("incorrect unlock hash")
	}
	return nil
}

// validateContractRenewal verifies that the renewed contract is valid given the
// old contract. A renewal is valid if the contract fields match and the
// revision number is 0.
func validateContractRenewal(existing types.FileContractRevision, renewal types.FileContract, hostKey, renterKey types.SiaPublicKey, renterCost, hostBurn types.Currency, currentHeight uint64, settings HostSettings) error {
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

// validateContractFinalization verifies that the revision locks the current
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
	case final.NewUnlockHash != current.NewUnlockHash:
		return errors.New("unlock hash must not change")
	case final.UnlockConditions.UnlockHash() != current.UnlockConditions.UnlockHash():
		return errors.New("unlock conditions must not change")
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
func validateRevision(current, revision types.FileContractRevision, payment, collateral types.Currency) error {
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

	minValid := current.NewValidProofOutputs[1].Value.Add(payment)
	maxMissed := current.NewMissedProofOutputs[1].Value.Sub(collateral)

	switch {
	case validPayout.Cmp(oldPayout) != 0:
		return errors.New("valid proof output sum must not change")
	case missedPayout.Cmp(oldPayout) != 0:
		return errors.New("missed proof output sum must not change")
	case revision.NewUnlockHash != current.NewUnlockHash:
		return errors.New("unlock hash must not change")
	case revision.UnlockConditions.UnlockHash() != current.UnlockConditions.UnlockHash():
		return errors.New("unlock conditions must not change")
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
	case revision.NewValidProofOutputs[1].Value.Cmp(minValid) < 0:
		return fmt.Errorf("insufficient host valid payment: expected value at least %v, got %v", minValid, revision.NewValidProofOutputs[1].Value)
	case revision.NewMissedProofOutputs[1].Value.Cmp(maxMissed) > 0:
		return fmt.Errorf("too much collateral transfer: expected value at most %v, got %v", maxMissed, revision.NewMissedProofOutputs[1].Value)
	}
	return nil
}

// validateWriteActions validates the actions received by the renter for the
// write RPC and estimates the cost of completing the actions.
func validateWriteActions(actions []rpcWriteAction, oldSectors uint64, proof bool, remainingDuration uint64, settings HostSettings) (cost, collateral types.Currency, _ error) {
	var uploadBytes uint64
	newSectors := oldSectors
	for _, action := range actions {
		switch action.Type {
		case rpcWriteActionAppend:
			if len(action.Data) != SectorSize {
				return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("invalid sector size: %v: %w", len(action.Data), ErrInvalidSectorLength)
			}
			newSectors++
			uploadBytes += SectorSize
		case rpcWriteActionTrim:
			if action.A > newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrTrimOutOfBounds
			}
			newSectors -= action.A
		case rpcWriteActionSwap:
			if action.A >= newSectors || action.B >= newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrSwapOutOfBounds
			}
		case rpcWriteActionUpdate:
			idx, offset := action.A, action.B
			if idx >= newSectors {
				return types.ZeroCurrency, types.ZeroCurrency, ErrUpdateOutOfBounds
			} else if offset+uint64(len(action.Data)) > SectorSize {
				return types.ZeroCurrency, types.ZeroCurrency, ErrOffsetOutOfBounds
			} else if proof && (offset%merkle.LeafSize != 0) || len(action.Data)%merkle.LeafSize != 0 {
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
		cost = cost.Add(settings.StoragePrice.Mul64(SectorSize * additionalSectors * remainingDuration)) // cost of storing the new sectors
		collateral = settings.Collateral.Mul64(SectorSize * additionalSectors)                           // collateral for the new sectors
	}

	if proof {
		// estimate cost of Merkle proof
		proofSize := crypto.HashSize * (128 + len(actions))
		cost = cost.Add(settings.DownloadBandwidthPrice.Mul64(uint64(proofSize)))
	}
	return cost, collateral, nil
}
