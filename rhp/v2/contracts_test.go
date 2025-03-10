package rhp

import (
	"math"
	"testing"

	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func TestValidateContractRenewal(t *testing.T) {
	hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)).PublicKey(), types.NewPrivateKeyFromSeed(frand.Bytes(32)).PublicKey()
	hostAddress, renterAddress := types.StandardUnlockHash(hostKey), types.StandardUnlockHash(renterKey)
	hostCollateral := types.NewCurrency64(frand.Uint64n(math.MaxUint64))
	renterAllowance := types.NewCurrency64(frand.Uint64n(math.MaxUint64))

	settings := rhp2.HostSettings{
		MaxDuration:   math.MaxUint64,
		MaxCollateral: types.NewCurrency(math.MaxUint64, math.MaxUint64),
		Address:       hostAddress,
	}

	existing := types.FileContractRevision{
		ParentID: types.FileContractID{1},
		UnlockConditions: types.UnlockConditions{
			PublicKeys:         []types.UnlockKey{renterKey.UnlockKey(), hostKey.UnlockKey()},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			RevisionNumber: frand.Uint64n(math.MaxUint64),
			Filesize:       frand.Uint64n(math.MaxUint64),
			FileMerkleRoot: frand.Entropy256(),
			WindowStart:    100,
			WindowEnd:      300,
			Payout:         types.ZeroCurrency, // not validated here
			UnlockHash: types.UnlockConditions{
				PublicKeys:         []types.UnlockKey{renterKey.UnlockKey(), hostKey.UnlockKey()},
				SignaturesRequired: 2,
			}.UnlockHash(),
			ValidProofOutputs: []types.SiacoinOutput{
				{Address: renterAddress, Value: renterAllowance},
				{Address: hostAddress, Value: hostCollateral},
			},
			MissedProofOutputs: []types.SiacoinOutput{
				{Address: renterAddress, Value: renterAllowance},
				{Address: hostAddress, Value: hostCollateral},
				{Address: types.VoidAddress, Value: types.ZeroCurrency},
			},
		},
	}

	renewal := types.FileContract{
		Filesize:       existing.Filesize,
		FileMerkleRoot: existing.FileMerkleRoot,
		WindowStart:    existing.WindowStart + 100,
		WindowEnd:      existing.WindowEnd + 100,
		ValidProofOutputs: []types.SiacoinOutput{
			{Address: renterAddress, Value: renterAllowance},
			{Address: hostAddress, Value: hostCollateral},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			{Address: renterAddress, Value: renterAllowance},
			{Address: hostAddress, Value: hostCollateral},
			{Address: types.VoidAddress, Value: types.ZeroCurrency},
		},
	}

	// bad renter key
	badRenterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)).PublicKey().UnlockKey()
	renewal.UnlockHash = contractUnlockConditions(hostKey.UnlockKey(), badRenterKey).UnlockHash()
	_, _, _, err := validateContractRenewal(existing, renewal, hostKey.UnlockKey(), renterKey.UnlockKey(), types.ZeroCurrency, types.ZeroCurrency, 0, settings)
	if err == nil || err.Error() != "incorrect unlock hash" {
		t.Fatalf("expected unlock hash error, got %v", err)
	}

	// bad host key
	badHostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)).PublicKey().UnlockKey()
	renewal.UnlockHash = contractUnlockConditions(badHostKey, renterKey.UnlockKey()).UnlockHash()
	_, _, _, err = validateContractRenewal(existing, renewal, hostKey.UnlockKey(), renterKey.UnlockKey(), types.ZeroCurrency, types.ZeroCurrency, 0, settings)
	if err == nil || err.Error() != "incorrect unlock hash" {
		t.Fatalf("expected unlock hash error, got %v", err)
	}

	// original keys
	renewal.UnlockHash = contractUnlockConditions(hostKey.UnlockKey(), renterKey.UnlockKey()).UnlockHash()
	_, _, _, err = validateContractRenewal(existing, renewal, hostKey.UnlockKey(), renterKey.UnlockKey(), types.ZeroCurrency, types.ZeroCurrency, 0, settings)
	if err != nil {
		t.Fatal(err)
	}

	// different renter key, same host key
	newRenterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)).PublicKey().UnlockKey()
	renewal.UnlockHash = contractUnlockConditions(hostKey.UnlockKey(), newRenterKey).UnlockHash()
	_, _, _, err = validateContractRenewal(existing, renewal, hostKey.UnlockKey(), newRenterKey, types.ZeroCurrency, types.ZeroCurrency, 0, settings)
	if err != nil {
		t.Fatal(err)
	}
}
