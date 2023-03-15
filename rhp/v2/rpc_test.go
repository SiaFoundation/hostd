package rhp_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/renterd/wallet"
	"lukechampine.com/frand"
)

func TestSettings(t *testing.T) {
	renter, host, err := test.NewTestingPair(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	hostSettings, err := host.RHPv2Settings()
	if err != nil {
		t.Fatal(err)
	}

	renterSettings, err := renter.Settings(context.Background(), host.RHPv2Addr(), host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// note: cannot use reflect.DeepEqual directly because the types are different
	hostVal := reflect.ValueOf(hostSettings)
	renterVal := reflect.ValueOf(renterSettings)
	if hostVal.NumField() != renterVal.NumField() {
		t.Fatalf("mismatched number of fields: host %v, renter %v", hostVal.NumField(), renterVal.NumField())
	}

	for i := 0; i < hostVal.NumField(); i++ {
		fieldName := hostVal.Type().Field(i).Name
		hostField := hostVal.FieldByName(fieldName)
		renterField := renterVal.FieldByName(fieldName)

		// check if the types are equal
		if hostField.Kind() != renterField.Kind() {
			t.Fatalf("field %s mismatch: host %v, renter %v", fieldName, hostField.Kind(), renterField.Kind())
		}

		// get the underlying values
		va := hostField.Interface()
		vb := renterField.Interface()

		if !reflect.DeepEqual(va, vb) {
			t.Errorf("field %s mismatch: host %v, renter %v", fieldName, hostField.Interface(), renterField.Interface())
		}
	}
}

func TestUploadDownload(t *testing.T) {
	renter, host, err := test.NewTestingPair(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	// form a contract
	contract, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), 200)
	if err != nil {
		t.Fatal(err)
	}

	session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), contract.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// generate a sector
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:256])
	sectorRoot := rhpv2.SectorRoot(&sector)

	// calculate the remaining duration of the contract
	var remainingDuration uint64
	contractExpiration := uint64(session.Revision().Revision.WindowStart)
	currentHeight := renter.TipState().Index.Height
	if contractExpiration < currentHeight {
		t.Fatal("contract expired")
	}
	// upload the sector
	remainingDuration = contractExpiration - currentHeight
	price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)
	writtenRoot, err := session.Append(context.Background(), &sector, price, collateral)
	if err != nil {
		t.Fatal(err)
	} else if writtenRoot != sectorRoot {
		t.Fatal("sector root mismatch")
	}

	// check the host's sector roots matches the sector we just uploaded
	price = rhpv2.RPCSectorRootsCost(session.Settings(), 1)
	roots, err := session.SectorRoots(context.Background(), 0, 1, price)
	if err != nil {
		t.Fatal(err)
	} else if roots[0] != sectorRoot {
		t.Fatal("sector root mismatch")
	}

	// check that the revision fields are correct
	revision := session.Revision().Revision
	switch {
	case revision.Filesize != rhpv2.SectorSize:
		t.Fatal("wrong filesize")
	case revision.FileMerkleRoot != sectorRoot:
		t.Fatal("wrong merkle root")
	}

	sections := []rhpv2.RPCReadRequestSection{
		{
			MerkleRoot: writtenRoot,
			Offset:     0,
			Length:     rhpv2.SectorSize,
		},
	}

	price = rhpv2.RPCReadCost(session.Settings(), sections)

	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := session.Read(ctx, &buf, sections, price); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("sector mismatch")
	}
}

func TestRenew(t *testing.T) {
	renter, host, err := test.NewTestingPair(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	t.Run("empty contract", func(t *testing.T) {
		state := renter.TipState()
		// form a contract
		origin, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), state.Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}

		session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), origin.ID())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()

		// mine a few blocks into the contract
		if err := host.MineBlocks(host.WalletAddress(), 10); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		renewHeight := origin.Revision.WindowStart + 10
		settings := session.Settings()
		current := session.Revision().Revision
		additionalCollateral := rhpv2.ContractRenewalCollateral(current.FileContract, 1<<22, settings, renter.TipState().Index.Height, renewHeight)
		renewed, basePrice := rhpv2.PrepareContractRenewal(current, renter.WalletAddress(), renter.PrivateKey(), types.Siacoins(10), additionalCollateral, host.PublicKey(), settings, renewHeight)
		renewalTxn := types.Transaction{
			FileContracts: []types.FileContract{renewed},
		}

		cost := rhpv2.ContractRenewalCost(renewed, settings.ContractPrice, types.ZeroCurrency, basePrice)
		toSign, discard, err := renter.Wallet().FundTransaction(&renewalTxn, cost)
		if err != nil {
			t.Fatal(err)
		}
		defer discard()

		if err := renter.Wallet().SignTransaction(host.TipState(), &renewalTxn, toSign, wallet.ExplicitCoveredFields(renewalTxn)); err != nil {
			t.Fatal(err)
		}

		renewal, _, err := session.RenewContract(context.Background(), []types.Transaction{renewalTxn}, types.ZeroCurrency)
		if err != nil {
			t.Fatal(err)
		}

		// mine a block to confirm the revision
		if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		contract, err := host.Contracts().Contract(renewal.ID())
		if err != nil {
			t.Fatal(err)
		} else if contract.Revision.Filesize != origin.Revision.Filesize {
			t.Fatal("filesize mismatch")
		} else if contract.Revision.FileMerkleRoot != origin.Revision.FileMerkleRoot {
			t.Fatal("merkle root mismatch")
		} else if !contract.LockedCollateral.Equals(additionalCollateral) {
			t.Fatalf("locked collateral mismatch: expected %d, got %d", additionalCollateral, contract.LockedCollateral)
		} else if !contract.Usage.RiskedCollateral.IsZero() {
			t.Fatalf("expected zero risked collateral, got %d", contract.Usage.RiskedCollateral)
		} else if !contract.Usage.RPCRevenue.Equals(settings.ContractPrice) {
			t.Fatalf("expected %d RPC revenue, got %d", settings.ContractPrice, contract.Usage.RPCRevenue)
		}
	})

	t.Run("non-empty contract", func(t *testing.T) {
		// form a contract
		state := renter.TipState()
		origin, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), state.Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}

		session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), origin.ID())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()

		// generate a sector
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])
		sectorRoot := rhpv2.SectorRoot(&sector)

		// calculate the remaining duration of the contract
		var remainingDuration uint64
		contractExpiration := uint64(session.Revision().Revision.WindowStart)
		currentHeight := renter.TipState().Index.Height
		if contractExpiration < currentHeight {
			t.Fatal("contract expired")
		}
		// upload the sector
		remainingDuration = contractExpiration - currentHeight
		price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)
		writtenRoot, err := session.Append(context.Background(), &sector, price, collateral)
		if err != nil {
			t.Fatal(err)
		} else if writtenRoot != sectorRoot {
			t.Fatal("sector root mismatch")
		}

		// mine a few blocks into the contract
		if err := host.MineBlocks(host.WalletAddress(), 10); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		settings := session.Settings()
		renewHeight := origin.Revision.WindowStart + 10
		current := session.Revision().Revision
		additionalCollateral := rhpv2.ContractRenewalCollateral(current.FileContract, 1<<22, settings, renter.TipState().Index.Height, renewHeight)
		renewed, basePrice := rhpv2.PrepareContractRenewal(session.Revision().Revision, renter.WalletAddress(), renter.PrivateKey(), types.Siacoins(10), additionalCollateral, host.PublicKey(), settings, renewHeight)
		renewalTxn := types.Transaction{
			FileContracts: []types.FileContract{renewed},
		}

		cost := rhpv2.ContractRenewalCost(renewed, settings.ContractPrice, types.ZeroCurrency, basePrice)
		toSign, discard, err := renter.Wallet().FundTransaction(&renewalTxn, cost)
		if err != nil {
			t.Fatal(err)
		}
		defer discard()

		if err := renter.Wallet().SignTransaction(host.TipState(), &renewalTxn, toSign, wallet.ExplicitCoveredFields(renewalTxn)); err != nil {
			t.Fatal(err)
		}

		renewal, _, err := session.RenewContract(context.Background(), []types.Transaction{renewalTxn}, types.ZeroCurrency)
		if err != nil {
			t.Fatal(err)
		}

		contract, err := host.Contracts().Contract(renewal.ID())
		if err != nil {
			t.Fatal(err)
		} else if contract.Revision.Filesize != current.Filesize {
			t.Fatal("filesize mismatch")
		} else if contract.Revision.FileMerkleRoot != current.FileMerkleRoot {
			t.Fatal("merkle root mismatch")
		} else if contract.LockedCollateral.Cmp(additionalCollateral) <= 0 {
			t.Fatalf("locked collateral mismatch: expected at least %d, got %d", additionalCollateral, contract.LockedCollateral)
		} else if !contract.Usage.RPCRevenue.Equals(settings.ContractPrice) {
			t.Fatalf("expected %d RPC revenue, got %d", settings.ContractPrice, contract.Usage.RPCRevenue)
		}
	})
}
