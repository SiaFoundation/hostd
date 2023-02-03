package rhp_test

import (
	"context"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
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

	// mine a block to confirm the contract
	if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
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
	contractExpiration := uint64(session.Revision().Revision.WindowEnd)
	currentHeight := renter.TipState().Index.Height
	if contractExpiration < currentHeight {
		t.Fatal("contract expired")
	}
	// upload the sector
	remainingDuration = contractExpiration - currentHeight
	price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)
	if writtenRoot, err := session.Append(context.Background(), &sector, price, collateral); err != nil {
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
}

func BenchmarkUpload(b *testing.B) {
	renter, host, err := test.NewTestingPair(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	if err := host.AddVolume(filepath.Join(b.TempDir(), "storage.dat"), uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	// form a contract
	contract, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), 200)
	if err != nil {
		b.Fatal(err)
	}

	// mine a block to confirm the contract
	if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
		b.Fatal(err)
	}

	session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), contract.ID())
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()

	// calculate the remaining duration of the contract
	var remainingDuration uint64
	contractExpiration := uint64(session.Revision().Revision.WindowEnd)
	currentHeight := renter.TipState().Index.Height
	if contractExpiration < currentHeight {
		b.Fatal("contract expired")
	}
	remainingDuration = contractExpiration - currentHeight

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(rhpv2.SectorSize)

	// upload b.N sectors
	for i := 0; i < b.N; i++ {
		// generate a sector
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])

		// upload the sector
		price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)
		if _, err := session.Append(context.Background(), &sector, price, collateral); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	renter, host, err := test.NewTestingPair(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	if err := host.AddVolume(filepath.Join(b.TempDir(), "storage.dat"), uint64(b.N)); err != nil {
		b.Fatal(err)
	}

	// form a contract
	contract, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), 200)
	if err != nil {
		b.Fatal(err)
	}

	// mine a block to confirm the contract
	if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
		b.Fatal(err)
	}

	session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), contract.ID())
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()

	// calculate the remaining duration of the contract
	var remainingDuration uint64
	contractExpiration := uint64(session.Revision().Revision.WindowEnd)
	currentHeight := renter.TipState().Index.Height
	if contractExpiration < currentHeight {
		b.Fatal("contract expired")
	}
	remainingDuration = contractExpiration - currentHeight

	var uploaded []types.Hash256
	// upload b.N sectors
	for i := 0; i < b.N; i++ {
		// generate a sector
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])

		// upload the sector
		price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)
		root, err := session.Append(context.Background(), &sector, price, collateral)
		if err != nil {
			b.Fatal(err)
		}
		uploaded = append(uploaded, root)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(rhpv2.SectorSize)

	for _, root := range uploaded {
		// download the sector
		sections := []rhpv2.RPCReadRequestSection{{
			MerkleRoot: root,
			Offset:     0,
			Length:     rhpv2.SectorSize,
		}}
		price := rhpv2.RPCReadCost(session.Settings(), sections)
		if err := session.Read(context.Background(), io.Discard, sections, price); err != nil {
			b.Fatal(err)
		}
	}
}
