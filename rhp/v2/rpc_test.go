package rhp_test

import (
	"reflect"
	"testing"

	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/siad/types"
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

	settings, err := renter.Settings(host.RHPv2Addr(), host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(hostSettings, settings) {
		t.Fatal("settings mismatch")
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
	contract, err := renter.FormContract(host.RHPv2Addr(), host.PublicKey(), types.SiacoinPrecision.Mul64(10), 200)
	if err != nil {
		t.Fatal(err)
	}

	// mine a block to confirm the contract
	if err := host.MineBlocks(1); err != nil {
		t.Fatal(err)
	}

	session, err := renter.NewRHP2Session(host.RHPv2Addr(), host.PublicKey(), contract.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// generate a sector
	sector := make([]byte, rhp.SectorSize)
	frand.Read(sector[:256])
	sectorRoot := merkle.SectorRoot(sector)

	// upload the sector
	if writtenRoot, err := session.Append(sector); err != nil {
		t.Fatal(err)
	} else if writtenRoot != sectorRoot {
		t.Fatal("sector root mismatch")
	}

	// check the host's sector roots matches the sector we just uploaded
	roots, err := session.SectorRoots(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if roots[0] != sectorRoot {
		t.Fatal("sector root mismatch")
	}

	// check that the revision fields are correct
	revision := session.Revision()
	switch {
	case revision.NewFileSize != rhp.SectorSize:
		t.Fatal("wrong filesize")
	case revision.NewFileMerkleRoot != sectorRoot:
		t.Fatal("wrong merkle root")
	}
}
