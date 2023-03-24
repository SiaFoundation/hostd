package rhp_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
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
	} else if !reflect.DeepEqual(hostSettings, renterSettings) {
		t.Errorf("host settings mismatch")
	}
}

func TestUploadDownload(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
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
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
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

		cost := rhpv2.ContractRenewalCost(state, renewed, settings.ContractPrice, types.ZeroCurrency, basePrice)
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

		old, err := host.Contracts().Contract(origin.ID())
		if err != nil {
			t.Fatal(err)
		} else if old.Revision.Filesize != 0 {
			t.Fatal("filesize mismatch")
		} else if old.Revision.FileMerkleRoot != (types.Hash256{}) {
			t.Fatal("merkle root mismatch")
		} else if old.RenewedTo != renewal.ID() {
			t.Fatal("renewed to mismatch")
		}

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
		} else if contract.RenewedFrom != origin.ID() {
			t.Fatalf("expected renewed from %s, got %s", origin.ID(), contract.RenewedFrom)
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

		cost := rhpv2.ContractRenewalCost(state, renewed, settings.ContractPrice, types.ZeroCurrency, basePrice)
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

		old, err := host.Contracts().Contract(origin.ID())
		if err != nil {
			t.Fatal(err)
		} else if old.Revision.Filesize != 0 {
			t.Fatal("filesize mismatch")
		} else if old.Revision.FileMerkleRoot != (types.Hash256{}) {
			t.Fatal("merkle root mismatch")
		} else if old.RenewedTo != renewal.ID() {
			t.Fatal("renewed to mismatch")
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
		} else if contract.RenewedFrom != origin.ID() {
			t.Fatalf("expected renewed from %s, got %s", origin.ID(), contract.RenewedFrom)
		}
	})
}

func BenchmarkUpload(b *testing.B) {
	log := zaptest.NewLogger(b)
	renter, host, err := test.NewTestingPair(b.TempDir(), log)
	if err != nil {
		b.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	// form a contract
	contract, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), 200)
	if err != nil {
		b.Fatal(err)
	}

	session, err := renter.NewRHP2Session(context.Background(), host.RHPv2Addr(), host.PublicKey(), contract.ID())
	if err != nil {
		b.Fatal(err)
	}
	defer session.Close()

	// calculate the remaining duration of the contract
	var remainingDuration uint64
	contractExpiration := uint64(session.Revision().Revision.WindowStart)
	currentHeight := renter.TipState().Index.Height
	if contractExpiration < currentHeight {
		b.Fatal("contract expired")
	}
	// calculate the cost of uploading a sector
	remainingDuration = contractExpiration - currentHeight
	price, collateral := rhpv2.RPCAppendCost(session.Settings(), remainingDuration)

	// generate b.N sectors
	sectors := make([][rhpv2.SectorSize]byte, b.N)
	for i := range sectors {
		frand.Read(sectors[i][:256])
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(rhpv2.SectorSize)

	// upload b.N sectors
	for i := 0; i < b.N; i++ {
		sector := sectors[i]

		// upload the sector
		if _, err := session.Append(context.Background(), &sector, price, collateral); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	log := zaptest.NewLogger(b)
	renter, host, err := test.NewTestingPair(b.TempDir(), log)
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
