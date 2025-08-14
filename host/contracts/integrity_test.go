package contracts_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestIntegrityResultJSON(t *testing.T) {
	type test struct {
		Name string
		Func func() contracts.IntegrityResult
	}
	tests := []test{
		{"without error", func() contracts.IntegrityResult {
			return contracts.IntegrityResult{
				ExpectedRoot: frand.Entropy256(),
				ActualRoot:   frand.Entropy256(),
			}
		}},
		{"with error", func() contracts.IntegrityResult {
			return contracts.IntegrityResult{
				ExpectedRoot: frand.Entropy256(),
				ActualRoot:   frand.Entropy256(),
				Error:        errors.New("foo"),
			}
		}},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			a := test.Func()

			buf, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}

			var b contracts.IntegrityResult
			if err := json.Unmarshal(buf, &b); err != nil {
				t.Fatal(err)
			}

			switch {
			case a.ExpectedRoot != b.ExpectedRoot:
				t.Fatal("ExpectedRoot does not match")
			case a.ActualRoot != b.ActualRoot:
				t.Fatal("ActualRoot does not match")
			case a.Error != nil && a.Error.Error() != b.Error.Error():
				t.Fatal("Error does not match")
			}
		})
	}
}

func TestCheckIntegrity(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))
	n, genesis := testutil.V2Network()
	host := testutil.NewHostNode(t, hostKey, n, genesis, log)

	volumePath := filepath.Join(t.TempDir(), "data.dat")
	result := make(chan error, 1)
	if _, err := host.Volumes.AddVolume(context.Background(), volumePath, 10, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	// mine enough for the wallet to have some funds
	testutil.MineAndSync(t, host, host.Wallet.Address(), int(n.MaturityDelay+5))

	contractID, _ := formV2Contract(t, host.Chain, host.Contracts, host.Wallet, renterKey, hostKey, types.Siacoins(500), types.Siacoins(1000), 10, true)
	contract, err := host.Contracts.V2Contract(contractID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.V2ContractStatusPending {
		t.Fatal("expected contract to be pending")
	}
	testutil.MineAndSync(t, host, types.VoidAddress, 1)

	contract, err = host.Contracts.V2Contract(contractID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.V2ContractStatusActive {
		t.Fatal("expected contract to be active")
	}

	var roots []types.Hash256
	for range 5 {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		root := proto4.SectorRoot(&sector)
		err := host.Volumes.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}

	cs := host.Chain.TipState()
	contract.V2FileContract.RevisionNumber++
	contract.V2FileContract.Filesize = uint64(len(roots)) * proto4.SectorSize
	contract.V2FileContract.Capacity = contract.V2FileContract.Filesize
	contract.V2FileContract.FileMerkleRoot = proto4.MetaRoot(roots)
	sigHash := cs.ContractSigHash(contract.V2FileContract)
	contract.V2FileContract.RenterSignature = renterKey.SignHash(sigHash)
	contract.V2FileContract.HostSignature = hostKey.SignHash(sigHash)

	err = host.Contracts.ReviseV2Contract(contract.ID, contract.V2FileContract, roots, proto4.Usage{})
	if err != nil {
		t.Fatal(err)
	}

	// helper func to serialize integrity check
	checkIntegrity := func() (issues, checked, sectors uint64, err error) {
		results, sectors, err := host.Contracts.CheckIntegrity(context.Background(), contract.ID)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("failed to check integrity: %w", err)
		}

		for result := range results {
			if result.Error != nil {
				issues++
			}
			checked++
		}
		return issues, checked, sectors, nil
	}

	// check for issues, should be none
	issues, checked, sectors, err := checkIntegrity()
	if err != nil {
		t.Fatal(err)
	} else if checked != uint64(len(roots)) {
		t.Fatalf("expected %v checked, got %v", len(roots), checked)
	} else if sectors != uint64(len(roots)) {
		t.Fatalf("expected %v sectors, got %v", len(roots), sectors)
	} else if issues != 0 {
		t.Fatalf("expected %v issues, got %v", 0, issues)
	}

	// delete a sector
	if err := host.Volumes.RemoveSector(roots[3]); err != nil {
		t.Fatal(err)
	}

	// check for issues, should be one
	issues, checked, sectors, err = checkIntegrity()
	if err != nil {
		t.Fatal(err)
	} else if checked != uint64(len(roots)) {
		t.Fatalf("expected %v checked, got %v", len(roots), checked)
	} else if sectors != uint64(len(roots)) {
		t.Fatalf("expected %v sectors, got %v", len(roots), sectors)
	} else if issues != 1 {
		t.Fatalf("expected %v issues, got %v", 1, issues)
	}

	// open the data file and corrupt the first sector
	f, err := os.OpenFile(volumePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte{255}, 300); err != nil {
		t.Fatal(err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// check for issues, should be one
	issues, checked, sectors, err = checkIntegrity()
	if err != nil {
		t.Fatal(err)
	} else if checked != uint64(len(roots)) {
		t.Fatalf("expected %v checked, got %v", len(roots), checked)
	} else if sectors != uint64(len(roots)) {
		t.Fatalf("expected %v sectors, got %v", len(roots), sectors)
	} else if issues != 2 {
		t.Fatalf("expected %v issues, got %v", 2, issues)
	}
}
