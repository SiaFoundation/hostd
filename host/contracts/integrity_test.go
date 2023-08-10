package contracts_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/test"
	stypes "go.sia.tech/siad/types"
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
	hostKey, renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32)), types.NewPrivateKeyFromSeed(frand.Bytes(32))

	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	node, err := test.NewWallet(hostKey, dir, log)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	am := alerts.NewManager()
	s, err := storage.NewVolumeManager(node.Store(), am, node.ChainManager(), log.Named("storage"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	result := make(chan error, 1)
	if _, err := s.AddVolume(filepath.Join(dir, "data.dat"), 10, result); err != nil {
		t.Fatal(err)
	} else if err := <-result; err != nil {
		t.Fatal(err)
	}

	c, err := contracts.NewManager(node.Store(), am, s, node.ChainManager(), node.TPool(), node, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// note: many more blocks than necessary are mined to ensure all forks have activated
	if err := node.MineBlocks(node.Address(), int(stypes.MaturityDelay*4)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time

	rev, err := formContract(renterKey, hostKey, 50, 60, types.Siacoins(500), types.Siacoins(1000), c, node, node.ChainManager(), node.TPool())
	if err != nil {
		t.Fatal(err)
	}

	contract, err := c.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.ContractStatusPending {
		t.Fatal("expected contract to be pending")
	}

	if err := node.MineBlocks(types.VoidAddress, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time

	contract, err = c.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if contract.Status != contracts.ContractStatusActive {
		t.Fatal("expected contract to be active")
	}

	updater, err := c.UpdateContract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	}
	defer updater.Close()

	var roots []types.Hash256
	var releases []func() error
	for i := 0; i < 5; i++ {
		var sector [rhpv2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhpv2.SectorRoot(&sector)
		release, err := s.Write(root, &sector)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
		roots = append(roots, root)
		updater.AppendSector(root)
	}

	contract.Revision.RevisionNumber++
	contract.Revision.Filesize = uint64(len(roots)) * rhpv2.SectorSize
	contract.Revision.FileMerkleRoot = rhpv2.MetaRoot(roots)

	if err := updater.Commit(contract.SignedRevision, contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	for _, release := range releases {
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}

	// helper func to serialize integrity check
	checkIntegrity := func() (issues, checked, sectors uint64, err error) {
		results, sectors, err := c.CheckIntegrity(context.Background(), rev.Revision.ParentID)
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
	if err := s.RemoveSector(roots[3]); err != nil {
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
	f, err := os.OpenFile(filepath.Join(dir, "data.dat"), os.O_RDWR, 0)
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
