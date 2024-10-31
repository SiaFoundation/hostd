package rhp_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	crhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/internal/testutil"
	rpc2 "go.sia.tech/hostd/internal/testutil/rhp/v2"
	"go.sia.tech/hostd/rhp"
	rhp2 "go.sia.tech/hostd/rhp/v2"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func dialHost(t *testing.T, hostKey types.PublicKey, netaddr string) *crhp2.Transport {
	t.Helper()

	conn, err := net.Dial("tcp", netaddr)
	if err != nil {
		t.Fatal("failed to dial host", err)
	}
	t.Cleanup(func() { conn.Close() })
	transport, err := crhp2.NewRenterTransport(conn, hostKey)
	if err != nil {
		t.Fatal("failed to create transport", err)
	}
	return transport
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey := types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	s := node.Settings.Settings()
	s.NetAddress = "localhost:9983"
	if err := node.Settings.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	sh, err := rhp2.NewSessionHandler(l, hostKey, "localhost:9983", node.Chain, node.Syncer, node.Wallet, node.Contracts, node.Settings, node.Volumes, rhp2.WithLog(log))
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	go sh.Serve()

	transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
	defer transport.Close()

	settings, err := rpc2.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	expected, err := sh.Settings()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(settings, expected) {
		t.Fatal("settings mismatch")
	}
}

func TestUploadDownload(t *testing.T) {
	log := zaptest.NewLogger(t)
	renterKey, hostKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// set the host to accept contracts
	s := node.Settings.Settings()
	s.AcceptingContracts = true
	s.NetAddress = "localhost:9983"
	if err := node.Settings.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}

	// initialize a storage volume
	res := make(chan error)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "storage.dat"), 10, res); err != nil {
		t.Fatal(err)
	} else if err := <-res; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	sh, err := rhp2.NewSessionHandler(l, hostKey, "localhost:9983", node.Chain, node.Syncer, node.Wallet, node.Contracts, node.Settings, node.Volumes, rhp2.WithLog(log))
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	go sh.Serve()

	transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
	defer transport.Close()

	settings, err := rpc2.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fc := crhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(10), types.Siacoins(20), node.Chain.Tip().Height+200, settings, node.Wallet.Address())
	formationCost := crhp2.ContractFormationCost(node.Chain.TipState(), fc, settings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	toSign, err := node.Wallet.FundTransaction(&txn, formationCost, true)
	if err != nil {
		t.Fatal(err)
	}
	node.Wallet.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))
	formationSet := append(node.Chain.UnconfirmedParents(txn), txn)

	revision, _, err := rpc2.RPCFormContract(transport, renterKey, formationSet)
	if err != nil {
		t.Fatal(err)
	} else if _, err := rpc2.RPCLock(transport, renterKey, revision.ID()); err != nil {
		t.Fatal(err)
	}
	defer rpc2.RPCUnlock(transport)

	var sector [crhp2.SectorSize]byte
	frand.Read(sector[:256])
	appendAction := []crhp2.RPCWriteAction{
		{
			Type: crhp2.RPCWriteActionAppend,
			Data: sector[:],
		},
	}
	root := crhp2.SectorRoot(&sector)

	err = rpc2.RPCWrite(transport, renterKey, &revision, appendAction, types.Siacoins(1), types.ZeroCurrency) // just overpay
	if err != nil {
		t.Fatal(err)
	} else if revision.Revision.FileMerkleRoot != root {
		t.Fatal("root mismatch")
	}

	roots, err := rpc2.RPCSectorRoots(transport, renterKey, 0, 1, &revision, types.Siacoins(1)) // just overpay
	if err != nil {
		t.Fatal(err)
	} else if len(roots) != 1 || roots[0] != root {
		t.Fatal("root mismatch")
	}

	readSection := []crhp2.RPCReadRequestSection{
		{
			MerkleRoot: root,
			Offset:     0,
			Length:     crhp2.SectorSize,
		},
	}
	buf := bytes.NewBuffer(make([]byte, 0, crhp2.SectorSize))
	err = rpc2.RPCRead(transport, buf, renterKey, &revision, readSection, types.Siacoins(1)) // just overpay
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("sector mismatch")
	}
}

func TestRenew(t *testing.T) {
	log := zaptest.NewLogger(t)
	renterKey, hostKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// set the host to accept contracts
	s := node.Settings.Settings()
	s.AcceptingContracts = true
	s.NetAddress = "localhost:9983"
	if err := node.Settings.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}

	// initialize a storage volume
	res := make(chan error)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "storage.dat"), 10, res); err != nil {
		t.Fatal(err)
	} else if err := <-res; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	sh, err := rhp2.NewSessionHandler(l, hostKey, "localhost:9983", node.Chain, node.Syncer, node.Wallet, node.Contracts, node.Settings, node.Volumes, rhp2.WithLog(log))
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	go sh.Serve()

	transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
	defer transport.ForceClose()

	settings, err := rpc2.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	formContract := func(t *testing.T, duration uint64) crhp2.ContractRevision {
		t.Helper()

		fc := crhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(10), types.Siacoins(20), node.Chain.Tip().Height+duration, settings, node.Wallet.Address())
		formationCost := crhp2.ContractFormationCost(node.Chain.TipState(), fc, settings.ContractPrice)
		txn := types.Transaction{
			FileContracts: []types.FileContract{fc},
		}
		toSign, err := node.Wallet.FundTransaction(&txn, formationCost, true)
		if err != nil {
			t.Fatal("failed to fund formation txn:", err)
		}
		node.Wallet.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))
		formationSet := append(node.Chain.UnconfirmedParents(txn), txn)

		revision, _, err := rpc2.RPCFormContract(transport, renterKey, formationSet)
		if err != nil {
			t.Fatal("failed to form contract:", err)
		}
		return revision
	}

	renewContract := func(t *testing.T, revision crhp2.ContractRevision, windowEnd uint64) crhp2.ContractRevision {
		current := revision.Revision

		additionalCollateral := crhp2.ContractRenewalCollateral(current.FileContract, 1<<22, settings, node.Chain.Tip().Height, windowEnd)
		renewed, basePrice := crhp2.PrepareContractRenewal(current, node.Wallet.Address(), types.Siacoins(10), additionalCollateral, settings, windowEnd)
		renewalTxn := types.Transaction{
			FileContracts: []types.FileContract{renewed},
		}

		cost := crhp2.ContractRenewalCost(node.Chain.TipState(), renewed, settings.ContractPrice, types.ZeroCurrency, basePrice)
		toSign, err := node.Wallet.FundTransaction(&renewalTxn, cost, true)
		if err != nil {
			t.Fatal("failed to fund formation txn:", err)
		}
		node.Wallet.SignTransaction(&renewalTxn, toSign, wallet.ExplicitCoveredFields(renewalTxn))
		renewalSet := append(node.Chain.UnconfirmedParents(renewalTxn), renewalTxn)

		renewal, _, err := rpc2.RPCRenewContract(transport, renterKey, &revision, renewalSet, settings.BaseRPCPrice)
		if err != nil {
			t.Fatal("failed to renew contract:", err)
		}
		return renewal
	}

	assertContract := func(t *testing.T, rev crhp2.ContractRevision, expectedRevisionNumber uint64, expectedRoot types.Hash256, expectedFilesize uint64) {
		t.Helper()

		if rev.Revision.FileMerkleRoot != expectedRoot {
			t.Fatalf("expected root %v, got %v", expectedRoot, rev.Revision.FileMerkleRoot)
		} else if rev.Revision.Filesize != expectedFilesize {
			t.Fatalf("expected filesize %d, got %d", expectedFilesize, rev.Revision.Filesize)
		} else if rev.Revision.RevisionNumber != expectedRevisionNumber {
			t.Fatalf("expected revision number %d, got %d", expectedRevisionNumber, rev.Revision.RevisionNumber)
		}

		sigHash := rhp.HashRevision(rev.Revision)
		if !hostKey.PublicKey().VerifyHash(sigHash, types.Signature(rev.Signatures[1].Signature)) {
			t.Fatal("host signature invalid")
		} else if !renterKey.PublicKey().VerifyHash(sigHash, types.Signature(rev.Signatures[0].Signature)) {
			t.Fatal("renter signature invalid")
		}
	}

	t.Run("empty contract", func(t *testing.T) {
		fc := formContract(t, 145)
		// mine to confirm the contract
		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)
		if _, err := rpc2.RPCLock(transport, renterKey, fc.ID()); err != nil {
			t.Fatal(err)
		}
		defer rpc2.RPCUnlock(transport)

		assertContract(t, fc, 1, types.Hash256{}, 0)
		renewal := renewContract(t, fc, fc.Revision.WindowEnd+10)
		assertContract(t, renewal, 1, types.Hash256{}, 0)
	})

	// note: rhp2 contracts could not be renewed if they did not have any funds.
	t.Run("refresh contract", func(t *testing.T) {
		fc := formContract(t, 145)
		// mine to confirm the contract
		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)
		if _, err := rpc2.RPCLock(transport, renterKey, fc.ID()); err != nil {
			t.Fatal(err)
		}
		defer rpc2.RPCUnlock(transport)

		// upload a sector and pay the entire contract value
		// minus the cost of renewal
		var sector [crhp2.SectorSize]byte
		frand.Read(sector[:256])
		appendAction := []crhp2.RPCWriteAction{
			{
				Type: crhp2.RPCWriteActionAppend,
				Data: sector[:],
			},
		}
		root := crhp2.SectorRoot(&sector)

		err = rpc2.RPCWrite(transport, renterKey, &fc, appendAction, fc.RenterFunds().Sub(settings.BaseRPCPrice), types.ZeroCurrency)
		if err != nil {
			t.Fatal(err)
		}
		assertContract(t, fc, 2, root, crhp2.SectorSize)

		renewal := renewContract(t, fc, fc.Revision.WindowEnd)
		assertContract(t, renewal, 1, root, crhp2.SectorSize)
	})

	t.Run("renew contract", func(t *testing.T) {
		fc := formContract(t, 145)
		// mine to confirm the contract
		testutil.MineAndSync(t, node, node.Wallet.Address(), 5)
		if _, err := rpc2.RPCLock(transport, renterKey, fc.ID()); err != nil {
			t.Fatal(err)
		}
		defer rpc2.RPCUnlock(transport)

		// upload a sector and pay the entire contract value
		// minus the cost of renewal
		var roots []types.Hash256
		var sector [crhp2.SectorSize]byte
		frand.Read(sector[:256])
		appendAction := []crhp2.RPCWriteAction{
			{
				Type: crhp2.RPCWriteActionAppend,
				Data: sector[:],
			},
		}
		root := crhp2.SectorRoot(&sector)
		roots = append(roots, root)

		err = rpc2.RPCWrite(transport, renterKey, &fc, appendAction, fc.RenterFunds().Sub(settings.BaseRPCPrice), types.ZeroCurrency)
		if err != nil {
			t.Fatal(err)
		}
		assertContract(t, fc, 2, crhp2.MetaRoot(roots), uint64(len(roots))*crhp2.SectorSize)

		renewal := renewContract(t, fc, fc.Revision.WindowEnd+10)
		assertContract(t, renewal, 1, crhp2.MetaRoot(roots), uint64(len(roots))*crhp2.SectorSize)

		if err := rpc2.RPCUnlock(transport); err != nil {
			t.Fatal(err)
		} else if _, err := rpc2.RPCLock(transport, renterKey, renewal.ID()); err != nil {
			t.Fatal(err)
		}

		// upload a new sector
		frand.Read(sector[:256])
		appendAction = []crhp2.RPCWriteAction{
			{
				Type: crhp2.RPCWriteActionAppend,
				Data: sector[:],
			},
		}
		root = crhp2.SectorRoot(&sector)
		roots = append(roots, root)

		err = rpc2.RPCWrite(transport, renterKey, &renewal, appendAction, types.Siacoins(1), types.ZeroCurrency)
		if err != nil {
			t.Fatal(err)
		}
		assertContract(t, renewal, 2, crhp2.MetaRoot(roots), uint64(len(roots))*crhp2.SectorSize)
	})
}

func TestRPCV2(t *testing.T) {
	log := zaptest.NewLogger(t)
	renterKey, hostKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	network, genesis := testutil.V2Network()
	network.HardforkV2.AllowHeight = 180
	network.HardforkV2.RequireHeight = 200
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	// initialize a storage volume
	res := make(chan error)
	if _, err := node.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "storage.dat"), 10, res); err != nil {
		t.Fatal(err)
	} else if err := <-res; err != nil {
		t.Fatal(err)
	}

	// fund the wallet
	testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.MaturityDelay+5))

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	sh, err := rhp2.NewSessionHandler(l, hostKey, "localhost:9983", node.Chain, node.Syncer, node.Wallet, node.Contracts, node.Settings, node.Volumes, rhp2.WithLog(log))
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	go sh.Serve()

	t.Run("ends after require height", func(t *testing.T) {
		transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
		defer transport.Close()

		settings, err := rpc2.RPCSettings(transport)
		if err != nil {
			t.Fatal(err)
		}

		// try to form a v1 contract that ends after the require height
		fc := crhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(10), types.Siacoins(20), network.HardforkV2.RequireHeight, settings, node.Wallet.Address())
		formationCost := crhp2.ContractFormationCost(node.Chain.TipState(), fc, settings.ContractPrice)
		txn := types.Transaction{
			FileContracts: []types.FileContract{fc},
		}
		toSign, err := node.Wallet.FundTransaction(&txn, formationCost, true)
		if err != nil {
			t.Fatal(err)
		}
		node.Wallet.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))
		formationSet := append(node.Chain.UnconfirmedParents(txn), txn)

		if _, _, err := rpc2.RPCFormContract(transport, renterKey, formationSet); !errors.Is(err, rhp2.ErrAfterV2Hardfork) {
			t.Fatalf("expected ErrV2Hardfork, got %v", err)
		}
	})

	t.Run("form after allow height", func(t *testing.T) {
		// mine until the allow height
		testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.HardforkV2.AllowHeight-node.Chain.Tip().Height))

		transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
		defer transport.Close()

		settings, err := rpc2.RPCSettings(transport)
		if err != nil {
			t.Fatal(err)
		}

		// try to form a v1 contract after the allow height
		fc := crhp2.PrepareContractFormation(renterKey.PublicKey(), hostKey.PublicKey(), types.Siacoins(10), types.Siacoins(20), node.Chain.Tip().Height+10, settings, node.Wallet.Address())
		formationCost := crhp2.ContractFormationCost(node.Chain.TipState(), fc, settings.ContractPrice)
		txn := types.Transaction{
			FileContracts: []types.FileContract{fc},
		}
		toSign, err := node.Wallet.FundTransaction(&txn, formationCost, true)
		if err != nil {
			t.Fatal(err)
		}
		node.Wallet.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))
		formationSet := append(node.Chain.UnconfirmedParents(txn), txn)

		_, _, err = rpc2.RPCFormContract(transport, renterKey, formationSet)
		if runtime.GOOS != "windows" && !errors.Is(err, rhp2.ErrV2Hardfork) { // windows responds with wsarecv rather than the error
			t.Fatalf("expected ErrV2Hardfork, got %v", err)
		} else if runtime.GOOS == "windows" && err == nil {
			t.Fatal("expected windows error, got nil")
		}
	})

	t.Run("rpc after require height", func(t *testing.T) {
		// mine until the require height
		testutil.MineAndSync(t, node, node.Wallet.Address(), int(network.HardforkV2.RequireHeight-node.Chain.Tip().Height))

		transport := dialHost(t, hostKey.PublicKey(), l.Addr().String())
		defer transport.Close()

		_, err := rpc2.RPCSettings(transport)
		if runtime.GOOS != "windows" && !errors.Is(err, rhp2.ErrV2Hardfork) { // windows responds with wsarecv rather than the error
			t.Fatalf("expected ErrV2Hardfork, got %v", err)
		} else if runtime.GOOS == "windows" && err == nil {
			t.Fatal("expected windows error, got nil")
		}
	})
}
