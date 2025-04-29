package api_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/api"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func formV2Contract(t *testing.T, cm *chain.Manager, c *contracts.Manager, w *wallet.SingleAddressWallet, s *syncer.Syncer, renterKey, hostKey types.PrivateKey, renterFunds, hostFunds types.Currency, duration uint64, broadcast bool) (types.FileContractID, types.V2FileContract) {
	t.Helper()

	cs := cm.TipState()
	fc := types.V2FileContract{
		RevisionNumber:   0,
		Filesize:         0,
		Capacity:         0,
		FileMerkleRoot:   types.Hash256{},
		ProofHeight:      cs.Index.Height + duration,
		ExpirationHeight: cs.Index.Height + duration + 10,
		RenterOutput: types.SiacoinOutput{
			Value:   renterFunds,
			Address: w.Address(),
		},
		HostOutput: types.SiacoinOutput{
			Value:   hostFunds,
			Address: w.Address(),
		},
		MissedHostValue: hostFunds,
		TotalCollateral: hostFunds,
		RenterPublicKey: renterKey.PublicKey(),
		HostPublicKey:   hostKey.PublicKey(),
	}
	fundAmount := cs.V2FileContractTax(fc).Add(hostFunds).Add(renterFunds)
	sigHash := cs.ContractSigHash(fc)
	fc.HostSignature = hostKey.SignHash(sigHash)
	fc.RenterSignature = renterKey.SignHash(sigHash)

	txn := types.V2Transaction{
		FileContracts: []types.V2FileContract{fc},
	}

	basis, toSign, err := w.FundV2Transaction(&txn, fundAmount, false)
	if err != nil {
		t.Fatal("failed to fund transaction:", err)
	}
	w.SignV2Inputs(&txn, toSign)
	formationSet := rhp4.TransactionSet{
		Transactions: []types.V2Transaction{txn},
		Basis:        basis,
	}

	if broadcast {
		if _, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
			t.Fatal("failed to add formation set to pool:", err)
		}
		_ = s.BroadcastV2TransactionSet(formationSet.Basis, formationSet.Transactions) // ignore error: no peers in testing
	}

	if err := c.AddV2Contract(formationSet, proto4.Usage{}); err != nil {
		t.Fatal("failed to add contract:", err)
	}
	return txn.V2FileContractID(txn.ID(), 0), fc
}

func startAPI(t testing.TB, sk types.PrivateKey, host *testutil.HostNode, log *zap.Logger) *api.Client {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	s := &http.Server{
		Handler:     jape.BasicAuth("test")(api.NewServer("test", sk.PublicKey(), host.Chain, host.Syncer, host.Accounts, host.Contracts, host.Volumes, host.Wallet, host.Store, host.Settings, host.Indexer)),
		ReadTimeout: 30 * time.Second,
	}
	t.Cleanup(func() { s.Close() })
	go func() {
		if err := s.Serve(l); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	client := api.NewClient("http://"+l.Addr().String(), "test")
	return client
}

func TestConsensusEndpoints(t *testing.T) {
	log := zap.NewNop()
	n, genesis := testutil.V1Network()
	hostKey := types.GeneratePrivateKey()
	host := testutil.NewHostNode(t, hostKey, n, genesis, log)
	client := startAPI(t, hostKey, host, log)

	testutil.MineAndSync(t, host, types.VoidAddress, 10)
	tip, err := client.ConsensusTip()
	if err != nil {
		t.Fatal(err)
	} else if tip.Height != 10 {
		t.Fatalf("expected tip height 10, got %v", tip.Height)
	}

	cs, err := client.ConsensusTipState()
	if err != nil {
		t.Fatal(err)
	} else if cs.Index != tip {
		t.Fatalf("expected tip %v, got %v", tip, cs.Index)
	}

	it, err := client.IndexTip()
	if err != nil {
		t.Fatal(err)
	} else if it != tip {
		t.Fatalf("expected index tip %v, got %v", tip, it)
	}
}

func TestV2Contracts(t *testing.T) {
	log := zap.NewNop()
	n, genesis := testutil.V2Network()
	hostKey := types.GeneratePrivateKey()
	host := testutil.NewHostNode(t, hostKey, n, genesis, log)
	client := startAPI(t, hostKey, host, log)

	testutil.MineAndSync(t, host, host.Wallet.Address(), 10)

	renterKey := types.GeneratePrivateKey()

	// form a contract
	contractID, revision := formV2Contract(t, host.Chain, host.Contracts, host.Wallet, host.Syncer, renterKey, hostKey, types.Siacoins(500), types.Siacoins(1000), 10, true)

	assertContract := func(t *testing.T, status contracts.V2ContractStatus) {
		t.Helper()

		c, err := client.V2Contract(contractID)
		if err != nil {
			t.Fatal(err)
		} else if c.V2FileContract != revision {
			t.Fatalf("expected contract %v, got %v", revision, c.V2FileContract)
		} else if c.Status != status {
			t.Fatalf("expected status %v, got %v", status, c.Status)
		}
	}

	assertContract(t, contracts.V2ContractStatusPending)
	testutil.MineAndSync(t, host, types.VoidAddress, 1)
	assertContract(t, contracts.V2ContractStatusActive)

	resp, count, err := client.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 contract, got %v", count)
	} else if resp[0].ID != contractID {
		t.Fatalf("expected contract %v, got %v", contractID, resp[0].ID)
	}

	// use a filter that doesn't match any contracts
	resp, count, err = client.V2Contracts(contracts.V2ContractFilter{
		Statuses: []contracts.V2ContractStatus{contracts.V2ContractStatusRejected},
	})
	if err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 contracts, got %v", count)
	} else if len(resp) != 0 {
		t.Fatalf("expected 0 contracts, got %v", len(resp))
	}

	// use a filter that matches the contract
	resp, count, err = client.V2Contracts(contracts.V2ContractFilter{
		Statuses: []contracts.V2ContractStatus{contracts.V2ContractStatusActive},
	})
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 contract, got %v", count)
	} else if resp[0].ID != contractID {
		t.Fatalf("expected contract %v, got %v", contractID, resp[0].ID)
	}
}
