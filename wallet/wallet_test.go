package wallet_test

import (
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/internal/persist/sql"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

type testNode struct {
	g  modules.Gateway
	cs modules.ConsensusSet
	tp modules.TransactionPool
	cm wallet.ChainManager
}

func (tn *testNode) Close() error {
	tn.tp.Close()
	tn.cs.Close()
	tn.g.Close()
	return nil
}

func newTestNode(dir string) (*testNode, error) {
	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}

	cs, errCh := mconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}

	tp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}

	chainStore := store.NewEphemeralChainManagerStore()
	cm, err := consensus.NewChainManager(cs, chainStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create chain manager: %w", err)
	}

	return &testNode{
		g:  g,
		cs: cs,
		tp: tp,
		cm: cm,
	}, nil
}

func TestWallet(t *testing.T) {
	node1, err := newTestNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Close()

	consensusStore := store.NewEphemeralChainManagerStore()
	cm, err := consensus.NewChainManager(node1.cs, consensusStore)
	if err != nil {
		t.Fatal(err)
	}

	privKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
	sqlStore, err := sql.NewSQLiteStore(filepath.Join(t.TempDir(), "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	walletStore := sql.NewWalletStore(sqlStore)
	wallet := wallet.NewSingleAddressWallet(privKey, cm, walletStore)

	ccID, err := walletStore.GetLastChange()
	if err != nil {
		t.Fatal(err)
	}

	if err := node1.cs.ConsensusSetSubscribe(wallet, ccID, nil); err != nil {
		t.Fatal(err)
	}

	_, balance, err := wallet.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	miner := test.NewMiner(node1.cs)
	if err := node1.cs.ConsensusSetSubscribe(miner, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	node1.tp.TransactionPoolSubscribe(miner)

	// mine a block to fund the wallet
	if err := miner.Mine(wallet.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// the outputs have not matured yet
	_, balance, err = wallet.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// mine until the first output has matured
	if err := miner.Mine(types.UnlockHash{}, int(types.MaturityDelay)); err != nil {
		t.Fatal(err)
	}

	// check the wallet's reported balance
	expectedBalance := types.CalculateCoinbase(1)
	_, balance, err = wallet.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	}

	// check that the wallet store only has a single UTXO
	utxos, err := walletStore.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatalf("expected 1 UTXO, got %v", len(utxos))
	}

	// check that the payout transaction was created
	txns, err := walletStore.Transactions(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %v", len(txns))
	}

	// send half of the wallet's balance to the zero address
	txn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: expectedBalance.Div64(2)},
		},
	}
	toSign, release, err := wallet.FundTransaction(&txn, expectedBalance.Div64(2), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := wallet.SignTransaction(&txn, toSign, types.FullCoveredFields); err != nil {
		t.Fatal(err)
	}

	if err := node1.tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// mine another block to confirm the transaction
	if err := miner.Mine(wallet.Address(), 1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// check that the wallet's balance has been reduced
	expectedBalance = expectedBalance.Div64(2)
	_, balance, err = wallet.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	}

	// check that the wallet has a single transaction
	txns, err = wallet.Transactions(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 2 {
		t.Fatalf("expected 2 transaction, got %v", len(txns))
	} else if txns[0].Transaction.ID() != txn.ID() {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), txns[0].Transaction.ID())
	}

	node2, err := newTestNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Close()

	miner2 := test.NewMiner(node2.cs)
	if err := node2.cs.ConsensusSetSubscribe(miner2, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal(err)
	}
	node2.tp.TransactionPoolSubscribe(miner2)

	// mine enough blocks on the second node to trigger a reorg
	if err := miner2.Mine(types.UnlockHash{}, int(types.MaturityDelay)*2); err != nil {
		t.Fatal(err)
	}

	// connect the nodes to trigger a reorg
	if err := node1.g.Connect(modules.NetAddress("127.0.0.1:" + node2.g.Address().Port())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	// check that the wallet's balance is back to 0
	_, balance, err = wallet.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// check that the all utxos have been deleted
	utxos, err = walletStore.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatalf("expected 0 UTXOs, got %v", len(utxos))
	}

	// check that all transactions have been deleted
	txns, err = wallet.Transactions(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected 0 transactions, got %v", len(txns))
	}
}
