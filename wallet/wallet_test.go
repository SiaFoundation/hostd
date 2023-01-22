package wallet_test

import (
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/internal/persist/sqlite"
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

	cm, err := consensus.NewChainManager(cs)
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

// sendSiacoins helper func to send siacoins from a wallet
func sendSiacoins(w *wallet.SingleAddressWallet, tp modules.TransactionPool, outputs []types.SiacoinOutput) (txn types.Transaction, err error) {
	var siacoinOutput types.Currency
	for _, o := range outputs {
		siacoinOutput = siacoinOutput.Add(o.Value)
	}
	txn.SiacoinOutputs = outputs

	toSign, release, err := w.FundTransaction(&txn, siacoinOutput)
	if err != nil {
		return types.Transaction{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()
	if err := w.SignTransaction(&txn, toSign, types.FullCoveredFields); err != nil {
		return types.Transaction{}, fmt.Errorf("failed to sign transaction: %w", err)
	} else if err := tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		return types.Transaction{}, fmt.Errorf("failed to accept transaction set: %w", err)
	}
	return txn, nil
}

func TestWallet(t *testing.T) {
	node1, err := newTestNode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Close()

	cm, err := consensus.NewChainManager(node1.cs)
	if err != nil {
		t.Fatal(err)
	}

	privKey := ed25519.NewKeyFromSeed(frand.Bytes(ed25519.SeedSize))
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"))
	if err != nil {
		t.Fatal(err)
	}
	w := wallet.NewSingleAddressWallet(privKey, cm, db)

	ccID, err := db.GetLastChange()
	if err != nil {
		t.Fatal(err)
	} else if err := node1.cs.ConsensusSetSubscribe(w, ccID, nil); err != nil {
		t.Fatal(err)
	}
	node1.tp.TransactionPoolSubscribe(w)

	_, balance, err := w.Balance()
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
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// the outputs have not matured yet
	_, balance, err = w.Balance()
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
	_, balance, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	}

	// check that the wallet store only has a single UTXO
	utxos, err := db.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatalf("expected 1 UTXO, got %v", len(utxos))
	}

	// check that the wallet has a single transaction
	count, err := w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transaction, got %v", count)
	}

	// check that the payout transaction was created
	txns, err := db.Transactions(100, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %v", len(txns))
	} else if txns[0].Source != wallet.TxnSourceMinerPayout {
		t.Fatalf("expected miner payout, got %v", txns[0].Source)
	}

	// split the wallet's balance into 20 outputs
	splitOutputs := make([]types.SiacoinOutput, 20)
	for i := range splitOutputs {
		splitOutputs[i] = types.SiacoinOutput{
			Value:      expectedBalance.Div64(20),
			UnlockHash: w.Address(),
		}
	}
	splitTxn, err := sendSiacoins(w, node1.tp, splitOutputs)
	if err != nil {
		t.Fatal(err)
	}

	// mine another block to confirm the transaction
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	// check that the wallet's balance is the same
	_, balance, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	}

	// check that the wallet has 20 UTXOs
	utxos, err = db.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 20 {
		t.Fatalf("expected 20 UTXOs, got %v", len(utxos))
	}

	// check that the wallet has two transactions
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}

	// check that the transaction was created at the top of the transaction list
	txns, err = w.Transactions(100, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 2 {
		t.Fatalf("expected 2 transaction, got %v", len(txns))
	} else if txns[0].Transaction.ID() != splitTxn.ID() {
		t.Fatalf("expected transaction %v, got %v", splitTxn.ID(), txns[0].Transaction.ID())
	} else if txns[0].Source != wallet.TxnSourceTransaction {
		t.Fatalf("expected transaction source, got %v", txns[0].Source)
	}

	// send all the outputs to the burn address individually
	var sentTransactions []types.Transaction
	for i := 0; i < 20; i++ {
		txn, err := sendSiacoins(w, node1.tp, []types.SiacoinOutput{
			{Value: expectedBalance.Div64(20)},
		})
		if err != nil {
			t.Fatal(err)
		}
		sentTransactions = append(sentTransactions, txn)
	}

	// mine another block to confirm the transactions
	if err := miner.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has 22 transactions
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 22 {
		t.Fatalf("expected 22 transactions, got %v", count)
	}

	// check that the paginated transactions are in the proper order
	for i := 0; i < 20; i++ {
		expectedTxn := sentTransactions[i]
		txns, err := w.Transactions(1, i)
		if err != nil {
			t.Fatal(err)
		} else if len(txns) != 1 {
			t.Fatalf("expected 1 transaction, got %v", len(txns))
		} else if txns[0].Transaction.ID() != expectedTxn.ID() {
			t.Fatalf("expected transaction %v, got %v", expectedTxn.ID(), txns[0].Transaction.ID())
		} else if txns[0].Source != wallet.TxnSourceTransaction {
			t.Fatalf("expected transaction source, got %v", txns[0].Source)
		}
	}

	// start a new node to trigger a reorg
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

	// connect the nodes. node1 should begin reverting its blocks
	if err := node1.g.Connect(modules.NetAddress("127.0.0.1:" + node2.g.Address().Port())); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	// check that the wallet's balance is back to 0
	_, balance, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// check that the all utxos have been deleted
	utxos, err = db.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 0 {
		t.Fatalf("expected 0 UTXOs, got %v", len(utxos))
	}

	// check that all transactions have been deleted
	txns, err = w.Transactions(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected 0 transactions, got %v", len(txns))
	}
}
