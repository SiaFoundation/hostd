package wallet_test

import (
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/wallet"
	stypes "go.sia.tech/siad/types"
)

func TestWallet(t *testing.T) {
	w, err := test.NewWallet(types.GeneratePrivateKey(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, balance, _, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// mine a block to fund the wallet
	if err := w.MineBlocks(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// the outputs have not matured yet
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// mine until the first output has matured
	if err := w.MineBlocks(types.Address{}, int(stypes.MaturityDelay)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // sleep for consensus sync

	// check the wallet's reported balance
	expectedBalance := (consensus.State{}).BlockReward()
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %d balance, got %d", expectedBalance, balance)
	}

	// check that the wallet has a single transaction
	count, err := w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transaction, got %v", count)
	}

	// check that the payout transaction was created
	txns, err := w.Transactions(100, 0)
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
			Value:   expectedBalance.Div64(20),
			Address: w.Address(),
		}
	}
	splitTxn, err := w.SendSiacoins(splitOutputs)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond) // sleep for tpool sync
	// check that the wallet's spendable balance and unconfiremed balance are
	// correct
	spendable, balance, unconfirmed, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero spendable balance, got %v", spendable)
	} else if !unconfirmed.Equals(expectedBalance) {
		t.Fatalf("expected %v unconfirmed balance, got %v", expectedBalance, unconfirmed)
	}

	// mine another block to confirm the transaction
	if err := w.MineBlocks(types.VoidAddress, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	// check that the wallet's balance is the same
	_, balance, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero unconfirmed balance, got %v", unconfirmed)
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
	} else if txns[0].Outflow.Equals(types.ZeroCurrency) {
		t.Fatalf("expected outflow, got %v", txns[0].Outflow)
	}

	// send all the outputs to the burn address individually
	var sentTransactions []types.Transaction
	for i := 0; i < 20; i++ {
		txn, err := w.SendSiacoins([]types.SiacoinOutput{
			{Value: expectedBalance.Div64(20)},
		})
		if err != nil {
			t.Fatal(err)
		}
		sentTransactions = append(sentTransactions, txn)
	}

	time.Sleep(250 * time.Millisecond) // sleep for tpool sync
	// check that the wallet's spendable balance and unconfiremed balance are
	// correct
	spendable, balance, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %v balance, got %v", expectedBalance, balance)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero spendable balance, got %v", spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero unconfirmed balance, got %v", unconfirmed)
	}

	// mine another block to confirm the transactions
	if err := w.MineBlocks(types.VoidAddress, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

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
	w2, err := test.NewWallet(types.GeneratePrivateKey(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// mine enough blocks on the second node to trigger a reorg
	if err := w2.MineBlocks(types.Address{}, int(stypes.MaturityDelay)*2); err != nil {
		t.Fatal(err)
	}

	// connect the nodes. node1 should begin reverting its blocks
	if err := w.ConnectPeer(w2.GatewayAddr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	// check that the wallet's balance is back to 0
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// check that all transactions have been deleted
	txns, err = w.Transactions(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected 0 transactions, got %v", len(txns))
	}
}
