package wallet_test

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
	"go.sia.tech/hostd/wallet"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap/zaptest"
)

func TestWallet(t *testing.T) {
	log := zaptest.NewLogger(t)
	w, err := test.NewWallet(types.GeneratePrivateKey(), t.TempDir(), log.Named("wallet"))
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

	initialState := w.TipState()

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
	} else if m, err := w.Store().Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %d", m.Balance)
	}

	// mine until the first output has matured
	if err := w.MineBlocks(types.VoidAddress, int(stypes.MaturityDelay)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // sleep for consensus sync

	// check the wallet's reported balance
	expectedBalance := initialState.BlockReward()
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected %d balance, got %d", expectedBalance, balance)
	} else if m, err := w.Store().Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Balance.Equals(expectedBalance) {
		t.Fatalf("expected %d balance, got %d", expectedBalance, m.Balance)
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
	if txn, err := w.SendSiacoins(splitOutputs); err != nil {
		buf, _ := json.MarshalIndent(txn, "", "  ")
		t.Log(string(buf))
		t.Fatal(err)
	}

	// check that the wallet's spendable balance and unconfirmed balance are
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
	} else if m, err := w.Store().Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Balance.Equals(expectedBalance) {
		t.Fatalf("expected %d balance, got %d", expectedBalance, m.Balance)
	}

	// check that the wallet only has one transaction. The split transaction
	// does not count since inflow = outflow
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transactions, got %v", count)
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
	// check that the wallet's spendable balance and unconfirmed balance are
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

	// check that the wallet now has 21 transactions, 1 + 20 void transactions
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 21 {
		t.Fatalf("expected 21 transactions, got %v", count)
	} else if m, err := w.Store().Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %d balance, got %d", types.ZeroCurrency, m.Balance)
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
	w2, err := test.NewWallet(types.GeneratePrivateKey(), t.TempDir(), log.Named("wallet2"))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// mine enough blocks on the second node to trigger a reorg
	if err := w2.MineBlocks(types.Address{}, int(stypes.MaturityDelay)*4); err != nil {
		t.Fatal(err)
	}

	// connect the nodes. node1 should begin reverting its blocks
	if err := w.ConnectPeer(w2.GatewayAddr()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if w.TipState().Index.ID == w2.TipState().Index.ID {
			break
		}
		time.Sleep(time.Second)
	}
	if w.TipState().Index.ID != w2.TipState().Index.ID {
		t.Fatal("nodes are not synced")
	}

	// check that the wallet's balance is back to 0
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero balance, got %v", balance)
	} else if m, err := w.Store().Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Balance.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %d balance, got %d", types.ZeroCurrency, m.Balance)
	}

	// check that all transactions have been deleted
	txns, err = w.Transactions(100, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected 0 transactions, got %v", len(txns))
	}
}

func TestWalletReset(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	w, err := test.NewWallet(types.GeneratePrivateKey(), dir, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, balance, _, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.IsZero() {
		t.Fatalf("expected zero balance, got %v", balance)
	}

	// mine until the wallet has funds
	if err := w.MineBlocks(w.Address(), int(stypes.MaturityDelay)*2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // sleep for sync

	height := w.ScanHeight()

	// check that the wallet has UTXOs and transactions
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if balance.IsZero() {
		t.Fatal("expected non-zero balance")
	} else if txns, err := w.Transactions(100, 0); err != nil {
		t.Fatal(err)
	} else if len(txns) == 0 {
		t.Fatal("expected transactions")
	}

	m, err := w.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if m.Balance.IsZero() {
		t.Fatal("expected non-zero balance")
	}

	// close the wallet and trigger a reset by using a different private key
	w.Close()

	w, err = test.NewWallet(types.GeneratePrivateKey(), dir, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// wait for the wallet to resync
	for i := 0; i < 100; i++ {
		if current := w.ScanHeight(); current == height {
			break
		}
		time.Sleep(time.Second) // sleep for sync
	}
	if current := w.ScanHeight(); current != height {
		t.Fatalf("expected scan height %v, got %v", height, current)
	}

	// check that the wallet has no UTXOs or transactions
	_, balance, _, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.IsZero() {
		t.Fatalf("expected zero balance, got %v", balance)
	} else if txns, err := w.Transactions(100, 0); err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatal("expected no transactions")
	}

	m, err = w.Store().Metrics(time.Now())
	if err != nil {
		t.Fatal(err)
	} else if !m.Balance.IsZero() {
		t.Fatal("expected zero balance")
	}
}

func TestWalletUTXOSelection(t *testing.T) {
	log := zaptest.NewLogger(t)
	w, err := test.NewWallet(types.GeneratePrivateKey(), t.TempDir(), log.Named("wallet"))
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

	// mine until the wallet has 100 mature outputs
	if err := w.MineBlocks(w.Address(), 100+int(stypes.MaturityDelay)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second) // sleep for consensus sync

	// check that the expected utxos were used
	utxos, err := w.Store().UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 100 {
		t.Fatalf("expected 100 utxos, got %v", len(utxos))
	}
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// send a transaction to the burn address
	sendAmount := types.Siacoins(10)
	minerFee := types.Siacoins(1)
	txn := types.Transaction{
		MinerFees: []types.Currency{minerFee},
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: sendAmount},
		},
	}

	fundAmount := sendAmount.Add(minerFee)
	toSign, release, err := w.FundTransaction(&txn, fundAmount)
	if err != nil {
		t.Fatal(err)
	}

	if len(txn.SiacoinInputs) != 11 {
		t.Fatalf("expected 10 additional defrag inputs, got %v", len(toSign)-1)
	} else if len(txn.SiacoinOutputs) != 2 {
		t.Fatalf("expected a change output, got %v", len(txn.SiacoinOutputs))
	}

	// check that the expected UTXOs were added
	spent := []wallet.SiacoinElement{utxos[0]}
	rem := utxos[90:]
	for i := len(rem) - 1; i >= 0; i-- {
		spent = append(spent, rem[i])
	}

	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].ParentID != spent[i].ID {
			t.Fatalf("expected input %v to spend %v, got %v", i, spent[i].ID, txn.SiacoinInputs[i].ParentID)
		}
	}

	if err := w.SignTransaction(w.TipState(), &txn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		release()
		t.Fatal(err)
	} else if err := w.TPool().AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		release()
		t.Fatal(err)
	}
}
