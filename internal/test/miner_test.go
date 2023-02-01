//go:build ignore

package test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// A siacoinElement groups a SiacoinOutput and its ID together
type siacoinElement struct {
	types.SiacoinOutput
	ID types.SiacoinOutputID
}

// A testWallet provides very basic wallet functionality for testing the miner
type testWallet struct {
	priv types.PrivateKey

	mu        sync.Mutex
	height    uint64
	spent     map[types.SiacoinOutputID]bool
	spendable map[types.SiacoinOutputID]siacoinElement
}

// UnlockConditions is a helper to return the standard unlock conditions using
// the wallet's private key
func (tw *testWallet) UnlockConditions() types.UnlockConditions {
	return wallet.StandardUnlockConditions(tw.priv.PublicKey())
}

// Address returns the address of the wallet
func (tw *testWallet) Address() types.Address {
	return tw.UnlockConditions().UnlockHash()
}

// Balance returns the balance of the wallet
func (tw *testWallet) Balance() types.Currency {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	var balance types.Currency
	for _, sco := range tw.spendable {
		if tw.spent[sco.ID] {
			continue
		}
		balance = balance.Add(sco.Value)
	}
	return balance
}

// FundTransaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be used again until release is called.
func (tw *testWallet) FundAndSignTransaction(txn *types.Transaction, amount types.Currency) (func(), error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if amount.IsZero() {
		return func() {}, nil
	}

	var added types.Currency
	var spent []siacoinElement
	for _, sco := range tw.spendable {
		if tw.spent[sco.ID] {
			continue
		}

		spent = append(spent, sco)
		added = added.Add(sco.Value)
		if added.Cmp(amount) >= 0 {
			break
		}
	}
	// check if the sum of the inputs is greater than the fund amount
	if added.Cmp(amount) < 0 {
		return nil, fmt.Errorf("not enough funds")
	} else if added.Cmp(amount) > 0 {
		// add a change output
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   added.Sub(amount),
			Address: tw.Address(),
		})
	}

	n := len(txn.Signatures)

	// add the spent outputs and signatures to the transaction
	for _, sce := range spent {
		tw.spent[sce.ID] = true
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         sce.ID,
			UnlockConditions: tw.UnlockConditions(),
		})
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:      types.Hash256(sce.ID),
			CoveredFields: types.CoveredFields{WholeTransaction: true},
		})
	}

	// sign all added signatures
	for i := n; i < len(txn.Signatures); i++ {
		cs := consensus.State{Index: types.ChainIndex{Height: tw.height}}
		sig := tw.priv.SignHash(cs.WholeSigHash(*txn, txn.Signatures[i].ParentID, 0, 0, nil))
		txn.Signatures[i].Signature = sig[:]
	}

	return func() {
		tw.mu.Lock()
		defer tw.mu.Unlock()
		for _, sce := range spent {
			delete(tw.spent, sce.ID)
		}
	}, nil
}

// ProcessConsensusChange processes a consensus change - adding new outputs to
// the wallet and removing spent outputs
func (tw *testWallet) ProcessConsensusChange(cc modules.ConsensusChange) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for _, scod := range cc.SiacoinOutputDiffs {
		if scod.Direction == modules.DiffApply && types.Address(scod.SiacoinOutput.UnlockHash) == tw.Address() {
			var sco types.SiacoinOutput
			convertToCore(scod.SiacoinOutput, &sco)
			tw.spendable[types.SiacoinOutputID(scod.ID)] = siacoinElement{
				ID:            types.SiacoinOutputID(scod.ID),
				SiacoinOutput: sco,
			}
		} else {
			delete(tw.spendable, types.SiacoinOutputID(scod.ID))
		}
	}

	tw.height = uint64(cc.BlockHeight)
}

// newTestWallet returns a new test wallet with a random private key.
func newTestWallet() *testWallet {
	return &testWallet{
		priv:      types.GeneratePrivateKey(),
		spent:     make(map[types.SiacoinOutputID]bool),
		spendable: make(map[types.SiacoinOutputID]siacoinElement),
	}
}

// TestMining tests that cpu mining works as expected and adds spendable outputs
// to the wallet.
func TestMining(t *testing.T) {
	dir := t.TempDir()

	g, err := gateway.New("localhost:0", false, filepath.Join(dir, modules.GatewayDir))
	if err != nil {
		t.Fatal("could not create gateway:", err)
	}
	t.Cleanup(func() { g.Close() })

	cs, errChan := mconsensus.New(g, false, filepath.Join(dir, modules.ConsensusDir))
	if err := <-errChan; err != nil {
		t.Fatal("could not create consensus set:", err)
	}
	go func() {
		for err := range errChan {
			panic(fmt.Errorf("consensus err: %w", err))
		}
	}()
	t.Cleanup(func() { cs.Close() })

	tp, err := transactionpool.New(cs, g, filepath.Join(dir, modules.TransactionPoolDir))
	if err != nil {
		t.Fatal("could not create tpool:", err)
	}
	t.Cleanup(func() { tp.Close() })

	w := newTestWallet()
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal("failed to subscribe to consensus set:", err)
	}

	m := NewMiner(cs)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		t.Fatal("failed to subscribe to consensus set:", err)
	}
	tp.TransactionPoolSubscribe(m)

	// mine a single block
	if err := m.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// make sure the block height is updated
	if height := cs.Height(); height != 1 {
		t.Fatalf("expected height 1, got %v", height)
	}

	// mine until the maturity height of the first payout is reached
	if err := m.Mine(w.Address(), int(stypes.MaturityDelay)); err != nil {
		t.Fatal(err)
	} else if height := cs.Height(); height != stypes.MaturityDelay+1 {
		t.Fatalf("expected height %v, got %v", stypes.MaturityDelay+1, height)
	}

	// make sure we have the expected balance
	siadExpectedBalance := stypes.CalculateCoinbase(1)
	var expectedBalance types.Currency
	convertToCore(siadExpectedBalance, &expectedBalance)
	if balance := w.Balance(); !balance.Equals(expectedBalance) {
		t.Fatalf("expected balance to be %v, got %v", expectedBalance, balance)
	}

	// mine more blocks until we have lots of outputs
	if err := m.Mine(w.Address(), 100); err != nil {
		t.Fatal(err)
	}

	// add random transactions to the tpool
	added := make([]types.TransactionID, 100)
	for i := range added {
		amount := types.Siacoins(uint32(1 + frand.Intn(1000)))
		txn := types.Transaction{
			ArbitraryData: [][]byte{append(modules.PrefixNonSia[:], frand.Bytes(16)...)},
			SiacoinOutputs: []types.SiacoinOutput{
				{Value: amount},
			},
		}

		release, err := w.FundAndSignTransaction(&txn, amount)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		if err := tp.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
			buf, _ := json.MarshalIndent(txn, "", "  ")
			t.Log(string(buf))
			t.Fatalf("failed to accept transaction %v: %v", i, err)
		}

		added[i] = txn.ID()
	}

	// mine a block to confirm the transactions
	if err := m.Mine(w.Address(), 1); err != nil {
		t.Fatal(err)
	}

	// check that the correct number of transactions are in the block. A random
	// transaction is added before all others.
	block := cs.CurrentBlock()
	if len(block.Transactions) != len(added)+1 {
		t.Fatalf("expected %v transactions, got %v", len(added), len(block.Transactions))
	}
	// the first transaction in the block should be ignored
	for i, txn := range block.Transactions[1:] {
		if txn.ID() != added[i] {
			t.Fatalf("transaction %v expected ID %v, got %v", i, added[i], txn.ID())
		}
	}
}
