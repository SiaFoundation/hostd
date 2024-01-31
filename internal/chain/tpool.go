package chain

import (
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
)

// TransactionPool wraps the siad transaction pool with a more convenient API.
type TransactionPool struct {
	tp modules.TransactionPool
}

// RecommendedFee returns the recommended fee per byte.
func (tp *TransactionPool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, (*types.V1Currency)(&fee))
	return
}

// Transactions returns the transactions in the transaction pool.
func (tp *TransactionPool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

// AcceptTransactionSet adds a transaction set to the tpool and broadcasts it to
// the network.
func (tp *TransactionPool) AcceptTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	return tp.tp.AcceptTransactionSet(stxns)
}

// UnconfirmedParents returns the unconfirmed parents of a transaction.
func (tp *TransactionPool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	pool := tp.Transactions()
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	seen := make(map[types.TransactionID]bool)
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[sci.ParentID]; ok {
			if txid := parent.ID(); !seen[txid] {
				seen[txid] = true
				parents = append(parents, *parent)
			}
		}
	}
	return parents, nil
}

// Subscribe subscribes to the transaction pool.
func (tp *TransactionPool) Subscribe(s modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(s)
}

// Close closes the transaction pool.
func (tp *TransactionPool) Close() error {
	return tp.tp.Close()
}

// NewTPool wraps a siad transaction pool with a more convenient API.
func NewTPool(tp modules.TransactionPool) *TransactionPool {
	return &TransactionPool{tp}
}
