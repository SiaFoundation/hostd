package store

import (
	"sync"

	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// EphemeralWalletStore implements wallet.SingleAddressStore in memory.
type EphemeralWalletStore struct {
	mu      sync.Mutex
	tip     wallet.ChainIndex
	ccid    modules.ConsensusChangeID
	addr    types.UnlockHash
	scElems map[types.SiacoinOutputID]wallet.SiacoinElement
	txns    []wallet.Transaction
}

// AddSiacoinElement adds a siacoin element to the store.
func (es *EphemeralWalletStore) AddSiacoinElement(se wallet.SiacoinElement) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.scElems[se.ID] = se
	return nil
}

// RemoveSiacoinElement removes a siacoin element from the store.
func (es *EphemeralWalletStore) RemoveSiacoinElement(id types.SiacoinOutputID) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	delete(es.scElems, id)
	return nil
}

// AddTransaction adds a transaction to the store.
func (es *EphemeralWalletStore) AddTransaction(txn wallet.Transaction) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.txns = append(es.txns, txn)
	return nil
}

// RemoveTransaction removes a transaction from the store.
func (es *EphemeralWalletStore) RemoveTransaction(id types.TransactionID) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	for i, txn := range es.txns {
		if txn.ID == id {
			es.txns = append(es.txns[:i], es.txns[i+1:]...)
		}
	}
	return nil
}

// Index returns the current chain index.
func (es *EphemeralWalletStore) Index() wallet.ChainIndex {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.tip
}

// LastChange returns the last processed consensus change
func (es *EphemeralWalletStore) LastChange() modules.ConsensusChangeID {
	return modules.ConsensusChangeBeginning
}

// SetLastChange sets the last processed consensus change
func (es *EphemeralWalletStore) SetLastChange(ccid modules.ConsensusChangeID, idx wallet.ChainIndex) error {
	es.ccid = ccid
	es.tip = idx
	return nil
}

// Close implements wallet.SingleAddressStore.
func (es *EphemeralWalletStore) Close() error {
	return nil
}

// UnspentSiacoinElements implements wallet.SingleAddressStore.
func (es *EphemeralWalletStore) UnspentSiacoinElements() ([]wallet.SiacoinElement, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	var elems []wallet.SiacoinElement
	for _, sce := range es.scElems {
		_ = sce // V2: sce.MerkleProof = append([]rhp.Hash256(nil), sce.MerkleProof...)
		elems = append(elems, sce)
	}
	return elems, nil
}

// Transactions implements wallet.SingleAddressStore.
func (es *EphemeralWalletStore) Transactions(skip, max int) ([]wallet.Transaction, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	var txns []wallet.Transaction
	for _, txn := range es.txns {
		if len(txns) == max {
			break
		}
		txns = append(txns, txn)
	}
	return txns, nil
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(addr types.UnlockHash) *EphemeralWalletStore {
	return &EphemeralWalletStore{
		addr:    addr,
		scElems: make(map[types.SiacoinOutputID]wallet.SiacoinElement),
	}
}
