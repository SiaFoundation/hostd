package wallet

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// A ChainIndex
	ChainIndex struct {
		ID     types.BlockID
		Height uint64
	}

	// A SiacoinElement is a SiacoinOutput along with its ID.
	SiacoinElement struct {
		types.SiacoinOutput
		ID types.SiacoinOutputID
	}

	// A Transaction is an on-chain transaction relevant to a particular wallet,
	// paired with useful metadata.
	Transaction struct {
		Raw       types.Transaction
		Index     ChainIndex
		ID        types.TransactionID
		Inflow    types.Currency
		Outflow   types.Currency
		Timestamp time.Time
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
	// a single address.
	SingleAddressWallet struct {
		priv  ed25519.PrivateKey
		addr  types.UnlockHash
		store SingleAddressStore

		// for building transactions
		mu   sync.Mutex
		used map[types.SiacoinOutputID]bool
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		Index() ChainIndex
		UnspentSiacoinElements() ([]SiacoinElement, error)
		Transactions(skip, max int) ([]Transaction, error)

		AddSiacoinElement(output SiacoinElement) error
		RemoveSiacoinElement(id types.SiacoinOutputID) error
		AddTransaction(txn Transaction) error
		RemoveTransaction(id types.TransactionID) error

		LastChange() modules.ConsensusChangeID
		SetLastChange(modules.ConsensusChangeID, ChainIndex) error

		Close() error
	}
)

func transactionIsRelevant(txn types.Transaction, addr types.UnlockHash) bool {
	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	for i := range txn.SiacoinOutputs {
		if txn.SiacoinOutputs[i].UnlockHash == addr {
			return true
		}
	}
	for i := range txn.SiafundInputs {
		if txn.SiafundInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
		if txn.SiafundInputs[i].ClaimUnlockHash == addr {
			return true
		}
	}
	for i := range txn.SiafundOutputs {
		if txn.SiafundOutputs[i].UnlockHash == addr {
			return true
		}
	}
	for i := range txn.FileContracts {
		for _, sco := range txn.FileContracts[i].ValidProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
		for _, sco := range txn.FileContracts[i].MissedProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
	}
	for i := range txn.FileContractRevisions {
		for _, sco := range txn.FileContractRevisions[i].NewValidProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
		for _, sco := range txn.FileContractRevisions[i].NewMissedProofOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
	}
	return false
}

func (sw *SingleAddressWallet) Close() error {
	return sw.store.Close()
}

// Address returns the address of the wallet.
func (sw *SingleAddressWallet) Address() types.UnlockHash {
	return sw.addr
}

// Balance returns the balance of the wallet.
func (sw *SingleAddressWallet) Balance() (spendable, confirmed types.Currency, err error) {
	outputs, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return types.Currency{}, types.Currency{}, fmt.Errorf("failed to get unspent outputs: %w", err)
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, sco := range outputs {
		confirmed = confirmed.Add(sco.Value)
		if !sw.used[sco.ID] {
			spendable = spendable.Add(sco.Value)
		}
	}
	return
}

// Transactions returns up to max transactions relevant to the wallet that have
// a timestamp later than since.
func (sw *SingleAddressWallet) Transactions(skip, max int) ([]Transaction, error) {
	return sw.store.Transactions(skip, max)
}

// FundTransaction adds siacoin inputs worth at least the requested amount to
// the provided transaction. If necessary, a change output will also be added.
// The inputs will not be available to future calls to FundTransaction unless
// ReleaseInputs is called.
func (sw *SingleAddressWallet) FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.SiacoinOutputID, func(), error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if amount.IsZero() {
		return nil, nil, nil
	}

	// avoid reusing any inputs currently in the transaction pool
	inPool := make(map[types.SiacoinOutputID]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[in.ParentID] = true
		}
	}

	utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, nil, err
	}
	var outputSum types.Currency
	var fundingElements []SiacoinElement
	for _, sce := range utxos {
		if sw.used[sce.ID] || inPool[sce.ID] {
			continue
		}
		fundingElements = append(fundingElements, sce)
		outputSum = outputSum.Add(sce.Value)
		if outputSum.Cmp(amount) >= 0 {
			break
		}
	}
	if outputSum.Cmp(amount) < 0 {
		return nil, nil, errors.New("insufficient balance")
	} else if outputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:      outputSum.Sub(amount),
			UnlockHash: sw.addr,
		})
	}

	toSign := make([]types.SiacoinOutputID, len(fundingElements))
	for i, sce := range fundingElements {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(sw.priv.Public().(ed25519.PublicKey)),
		})
		toSign[i] = sce.ID
		sw.used[sce.ID] = true
	}

	release := func() {
		sw.mu.Lock()
		defer sw.mu.Unlock()
		for _, id := range toSign {
			delete(sw.used, id)
		}
	}

	return toSign, release, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (sw *SingleAddressWallet) ReleaseInputs(txn types.Transaction) {
	for _, in := range txn.SiacoinInputs {
		delete(sw.used, in.ParentID)
	}
}

// SignTransaction adds a signature to each of the specified inputs using the
// provided seed.
func (sw *SingleAddressWallet) SignTransaction(txn *types.Transaction, toSign []types.SiacoinOutputID) error {
	sigMap := make(map[types.SiacoinOutputID]bool)
	for _, id := range toSign {
		sigMap[id] = true
	}
	for i := range txn.SiacoinInputs {
		if !sigMap[txn.SiacoinInputs[i].ParentID] {
			continue
		}

		id := crypto.Hash(txn.SiacoinInputs[i].ParentID)
		n := len(txn.TransactionSignatures)
		txn.TransactionSignatures = append(txn.TransactionSignatures, types.TransactionSignature{
			ParentID:       crypto.Hash(id),
			CoveredFields:  types.FullCoveredFields,
			PublicKeyIndex: 0,
		})
		sigHash := txn.SigHash(n, types.BlockHeight(sw.store.Index().Height))
		txn.TransactionSignatures[n].Signature = ed25519.Sign(sw.priv, sigHash[:])
	}
	return nil
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (sw *SingleAddressWallet) ProcessConsensusChange(cc modules.ConsensusChange) {
	index := sw.store.Index()
	for _, diff := range cc.SiacoinOutputDiffs {
		if diff.SiacoinOutput.UnlockHash != sw.addr {
			continue
		}
		if diff.Direction == modules.DiffApply {
			err := sw.store.AddSiacoinElement(SiacoinElement{
				SiacoinOutput: diff.SiacoinOutput,
				ID:            diff.ID,
			})
			if err != nil {
				panic(err)
			}
		} else {
			err := sw.store.RemoveSiacoinElement(diff.ID)
			if err != nil {
				panic(err)
			}
		}
	}

	for _, block := range cc.RevertedBlocks {
		for _, txn := range block.Transactions {
			if transactionIsRelevant(txn, sw.addr) {
				if err := sw.store.RemoveTransaction(txn.ID()); err != nil {
					panic(err)
				}
			}
		}
		index.Height--
		index.ID = block.ID()
	}

	for _, block := range cc.AppliedBlocks {
		index.Height++
		index.ID = block.ID()

		for _, txn := range block.Transactions {
			if transactionIsRelevant(txn, sw.addr) {
				var inflow, outflow types.Currency
				for _, out := range txn.SiacoinOutputs {
					if out.UnlockHash == sw.addr {
						inflow = inflow.Add(out.Value)
					}
				}
				for _, in := range txn.SiacoinInputs {
					if in.UnlockConditions.UnlockHash() == sw.addr {
						inputValue := types.ZeroCurrency
						outflow = outflow.Add(inputValue)
					}
				}

				err := sw.store.AddTransaction(Transaction{
					ID:        txn.ID(),
					Index:     index,
					Inflow:    inflow,
					Outflow:   outflow,
					Raw:       txn,
					Timestamp: time.Unix(int64(block.Header().Timestamp), 0),
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}
	if err := sw.store.SetLastChange(cc.ID, index); err != nil {
		panic(err)
	}
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv ed25519.PrivateKey, store SingleAddressStore) *SingleAddressWallet {
	return &SingleAddressWallet{
		priv:  priv,
		addr:  StandardAddress(priv.Public().(ed25519.PublicKey)),
		store: store,
		used:  make(map[types.SiacoinOutputID]bool),
	}
}

// StandardUnlockConditions returns the standard unlock conditions for a single
// Ed25519 key.
func StandardUnlockConditions(pub ed25519.PublicKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys: []types.SiaPublicKey{{
			Algorithm: types.SignatureEd25519,
			Key:       pub,
		}},
		SignaturesRequired: 1,
	}
}

// StandardAddress returns the standard address for an Ed25519 key.
func StandardAddress(pub ed25519.PublicKey) types.UnlockHash {
	return StandardUnlockConditions(pub).UnlockHash()
}
