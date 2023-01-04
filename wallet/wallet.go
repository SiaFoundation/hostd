package wallet

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// transaction sources indicate the source of a transaction. Transactions can
// either be created by sending Siacoins between unlock hashes or they can be
// created by consensus (e.g. a miner payout, a siafund claim, or a contract).
const (
	TxnSourceTransaction  TransactionSource = "transaction"
	TxnSourceMinerPayout  TransactionSource = "minerPayout"
	TxnSourceSiafundClaim TransactionSource = "siafundClaim"
	TxnSourceContract     TransactionSource = "contract"
)

type (
	// A TransactionSource is a string indicating the source of a transaction.
	TransactionSource string

	// A ChainManager manages the current state of the blockchain.
	ChainManager interface {
		Tip() consensus.State
		BlockAtHeight(height uint64) (types.Block, bool)
	}

	// A SiacoinElement is a SiacoinOutput along with its ID.
	SiacoinElement struct {
		types.SiacoinOutput
		ID types.SiacoinOutputID
	}

	// A Transaction is an on-chain transaction relevant to a particular wallet,
	// paired with useful metadata.
	Transaction struct {
		ID          types.TransactionID  `json:"id"`
		Index       consensus.ChainIndex `json:"index"`
		Transaction types.Transaction    `json:"transaction"`
		Inflow      types.Currency       `json:"inflow"`
		Outflow     types.Currency       `json:"outflow"`
		Source      TransactionSource    `json:"source"`
		Timestamp   time.Time            `json:"timestamp"`
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
	// a single address.
	SingleAddressWallet struct {
		priv  ed25519.PrivateKey
		addr  types.UnlockHash
		cm    ChainManager
		store SingleAddressStore

		// for building transactions
		mu   sync.Mutex
		used map[types.SiacoinOutputID]bool
	}

	// An UpdateTransaction atomically updates the wallet store
	UpdateTransaction interface {
		AddSiacoinElement(utxo SiacoinElement) error
		RemoveSiacoinElement(id types.SiacoinOutputID) error
		AddTransaction(txn Transaction) error
		RemoveTransaction(id types.TransactionID) error
		SetLastChange(id modules.ConsensusChangeID) error
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		Update(context.Context, func(UpdateTransaction) error) error
		Close() error

		UnspentSiacoinElements() ([]SiacoinElement, error)
		Transactions(skip, max int) ([]Transaction, error)
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

// Close closes the underlying wallet store
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

// FundTransaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be available to future calls to FundTransaction unless ReleaseInputs
// is called.
func (sw *SingleAddressWallet) FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]crypto.Hash, func(), error) {
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
	var inputSum types.Currency
	var fundingElements []SiacoinElement
	for _, sce := range utxos {
		if sw.used[sce.ID] || inPool[sce.ID] {
			continue
		}
		fundingElements = append(fundingElements, sce)
		inputSum = inputSum.Add(sce.Value)
		if inputSum.Cmp(amount) >= 0 {
			break
		}
	}
	if inputSum.Cmp(amount) < 0 {
		return nil, nil, errors.New("insufficient balance")
	} else if inputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:      inputSum.Sub(amount),
			UnlockHash: sw.addr,
		})
	}

	toSign := make([]crypto.Hash, len(fundingElements))
	for i, sce := range fundingElements {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(sw.priv.Public().(ed25519.PublicKey)),
		})
		toSign[i] = crypto.Hash(sce.ID)
		sw.used[sce.ID] = true
	}

	release := func() {
		sw.mu.Lock()
		defer sw.mu.Unlock()
		for _, id := range toSign {
			delete(sw.used, types.SiacoinOutputID(id))
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
func (sw *SingleAddressWallet) SignTransaction(txn *types.Transaction, toSign []crypto.Hash, cf types.CoveredFields) error {
	sigMap := make(map[crypto.Hash]bool)
	for _, id := range toSign {
		sigMap[id] = true
	}
	for _, id := range toSign {
		i := len(txn.TransactionSignatures)
		txn.TransactionSignatures = append(txn.TransactionSignatures, types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
		})
		sigHash := txn.SigHash(i, types.BlockHeight(sw.cm.Tip().Index.Height))
		txn.TransactionSignatures[i].Signature = ed25519.Sign(sw.priv, sigHash[:])
	}
	return nil
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (sw *SingleAddressWallet) ProcessConsensusChange(cc modules.ConsensusChange) {
	// create payout transactions for each matured siacoin output. Each diff
	// should correspond to an applied block. This is done outside of the
	// database transaction to reduce lock contention.
	appliedPayoutTxns := make([][]Transaction, len(cc.AppliedDiffs))
	// calculate the block height of the first applied diff
	blockHeight := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
	for i := 0; i < len(cc.AppliedDiffs); i, blockHeight = i+1, blockHeight+1 {
		block := cc.AppliedBlocks[i]
		diff := cc.AppliedDiffs[i]
		index := consensus.ChainIndex{
			ID:     block.ID(),
			Height: blockHeight,
		}

		// determine the source of each delayed output
		delayedOutputSources := make(map[types.SiacoinOutputID]TransactionSource)
		if blockHeight > uint64(types.MaturityDelay) {
			// get the block that has matured
			matureBlock, ok := sw.cm.BlockAtHeight(blockHeight - uint64(types.MaturityDelay))
			if !ok {
				panic(fmt.Errorf("failed to get mature block at height %v", blockHeight-uint64(types.MaturityDelay)))
			}
			for i := range matureBlock.MinerPayouts {
				delayedOutputSources[matureBlock.MinerPayoutID(uint64(i))] = TxnSourceMinerPayout
			}
			for _, txn := range matureBlock.Transactions {
				for _, output := range txn.SiafundInputs {
					delayedOutputSources[output.ParentID.SiaClaimOutputID()] = TxnSourceSiafundClaim
				}
			}
		}

		for _, dsco := range diff.DelayedSiacoinOutputDiffs {
			// if a delayed output is reverted in an applied diff, the
			// output has matured -- add a payout transaction.
			if dsco.SiacoinOutput.UnlockHash != sw.addr || dsco.Direction != modules.DiffRevert {
				continue
			}
			// contract payouts are harder to identify, any unknown output
			// ID is assumed to be a contract payout.
			var source TransactionSource
			if s, ok := delayedOutputSources[dsco.ID]; ok {
				source = s
			} else {
				source = TxnSourceContract
			}
			// append the payout transaction to the diff
			appliedPayoutTxns[i] = append(appliedPayoutTxns[i], payoutTransaction(dsco.SiacoinOutput, index, source, time.Unix(int64(block.Timestamp), 0)))
		}
	}

	// begin a database transaction to update the wallet state
	err := sw.store.Update(context.Background(), func(tx UpdateTransaction) error {
		// add new siacoin outputs and remove spent or reverted siacoin outputs
		for _, diff := range cc.SiacoinOutputDiffs {
			if diff.SiacoinOutput.UnlockHash != sw.addr {
				continue
			}
			if diff.Direction == modules.DiffApply {
				err := tx.AddSiacoinElement(SiacoinElement{
					SiacoinOutput: diff.SiacoinOutput,
					ID:            diff.ID,
				})
				if err != nil {
					return fmt.Errorf("failed to add siacoin element %v: %w", diff.ID, err)
				}
			} else {
				err := tx.RemoveSiacoinElement(diff.ID)
				if err != nil {
					return fmt.Errorf("failed to remove siacoin element %v: %w", diff.ID, err)
				}
			}
		}

		// revert payout transactions
		for _, reverted := range cc.RevertedDiffs {
			for _, dsco := range reverted.DelayedSiacoinOutputDiffs {
				// if a delayed output is applied in a revert diff, the output
				// is no longer matured -- remove the payout transaction.
				if dsco.SiacoinOutput.UnlockHash != sw.addr || dsco.Direction != modules.DiffApply {
					continue
				}
				err := tx.RemoveTransaction(payoutTransactionID(dsco.SiacoinOutput))
				if err != nil {
					return fmt.Errorf("failed to remove payout transaction: %w", err)
				}
			}
		}

		// revert actual transactions
		for _, block := range cc.RevertedBlocks {
			for _, txn := range block.Transactions {
				if transactionIsRelevant(txn, sw.addr) {
					if err := tx.RemoveTransaction(txn.ID()); err != nil {
						return fmt.Errorf("failed to remove transaction %v: %w", txn.ID(), err)
					}
				}
			}
		}

		// calculate the block height of the first applied block
		blockHeight = uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
		// apply transactions
		for i := 0; i < len(cc.AppliedBlocks); i, blockHeight = i+1, blockHeight+1 {
			block := cc.AppliedBlocks[i]
			index := consensus.ChainIndex{
				ID:     block.ID(),
				Height: blockHeight,
			}

			// apply payout transactions -- all transactions should be relevant
			// to the wallet
			for _, txn := range appliedPayoutTxns[i] {
				if err := tx.AddTransaction(txn); err != nil {
					return fmt.Errorf("failed to add payout transaction %v: %w", txn.ID, err)
				}
			}

			// apply actual transactions -- only relevant transactions should be
			// added to the database
			for _, txn := range block.Transactions {
				if !transactionIsRelevant(txn, sw.addr) {
					continue
				}
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

				err := tx.AddTransaction(Transaction{
					ID:          txn.ID(),
					Index:       index,
					Inflow:      inflow,
					Outflow:     outflow,
					Source:      TxnSourceTransaction,
					Transaction: txn,
					Timestamp:   time.Unix(int64(block.Timestamp), 0),
				})
				if err != nil {
					return fmt.Errorf("failed to add transaction %v: %w", txn.ID(), err)
				}
			}
		}

		// update the change ID
		if err := tx.SetLastChange(cc.ID); err != nil {
			return fmt.Errorf("failed to set index: %w", err)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// payoutTransaction wraps a delayed siacoin output in a transaction for display
// in the wallet.
func payoutTransaction(output types.SiacoinOutput, index consensus.ChainIndex, source TransactionSource, timestamp time.Time) Transaction {
	txn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{output},
	}
	return Transaction{
		ID:          txn.ID(),
		Index:       index,
		Transaction: txn,
		Inflow:      output.Value,
		Source:      source,
		Timestamp:   timestamp,
	}
}

func payoutTransactionID(output types.SiacoinOutput) types.TransactionID {
	txn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{output},
	}
	return txn.ID()
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv ed25519.PrivateKey, cm ChainManager, store SingleAddressStore) *SingleAddressWallet {
	return &SingleAddressWallet{
		priv:  priv,
		addr:  StandardAddress(priv.Public().(ed25519.PublicKey)),
		store: store,
		used:  make(map[types.SiacoinOutputID]bool),
		cm:    cm,
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
