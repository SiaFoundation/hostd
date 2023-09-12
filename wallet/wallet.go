package wallet

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

// transaction sources indicate the source of a transaction. Transactions can
// either be created by sending Siacoins between unlock hashes or they can be
// created by consensus (e.g. a miner payout, a siafund claim, or a contract).
const (
	TxnSourceTransaction      TransactionSource = "transaction"
	TxnSourceMinerPayout      TransactionSource = "miner"
	TxnSourceSiafundClaim     TransactionSource = "siafundClaim"
	TxnSourceContract         TransactionSource = "contract"
	TxnSourceFoundationPayout TransactionSource = "foundation"
)

type (
	// A TransactionSource is a string indicating the source of a transaction.
	TransactionSource string

	// A ChainManager manages the current state of the blockchain.
	ChainManager interface {
		TipState() consensus.State
		BlockAtHeight(height uint64) (types.Block, bool)
		Subscribe(subscriber modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error
	}

	// A TransactionPool manages unconfirmed transactions.
	TransactionPool interface {
		Subscribe(subscriber modules.TransactionPoolSubscriber)
	}

	// A SiacoinElement is a SiacoinOutput along with its ID.
	SiacoinElement struct {
		types.SiacoinOutput
		ID types.SiacoinOutputID
	}

	// A Transaction is an on-chain transaction relevant to a particular wallet,
	// paired with useful metadata.
	Transaction struct {
		ID          types.TransactionID `json:"ID"`
		Index       types.ChainIndex    `json:"index"`
		Transaction types.Transaction   `json:"transaction"`
		Inflow      types.Currency      `json:"inflow"`
		Outflow     types.Currency      `json:"outflow"`
		Source      TransactionSource   `json:"source"`
		Timestamp   time.Time           `json:"timestamp"`
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
	// a single address.
	SingleAddressWallet struct {
		scanHeight uint64 // ensure 64-bit alignment on 32-bit systems

		priv types.PrivateKey
		addr types.Address

		cm    ChainManager
		store SingleAddressStore
		log   *zap.Logger
		tg    *threadgroup.ThreadGroup

		mu sync.Mutex // protects the following fields
		// tpoolTxns maps a transaction set ID to the transactions in that set
		tpoolTxns map[modules.TransactionSetID][]Transaction
		// tpoolUtxos maps a siacoin output ID to its corresponding siacoin
		// element. It is used to track siacoin outputs that are currently in
		// the transaction pool.
		tpoolUtxos map[types.SiacoinOutputID]SiacoinElement
		// tpoolSpent is a set of siacoin output IDs that are currently in the
		// transaction pool.
		tpoolSpent map[types.SiacoinOutputID]bool
		// consensusLocked is a set of siacoin output IDs that are currently in
		// the process of removal. Reduces a race-condition with UnspentUtxos
		// causing "siacoin output does not exist errors."
		consensusLocked map[types.SiacoinOutputID]bool
		// locked is a set of siacoin output IDs locked by FundTransaction. They
		// will be released either by calling Release for unused transactions or
		// being confirmed in a block.
		locked map[types.SiacoinOutputID]bool
	}
)

// ErrDifferentSeed is returned when a different seed is provided to
// NewSingleAddressWallet than was used to initialize the wallet
var ErrDifferentSeed = errors.New("seed differs from wallet seed")

// EncodeTo implements types.EncoderTo.
func (txn Transaction) EncodeTo(e *types.Encoder) {
	txn.ID.EncodeTo(e)
	txn.Index.EncodeTo(e)
	txn.Transaction.EncodeTo(e)
	txn.Inflow.EncodeTo(e)
	txn.Outflow.EncodeTo(e)
	e.WriteString(string(txn.Source))
	e.WriteTime(txn.Timestamp)
}

// DecodeFrom implements types.DecoderFrom.
func (txn *Transaction) DecodeFrom(d *types.Decoder) {
	txn.ID.DecodeFrom(d)
	txn.Index.DecodeFrom(d)
	txn.Transaction.DecodeFrom(d)
	txn.Inflow.DecodeFrom(d)
	txn.Outflow.DecodeFrom(d)
	txn.Source = TransactionSource(d.ReadString())
	txn.Timestamp = d.ReadTime()
}

func transactionIsRelevant(txn types.Transaction, addr types.Address) bool {
	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	for i := range txn.SiacoinOutputs {
		if txn.SiacoinOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.SiafundInputs {
		if txn.SiafundInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
		if txn.SiafundInputs[i].ClaimAddress == addr {
			return true
		}
	}
	for i := range txn.SiafundOutputs {
		if txn.SiafundOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.FileContracts {
		for _, sco := range txn.FileContracts[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContracts[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	for i := range txn.FileContractRevisions {
		for _, sco := range txn.FileContractRevisions[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContractRevisions[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	return false
}

// Close closes the wallet
func (sw *SingleAddressWallet) Close() error {
	sw.tg.Stop()
	return nil
}

// Address returns the address of the wallet.
func (sw *SingleAddressWallet) Address() types.Address {
	return sw.addr
}

// UnlockConditions returns the unlock conditions of the wallet.
func (sw *SingleAddressWallet) UnlockConditions() types.UnlockConditions {
	return sw.priv.PublicKey().StandardUnlockConditions()
}

// Balance returns the balance of the wallet.
func (sw *SingleAddressWallet) Balance() (spendable, confirmed, unconfirmed types.Currency, err error) {
	done, err := sw.tg.Add()
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, err
	}
	defer done()

	outputs, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to get unspent outputs: %w", err)
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, sco := range outputs {
		confirmed = confirmed.Add(sco.Value)
		if !sw.locked[sco.ID] && !sw.tpoolSpent[sco.ID] {
			spendable = spendable.Add(sco.Value)
		}
	}

	for _, sco := range sw.tpoolUtxos {
		unconfirmed = unconfirmed.Add(sco.Value)
	}
	return
}

// Transactions returns a paginated list of transactions, ordered by block
// height descending. If no more transactions are available, (nil, nil) is
// returned.
func (sw *SingleAddressWallet) Transactions(limit, offset int) ([]Transaction, error) {
	done, err := sw.tg.Add()
	if err != nil {
		return nil, err
	}
	defer done()
	return sw.store.Transactions(limit, offset)
}

// TransactionCount returns the total number of transactions in the wallet.
func (sw *SingleAddressWallet) TransactionCount() (uint64, error) {
	done, err := sw.tg.Add()
	if err != nil {
		return 0, err
	}
	defer done()
	return sw.store.TransactionCount()
}

// FundTransaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be available to future calls to FundTransaction unless ReleaseInputs
// is called.
func (sw *SingleAddressWallet) FundTransaction(txn *types.Transaction, amount types.Currency) ([]types.Hash256, func(), error) {
	done, err := sw.tg.Add()
	if err != nil {
		return nil, nil, err
	}
	defer done()

	if amount.IsZero() {
		return nil, func() {}, nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, nil, err
	}
	var inputSum types.Currency
	var fundingElements []SiacoinElement
	for _, sce := range utxos {
		if sw.locked[sce.ID] || sw.tpoolSpent[sce.ID] || sw.consensusLocked[sce.ID] {
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
			Value:   inputSum.Sub(amount),
			Address: sw.addr,
		})
	}

	toSign := make([]types.Hash256, len(fundingElements))
	for i, sce := range fundingElements {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: sw.priv.PublicKey().StandardUnlockConditions(),
		})
		toSign[i] = types.Hash256(sce.ID)
		sw.locked[sce.ID] = true
	}

	release := func() {
		sw.mu.Lock()
		defer sw.mu.Unlock()
		for _, id := range toSign {
			delete(sw.locked, types.SiacoinOutputID(id))
		}
	}
	return toSign, release, nil
}

// SignTransaction adds a signature to each of the specified inputs.
func (sw *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	done, err := sw.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	for _, id := range toSign {
		var h types.Hash256
		if cf.WholeTransaction {
			h = cs.WholeSigHash(*txn, id, 0, 0, cf.Signatures)
		} else {
			h = cs.PartialSigHash(*txn, cf)
		}
		sig := sw.priv.SignHash(h)
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}
	return nil
}

// ScanHeight returns the block height the wallet has scanned to.
func (sw *SingleAddressWallet) ScanHeight() uint64 {
	return atomic.LoadUint64(&sw.scanHeight)
}

// ReceiveUpdatedUnconfirmedTransactions implements modules.TransactionPoolSubscriber.
func (sw *SingleAddressWallet) ReceiveUpdatedUnconfirmedTransactions(diff *modules.TransactionPoolDiff) {
	done, err := sw.tg.Add()
	if err != nil {
		return
	}
	defer done()

	siacoinOutputs := make(map[types.SiacoinOutputID]SiacoinElement)
	utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return
	}
	for _, output := range utxos {
		siacoinOutputs[output.ID] = output
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	for id, output := range sw.tpoolUtxos {
		siacoinOutputs[id] = output
	}

	for _, txnsetID := range diff.RevertedTransactions {
		txns, ok := sw.tpoolTxns[txnsetID]
		if !ok {
			continue
		}
		for _, txn := range txns {
			for _, sci := range txn.Transaction.SiacoinInputs {
				delete(sw.tpoolSpent, sci.ParentID)
			}
			for i := range txn.Transaction.SiacoinOutputs {
				delete(sw.tpoolUtxos, txn.Transaction.SiacoinOutputID(i))
			}
		}
		delete(sw.tpoolTxns, txnsetID)
	}

	currentHeight := sw.cm.TipState().Index.Height

	for _, txnset := range diff.AppliedTransactions {
		var relevantTxns []Transaction

	txnLoop:
		for _, stxn := range txnset.Transactions {
			var relevant bool
			var txn types.Transaction
			convertToCore(stxn, &txn)
			processed := Transaction{
				ID: txn.ID(),
				Index: types.ChainIndex{
					Height: currentHeight + 1,
				},
				Transaction: txn,
				Source:      TxnSourceTransaction,
				Timestamp:   time.Now(),
			}
			for _, sci := range txn.SiacoinInputs {
				if sci.UnlockConditions.UnlockHash() != sw.addr {
					continue
				}
				relevant = true
				sw.tpoolSpent[sci.ParentID] = true

				output, ok := siacoinOutputs[sci.ParentID]
				if !ok {
					// note: happens during deep reorgs. Possibly a race
					// condition in siad. Log and skip.
					sw.log.Debug("tpool transaction unknown utxo", zap.Stringer("outputID", sci.ParentID), zap.Stringer("txnID", txn.ID()))
					continue txnLoop
				}
				processed.Outflow = processed.Outflow.Add(output.Value)
			}

			for i, sco := range txn.SiacoinOutputs {
				if sco.Address != sw.addr {
					continue
				}
				relevant = true
				outputID := txn.SiacoinOutputID(i)
				processed.Inflow = processed.Inflow.Add(sco.Value)
				sce := SiacoinElement{
					ID:            outputID,
					SiacoinOutput: sco,
				}
				siacoinOutputs[outputID] = sce
				sw.tpoolUtxos[outputID] = sce
			}

			if relevant {
				relevantTxns = append(relevantTxns, processed)
			}
		}

		if len(relevantTxns) != 0 {
			sw.tpoolTxns[txnset.ID] = relevantTxns
		}
	}
}

// UnconfirmedTransactions returns all unconfirmed transactions relevant to the
// wallet.
func (sw *SingleAddressWallet) UnconfirmedTransactions() (txns []Transaction, _ error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, txnset := range sw.tpoolTxns {
		txns = append(txns, txnset...)
	}
	return
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
func (sw *SingleAddressWallet) ProcessConsensusChange(cc modules.ConsensusChange) {
	done, err := sw.tg.Add()
	if err != nil {
		return
	}
	defer done()

	sw.log.Debug("processing consensus change", zap.Int("applied", len(cc.AppliedBlocks)), zap.Int("reverted", len(cc.RevertedBlocks)))
	start := time.Now()

	// create payout transactions for each matured siacoin output. Each diff
	// should correspond to an applied block. This is done outside of the
	// database transaction to reduce lock contention.
	appliedPayoutTxns := make([][]Transaction, len(cc.AppliedDiffs))
	// calculate the block height of the first applied diff
	blockHeight := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
	for i := 0; i < len(cc.AppliedDiffs); i, blockHeight = i+1, blockHeight+1 {
		var block types.Block
		convertToCore(cc.AppliedBlocks[i], &block)
		diff := cc.AppliedDiffs[i]
		index := types.ChainIndex{
			ID:     block.ID(),
			Height: blockHeight,
		}

		// determine the source of each delayed output
		delayedOutputSources := make(map[types.SiacoinOutputID]TransactionSource)
		if blockHeight > uint64(stypes.MaturityDelay) {
			matureHeight := blockHeight - uint64(stypes.MaturityDelay)
			// get the block that has matured
			matureBlock, ok := sw.cm.BlockAtHeight(matureHeight)
			if !ok {
				sw.log.Error("failed to get matured block", zap.Uint64("height", blockHeight), zap.Uint64("maturedHeight", matureHeight))
				sw.Close()
				return
			}
			matureID := matureBlock.ID()
			delayedOutputSources[matureID.FoundationOutputID()] = TxnSourceFoundationPayout
			for i := range matureBlock.MinerPayouts {
				delayedOutputSources[matureID.MinerOutputID(i)] = TxnSourceMinerPayout
			}
			for _, txn := range matureBlock.Transactions {
				for _, output := range txn.SiafundInputs {
					delayedOutputSources[output.ParentID.ClaimOutputID()] = TxnSourceSiafundClaim
				}
			}
		}

		for _, dsco := range diff.DelayedSiacoinOutputDiffs {
			// if a delayed output is reverted in an applied diff, the
			// output has matured -- add a payout transaction.
			if types.Address(dsco.SiacoinOutput.UnlockHash) != sw.addr || dsco.Direction != modules.DiffRevert {
				continue
			}
			// contract payouts are harder to identify, any unknown output
			// ID is assumed to be a contract payout.
			var source TransactionSource
			if s, ok := delayedOutputSources[types.SiacoinOutputID(dsco.ID)]; ok {
				source = s
			} else {
				source = TxnSourceContract
			}
			// append the payout transaction to the diff
			var utxo types.SiacoinOutput
			convertToCore(dsco.SiacoinOutput, &utxo)
			sce := SiacoinElement{
				ID:            types.SiacoinOutputID(dsco.ID),
				SiacoinOutput: utxo,
			}
			appliedPayoutTxns[i] = append(appliedPayoutTxns[i], payoutTransaction(sce, index, source, block.Timestamp))
		}
	}

	spentOutputs := make(map[types.SiacoinOutputID]types.SiacoinOutput)
	for _, applied := range cc.AppliedDiffs {
		for _, diff := range applied.SiacoinOutputDiffs {
			if diff.Direction == modules.DiffRevert {
				var so types.SiacoinOutput
				convertToCore(diff.SiacoinOutput, &so)
				spentOutputs[types.SiacoinOutputID(diff.ID)] = so
			}
		}
	}

	var locked []types.SiacoinOutputID
	sw.mu.Lock()
	for _, diff := range cc.SiacoinOutputDiffs {
		sw.consensusLocked[types.SiacoinOutputID(diff.ID)] = true
		locked = append(locked, types.SiacoinOutputID(diff.ID))
	}
	sw.mu.Unlock()

	// begin a database transaction to update the wallet state
	err = sw.store.UpdateWallet(cc.ID, uint64(cc.BlockHeight), func(tx UpdateTransaction) error {
		// add new siacoin outputs and remove spent or reverted siacoin outputs
		for _, diff := range cc.SiacoinOutputDiffs {
			if types.Address(diff.SiacoinOutput.UnlockHash) != sw.addr {
				continue
			}
			if diff.Direction == modules.DiffApply {
				var sco types.SiacoinOutput
				convertToCore(diff.SiacoinOutput, &sco)
				err := tx.AddSiacoinElement(SiacoinElement{
					SiacoinOutput: sco,
					ID:            types.SiacoinOutputID(diff.ID),
				})
				sw.log.Debug("added utxo", zap.String("id", diff.ID.String()), zap.String("value", sco.Value.ExactString()), zap.String("address", sco.Address.String()))
				if err != nil {
					return fmt.Errorf("failed to add siacoin element %v: %w", diff.ID, err)
				}
			} else {
				err := tx.RemoveSiacoinElement(types.SiacoinOutputID(diff.ID))
				if err != nil {
					return fmt.Errorf("failed to remove siacoin element %v: %w", diff.ID, err)
				}
				sw.log.Debug("removed utxo", zap.String("id", diff.ID.String()), zap.String("value", diff.SiacoinOutput.Value.String()), zap.String("address", diff.SiacoinOutput.UnlockHash.String()))
			}
		}

		// revert blocks -- will also revert all transactions and payout transactions
		for _, reverted := range cc.RevertedBlocks {
			blockID := types.BlockID(reverted.ID())
			if err := tx.RevertBlock(blockID); err != nil {
				return fmt.Errorf("failed to revert block %v: %w", blockID, err)
			}
		}

		for i, diff := range cc.RevertedDiffs {
			blockTimestamp := time.Unix(int64(cc.RevertedBlocks[i].Timestamp), 0)
			for _, sco := range diff.SiacoinOutputDiffs {
				var addr types.Address
				copy(addr[:], sco.SiacoinOutput.UnlockHash[:])
				if addr != sw.addr {
					continue
				}

				var value types.Currency
				convertToCore(sco.SiacoinOutput.Value, &value)
				switch sco.Direction {
				case modules.DiffApply:
					if err := tx.AddWalletDelta(value, blockTimestamp); err != nil {
						return fmt.Errorf("failed to add wallet delta: %w", err)
					}
				case modules.DiffRevert:
					if err := tx.SubWalletDelta(value, blockTimestamp); err != nil {
						return fmt.Errorf("failed to sub wallet delta: %w", err)
					}
				}
			}
		}

		for i, diff := range cc.AppliedDiffs {
			blockTimestamp := time.Unix(int64(cc.AppliedBlocks[i].Timestamp), 0)
			for _, sco := range diff.SiacoinOutputDiffs {
				var addr types.Address
				copy(addr[:], sco.SiacoinOutput.UnlockHash[:])
				if addr != sw.addr {
					continue
				}

				var value types.Currency
				convertToCore(sco.SiacoinOutput.Value, &value)
				switch sco.Direction {
				case modules.DiffApply:
					if err := tx.AddWalletDelta(value, blockTimestamp); err != nil {
						return fmt.Errorf("failed to add wallet delta: %w", err)
					}
				case modules.DiffRevert:
					if err := tx.SubWalletDelta(value, blockTimestamp); err != nil {
						return fmt.Errorf("failed to sub wallet delta: %w", err)
					}
				}
			}
		}

		// calculate the block height of the first applied block
		blockHeight = uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + 1
		// apply transactions
		for i := 0; i < len(cc.AppliedBlocks); i, blockHeight = i+1, blockHeight+1 {
			var block types.Block
			convertToCore(cc.AppliedBlocks[i], &block)
			index := types.ChainIndex{
				ID:     block.ID(),
				Height: blockHeight,
			}

			// apply actual transactions -- only relevant transactions should be
			// added to the database
			for _, txn := range block.Transactions {
				if !transactionIsRelevant(txn, sw.addr) {
					continue
				}
				var inflow, outflow types.Currency
				for _, out := range txn.SiacoinOutputs {
					if out.Address == sw.addr {
						inflow = inflow.Add(out.Value)
					}
				}
				for _, in := range txn.SiacoinInputs {
					if in.UnlockConditions.UnlockHash() == sw.addr {
						so, ok := spentOutputs[in.ParentID]
						if !ok {
							panic("spent output not found")
						}
						outflow = outflow.Add(so.Value)
					}
				}

				// ignore transactions that don't affect the wallet (e.g.
				// "setup" transactions)
				if inflow.Equals(outflow) {
					continue
				}

				err := tx.AddTransaction(Transaction{
					ID:          txn.ID(),
					Index:       index,
					Inflow:      inflow,
					Outflow:     outflow,
					Source:      TxnSourceTransaction,
					Transaction: txn,
					Timestamp:   block.Timestamp,
				})
				if err != nil {
					return fmt.Errorf("failed to add transaction %v: %w", txn.ID(), err)
				}
			}

			// apply payout transactions -- all transactions should be relevant
			// to the wallet
			for _, txn := range appliedPayoutTxns[i] {
				if err := tx.AddTransaction(txn); err != nil {
					return fmt.Errorf("failed to add payout transaction %v: %w", txn.ID, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		sw.log.Panic("failed to update wallet", zap.Error(err), zap.String("changeID", cc.ID.String()), zap.Uint64("height", uint64(cc.BlockHeight)))
	}

	sw.mu.Lock()
	for _, id := range locked {
		delete(sw.consensusLocked, id)
	}
	sw.mu.Unlock()

	atomic.StoreUint64(&sw.scanHeight, uint64(cc.BlockHeight))
	sw.log.Debug("applied consensus change", zap.String("changeID", cc.ID.String()), zap.Int("applied", len(cc.AppliedBlocks)), zap.Int("reverted", len(cc.RevertedBlocks)), zap.Uint64("height", uint64(cc.BlockHeight)), zap.Duration("elapsed", time.Since(start)), zap.String("address", sw.addr.String()))
}

// payoutTransaction wraps a delayed siacoin output in a transaction for display
// in the wallet.
func payoutTransaction(output SiacoinElement, index types.ChainIndex, source TransactionSource, timestamp time.Time) Transaction {
	return Transaction{
		ID:    types.TransactionID(output.ID),
		Index: index,
		Transaction: types.Transaction{
			SiacoinOutputs: []types.SiacoinOutput{output.SiacoinOutput},
		},
		Inflow:    output.Value,
		Source:    source,
		Timestamp: timestamp,
	}
}

// convertToCore converts a siad type to an equivalent core type.
func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv types.PrivateKey, cm ChainManager, tp TransactionPool, store SingleAddressStore, log *zap.Logger) (*SingleAddressWallet, error) {
	changeID, scanHeight, err := store.LastWalletChange()
	if err != nil {
		return nil, fmt.Errorf("failed to get last wallet change: %w", err)
	}

	seedHash := types.HashBytes(priv[:])
	if err := store.VerifyWalletKey(seedHash); errors.Is(err, ErrDifferentSeed) {
		changeID = modules.ConsensusChangeBeginning
		scanHeight = 0
		if err := store.ResetWallet(seedHash); err != nil {
			return nil, fmt.Errorf("failed to reset wallet: %w", err)
		}
		log.Info("wallet reset due to seed change")
	} else if err != nil {
		return nil, fmt.Errorf("failed to verify wallet key: %w", err)
	}

	sw := &SingleAddressWallet{
		priv:       priv,
		scanHeight: scanHeight,

		store: store,
		cm:    cm,
		log:   log,
		tg:    threadgroup.New(),

		addr: priv.PublicKey().StandardAddress(),

		locked:          make(map[types.SiacoinOutputID]bool),
		consensusLocked: make(map[types.SiacoinOutputID]bool),
		tpoolSpent:      make(map[types.SiacoinOutputID]bool),

		tpoolUtxos: make(map[types.SiacoinOutputID]SiacoinElement),
		tpoolTxns:  make(map[modules.TransactionSetID][]Transaction),
	}

	go func() {
		// note: start in goroutine to avoid blocking startup
		if err := cm.Subscribe(sw, changeID, sw.tg.Done()); err != nil {
			sw.log.Error("failed to subscribe to consensus changes", zap.Error(err))
			if errors.Is(err, chain.ErrInvalidChangeID) {
				// reset change ID and subscribe again
				if err := store.ResetWallet(seedHash); err != nil {
					sw.log.Fatal("failed to reset wallet", zap.Error(err))
				} else if err = cm.Subscribe(sw, modules.ConsensusChangeBeginning, sw.tg.Done()); err != nil {
					sw.log.Fatal("failed to reset consensus change subscription", zap.Error(err))
				}
			}
		}
	}()
	tp.Subscribe(sw)
	return sw, nil
}
