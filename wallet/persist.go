package wallet

import (
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
)

type (
	// An UpdateTransaction atomically updates the wallet store
	UpdateTransaction interface {
		AddSiacoinElement(SiacoinElement) error
		RemoveSiacoinElement(types.SiacoinOutputID) error
		AddTransaction(Transaction) error
		RevertBlock(types.BlockID) error
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		// LastWalletChange returns the consensus change ID and block height of
		// the last wallet change.
		LastWalletChange() (id modules.ConsensusChangeID, height uint64, err error)
		// UnspentSiacoinElements returns a list of all unspent siacoin outputs
		UnspentSiacoinElements() ([]SiacoinElement, error)
		// Transactions returns a paginated list of transactions ordered by
		// block height, descending. If no more transactions are available,
		// (nil, nil) should be returned.
		Transactions(limit, offset int) ([]Transaction, error)
		// TransactionCount returns the total number of transactions in the
		// wallet.
		TransactionCount() (uint64, error)

		UpdateWallet(ccID modules.ConsensusChangeID, height uint64, fn func(UpdateTransaction) error) error
	}
)
