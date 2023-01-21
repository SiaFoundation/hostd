package sqlite

import (
	"go.sia.tech/hostd/host/contracts"
)

// Update begins an update transaction on the wallet store.
func (s *Store) UpdateContracts(fn func(contracts.UpdateContractTransaction) error) error {
	return s.exclusiveTransaction(func(tx txn) error {
		return fn(nil)
	})
}
