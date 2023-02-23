package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

func accountBalance(tx txn, accountID rhpv3.Account) (balance types.Currency, err error) {
	err = tx.QueryRow(`SELECT balance FROM accounts WHERE id=$1`, sqlHash256(accountID)).Scan((*sqlCurrency)(&balance))
	if errors.Is(err, sql.ErrNoRows) {
		return types.ZeroCurrency, nil
	}
	return
}

// AccountBalance returns the balance of the account with the given ID.
func (s *Store) AccountBalance(accountID rhpv3.Account) (balance types.Currency, err error) {
	return accountBalance(&dbTxn{s}, accountID)
}

// CreditAccount adds the specified amount to the account with the given ID.
func (s *Store) CreditAccount(accountID rhpv3.Account, amount types.Currency, expiration time.Time) (balance types.Currency, err error) {
	err = s.transaction(func(tx txn) error {
		// get current balance
		balance, err = accountBalance(tx, accountID)
		if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		}
		// update balance
		balance = balance.Add(amount)
		_, err = tx.Exec(`INSERT INTO accounts (id, balance, expiration_timestamp) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET balance=EXCLUDED.balance, expiration_timestamp=EXCLUDED.expiration_timestamp`, sqlHash256(accountID), sqlCurrency(balance), sqlTime(expiration))
		return err
	})
	return
}

// DebitAccount subtracts the specified amount from the account with the given
// ID. Returns the remaining balance of the account.
func (s *Store) DebitAccount(accountID rhpv3.Account, amount types.Currency) (balance types.Currency, err error) {
	err = s.transaction(func(tx txn) error {
		// get current balance
		err := tx.QueryRow(`SELECT balance FROM accounts WHERE id=$1`, sqlHash256(accountID)).Scan((*sqlCurrency)(&balance))
		if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		} else if balance.Cmp(amount) < 0 {
			return fmt.Errorf("insufficient balance")
		}
		// update balance
		balance = balance.Sub(amount)
		_, err = tx.Exec(`UPDATE accounts SET balance=$1 WHERE id=$2`, sqlCurrency(balance), sqlHash256(accountID))
		return err
	})
	return
}

// PruneAccounts removes all accounts that have expired
func (s *Store) PruneAccounts(height uint64) error {
	_, err := s.db.Exec(`DELETE FROM accounts WHERE expiration_height<$1`, height)
	return err
}
