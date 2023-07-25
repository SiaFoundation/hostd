package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

func accountBalance(tx txn, accountID rhpv3.Account) (dbID int64, balance types.Currency, err error) {
	err = tx.QueryRow(`SELECT id, balance FROM accounts WHERE account_id=$1`, sqlHash256(accountID)).Scan(&dbID, (*sqlCurrency)(&balance))
	return
}

// AccountBalance returns the balance of the account with the given ID.
func (s *Store) AccountBalance(accountID rhpv3.Account) (balance types.Currency, err error) {
	_, balance, err = accountBalance(&dbTxn{s}, accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return types.ZeroCurrency, nil
	}
	return
}

// CreditAccount adds the specified amount to the account with the given ID.
func (s *Store) CreditAccount(accountID rhpv3.Account, amount types.Currency, expiration time.Time) (balance types.Currency, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTxnTimeout)
	defer cancel()

	err = s.transaction(ctx, func(tx txn) error {
		var dbID int64
		// get current balance
		dbID, initial, err := accountBalance(tx, accountID)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.Exec(`INSERT INTO accounts (account_id, balance, expiration_timestamp) VALUES ($1, $2, $3)`, sqlHash256(accountID), sqlCurrency(amount), sqlTime(expiration))
			if err != nil {
				return fmt.Errorf("failed to create account: %w", err)
			}
			balance = amount
		} else if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		}
		// update balance
		balance = initial.Add(amount)
		_, err = tx.Exec(`UPDATE accounts SET (balance, expiration_timestamp) = ($1, $2) WHERE id=$3`, sqlCurrency(balance), sqlTime(expiration), dbID)
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}
		return nil
	})
	return
}

// DebitAccount subtracts the specified amount from the account with the given
// ID. Returns the remaining balance of the account.
func (s *Store) DebitAccount(accountID rhpv3.Account, amount types.Currency) (balance types.Currency, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTxnTimeout)
	defer cancel()
	err = s.transaction(ctx, func(tx txn) error {
		var dbID int64
		dbID, balance, err = accountBalance(tx, accountID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if balance.Cmp(amount) < 0 {
			return fmt.Errorf("insufficient balance")
		}
		// update balance
		balance = balance.Sub(amount)
		_, err = tx.Exec(`UPDATE accounts SET balance=$1 WHERE id=$2`, sqlCurrency(balance), dbID)
		return err
	})
	return
}

// PruneAccounts removes all accounts that have expired
func (s *Store) PruneAccounts(height uint64) error {
	_, err := s.exec(`DELETE FROM accounts WHERE expiration_height<$1`, height)
	return err
}
