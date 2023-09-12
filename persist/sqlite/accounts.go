package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.uber.org/zap"
)

// AccountBalance returns the balance of the account with the given ID.
func (s *Store) AccountBalance(accountID rhp3.Account) (balance types.Currency, err error) {
	_, balance, err = accountBalance(&dbTxn{s}, accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return types.ZeroCurrency, nil
	}
	return
}

func incrementContractAccountFunding(tx txn, accountID int64, fundingSource types.FileContractID, amount types.Currency) error {
	var contractDBID int64
	var fundingValue types.Currency
	err := tx.QueryRow(`SELECT id FROM contracts WHERE contract_id=$1`, sqlHash256(fundingSource)).Scan(&contractDBID)
	if err != nil {
		return fmt.Errorf("failed to get funding source: %w", err)
	}
	err = tx.QueryRow(`SELECT amount FROM contract_account_funding WHERE contract_id=$1 AND account_id=$2`, contractDBID, accountID).Scan((*sqlCurrency)(&fundingValue))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get fund amount: %w", err)
	}
	fundingValue = fundingValue.Add(amount)
	_, err = tx.Exec(`INSERT INTO contract_account_funding (contract_id, account_id, amount) VALUES ($1, $2, $3) ON CONFLICT (contract_id, account_id) DO UPDATE SET amount=EXCLUDED.amount`, contractDBID, accountID, sqlCurrency(fundingValue))
	if err != nil {
		return fmt.Errorf("failed to update funding source: %w", err)
	}
	return nil
}

// CreditAccount adds the specified amount to the account with the given ID.
func (s *Store) CreditAccount(account rhp3.Account, amount types.Currency, fundingSource types.FileContractID, expiration time.Time) (balance types.Currency, err error) {
	err = s.transaction(func(tx txn) error {
		// get current balance
		accountID, balance, err := accountBalance(tx, account)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to query balance: %w", err)
		}
		// update balance
		balance = balance.Add(amount)
		const query = `INSERT INTO accounts (account_id, balance, expiration_timestamp) VALUES ($1, $2, $3) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance, expiration_timestamp=EXCLUDED.expiration_timestamp RETURNING id`
		err = tx.QueryRow(query, sqlHash256(account), sqlCurrency(balance), sqlTime(expiration)).Scan(&accountID)
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}

		// update the funding source
		if err := incrementContractAccountFunding(tx, accountID, fundingSource, amount); err != nil {
			return fmt.Errorf("failed to update funding source: %w", err)
		}
		return nil
	})
	return
}

// DebitAccount subtracts the specified amount from the account with the given
// ID. Returns the remaining balance of the account.
func (s *Store) DebitAccount(accountID rhp3.Account, usage accounts.Usage) (balance types.Currency, err error) {
	amount := usage.Total()
	err = s.transaction(func(tx txn) error {
		dbID, balance, err := accountBalance(tx, accountID)
		if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		} else if balance.Cmp(amount) < 0 {
			return fmt.Errorf("insufficient balance")
		}

		// update balance
		balance = balance.Sub(amount)
		const query = `UPDATE accounts SET balance=$1 WHERE id=$8 RETURNING id`
		err = tx.QueryRow(query, sqlCurrency(balance), dbID).Scan(&dbID)
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		} else if err := updateContractUsage(tx, dbID, usage, s.log); err != nil {
			return fmt.Errorf("failed to update contract usage: %w", err)
		}
		return nil
	})
	return
}

// Accounts returns all accounts in the database paginated.
func (s *Store) Accounts(limit, offset int) (acc []accounts.Account, err error) {
	rows, err := s.query(`SELECT account_id, balance, expiration_timestamp FROM accounts LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var a accounts.Account
		if err := rows.Scan((*sqlHash256)(&a.ID), (*sqlCurrency)(&a.Balance), (*sqlTime)(&a.Expiration)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		acc = append(acc, a)
	}
	return
}

// AccountFunding returns all contracts that were used to fund the account.
func (s *Store) AccountFunding(account rhp3.Account) (srcs []accounts.FundingSource, err error) {
	const query = `SELECT a.account_id, c.contract_id, caf.amount
FROM contract_account_funding caf
INNER JOIN accounts a ON a.id=caf.account_id
INNER JOIN contracts c ON c.id=caf.contract_id
WHERE a.account_id=$1`

	rows, err := s.query(query, sqlHash256(account))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var src accounts.FundingSource
		if err := rows.Scan((*sqlHash256)(&src.AccountID), (*sqlHash256)(&src.ContractID), (*sqlCurrency)(&src.Amount)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		srcs = append(srcs, src)
	}
	return
}

// PruneAccounts removes all accounts that have expired
func (s *Store) PruneAccounts(height uint64) error {
	_, err := s.exec(`DELETE FROM accounts WHERE expiration_height<$1`, height)
	return err
}

func accountBalance(tx txn, accountID rhp3.Account) (dbID int64, balance types.Currency, err error) {
	err = tx.QueryRow(`SELECT id, balance FROM accounts WHERE account_id=$1`, sqlHash256(accountID)).Scan(&dbID, (*sqlCurrency)(&balance))
	return
}

type fundAmount struct {
	ID         int64
	ContractID int64
	Amount     types.Currency
}

// contractFunding returns all contracts that were used to fund the account.
func contractFunding(tx txn, accountID int64) (fund []fundAmount, err error) {
	rows, err := tx.Query(`SELECT id, contract_id, amount FROM contract_account_funding WHERE account_id=$1`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var f fundAmount
		if err := rows.Scan(&f.ID, &f.ContractID, (*sqlCurrency)(&f.Amount)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		} else if f.Amount.IsZero() {
			continue
		}
		fund = append(fund, f)
	}
	return
}

// updateContractUsage distributes account usage to the contracts that funded
// the account.
func updateContractUsage(tx txn, accountID int64, usage accounts.Usage, log *zap.Logger) error {
	funding, err := contractFunding(tx, accountID)
	if err != nil {
		return fmt.Errorf("failed to get contract funding: %w", err)
	}

	distributeFunds := func(usage, additional, remainder *types.Currency) {
		if remainder.IsZero() || usage.IsZero() {
			return
		}

		v := *usage
		if usage.Cmp(*remainder) > 0 {
			v = *remainder
		}
		*usage = usage.Sub(v)
		*remainder = remainder.Sub(v)
		*additional = additional.Add(v)
	}

	// distribute account usage to the funding contracts
	for _, f := range funding {
		remainder := f.Amount

		var additionalUsage contracts.Usage
		distributeFunds(&usage.StorageRevenue, &additionalUsage.StorageRevenue, &remainder)
		distributeFunds(&usage.IngressRevenue, &additionalUsage.IngressRevenue, &remainder)
		distributeFunds(&usage.EgressRevenue, &additionalUsage.EgressRevenue, &remainder)
		distributeFunds(&usage.RegistryRead, &additionalUsage.RegistryRead, &remainder)
		distributeFunds(&usage.RegistryWrite, &additionalUsage.RegistryWrite, &remainder)
		distributeFunds(&usage.RPCRevenue, &additionalUsage.RPCRevenue, &remainder)

		// add the additional usage to the contract
		if err := incrementContractUsage(tx, f.ContractID, additionalUsage); err != nil {
			return fmt.Errorf("failed to increment contract usage: %w", err)
		}
		// update the remaining value for the funding source
		if err := setContractAccountFunding(tx, f.ID, remainder); err != nil {
			return fmt.Errorf("failed to set account funding: %w", err)
		}

		contract, err := getContract(tx, f.ContractID)
		if err != nil {
			return fmt.Errorf("failed to get contract: %w", err)
		}
		// subtract the spending from the contract's account funding
		unspentContractFunds := contract.Usage.AccountFunding.Sub(f.Amount.Sub(remainder))
		if err := setContractRemainingFunds(tx, f.ContractID, unspentContractFunds); err != nil {
			return fmt.Errorf("failed to decrement account funding: %w", err)
		}

		if contract.Status == contracts.ContractStatusActive || contract.Status == contracts.ContractStatusPending {
			// increment potential revenue
			if err := incrementPotentialRevenueMetrics(tx, additionalUsage, false); err != nil {
				return fmt.Errorf("failed to increment contract potential revenue: %w", err)
			}
		} else if contract.Status == contracts.ContractStatusSuccessful && contract.RevisionConfirmed {
			// increment earned revenue
			if err := incrementEarnedRevenueMetrics(tx, additionalUsage, false); err != nil {
				return fmt.Errorf("failed to increment contract earned revenue: %w", err)
			}
		}
	}

	if !usage.Total().IsZero() {
		// note: any accounts funded before the v0.2.0 upgrade will have
		// unallocated usage.
		log.Debug("account usage not fully distributed", zap.Int64("account", accountID), zap.String("remainder", usage.Total().ExactString()))
	}
	return nil
}

func setContractRemainingFunds(tx txn, contractID int64, amount types.Currency) error {
	return tx.QueryRow(`UPDATE contracts SET account_funding=$1 WHERE id=$2 RETURNING id`, sqlCurrency(amount), contractID).Scan(&contractID)
}

func setContractAccountFunding(tx txn, fundingID int64, amount types.Currency) error {
	if amount.IsZero() {
		_, err := tx.Exec(`DELETE FROM contract_account_funding WHERE id=$1`, fundingID)
		return err
	}

	_, err := tx.Exec(`UPDATE contract_account_funding SET amount=$1 WHERE id=$2`, sqlCurrency(amount), fundingID)
	return err
}
