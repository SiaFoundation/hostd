package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/accounts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.uber.org/zap"
)

const accountExpirationTime = 90 * 24 * time.Hour

// RHP4AccountFunding returns all contracts that were used to fund the account.
func (s *Store) RHP4AccountFunding(account proto4.Account) (srcs []accounts.FundingSource, err error) {
	const query = `SELECT c.contract_id, caf.amount
FROM contract_v2_account_funding caf
INNER JOIN accounts a ON a.id=caf.account_id
INNER JOIN contracts_v2 c ON c.id=caf.contract_id
WHERE a.account_id=$1`

	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(query, encode(account))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var src accounts.FundingSource
			if err := rows.Scan(decode(&src.ContractID), decode(&src.Amount)); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			srcs = append(srcs, src)
		}
		return rows.Err()
	})
	return
}

// RHP4AccountBalance returns the balance of the account with the given ID.
func (s *Store) RHP4AccountBalance(account proto4.Account) (balance types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT balance FROM accounts WHERE account_id=$1`, encode(account)).Scan(decode(&balance))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		return nil
	})
	return
}

// RHP4DebitAccount debits the account with the given ID. The account's
// balance is drained first; any shortfall is drained from attached pools in
// attachment order. If the combined balance is insufficient, no balances are
// modified and proto4.ErrNotEnoughFunds is returned.
func (s *Store) RHP4DebitAccount(account proto4.Account, usage proto4.Usage) error {
	return s.transaction(func(tx *txn) error {
		var accountDBID int64
		var accountBalance types.Currency
		err := tx.QueryRow(`SELECT id, balance FROM accounts WHERE account_id=$1`, encode(account)).Scan(&accountDBID, decode(&accountBalance))
		if errors.Is(err, sql.ErrNoRows) {
			return proto4.ErrNotEnoughFunds
		} else if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		}

		attached, err := attachedPools(tx, accountDBID)
		if err != nil {
			return fmt.Errorf("failed to get attached pools: %w", err)
		}

		cost := usage.RenterCost()
		drawable := accountBalance
		for _, p := range attached {
			if drawable.Cmp(cost) >= 0 {
				break
			}
			drawable = drawable.Add(p.Balance)
		}
		if drawable.Cmp(cost) < 0 {
			return proto4.ErrNotEnoughFunds
		}

		// try account balance first
		accountUsage := takeRHP4Usage(&usage, accountBalance)
		if taken := accountUsage.RenterCost(); !taken.IsZero() {
			if _, err := tx.Exec(`UPDATE accounts SET balance=$1, expiration_timestamp=$2 WHERE id=$3`, encode(accountBalance.Sub(taken)), encode(time.Now().Add(accountExpirationTime)), accountDBID); err != nil {
				return fmt.Errorf("failed to update account balance: %w", err)
			} else if err := distributeRHP4AccountUsage(tx, accountDBID, accountUsage); err != nil {
				return fmt.Errorf("failed to update contract funding: %w", err)
			}
			cost = cost.Sub(taken)
		}

		if cost.IsZero() {
			return nil
		}

		updatePoolStmt, err := tx.Prepare(`UPDATE rhp4_pools SET balance=$1 WHERE id=$2`)
		if err != nil {
			return fmt.Errorf("failed to prepare update pool statement: %w", err)
		}
		defer updatePoolStmt.Close()

		// try all attached pools
		for _, p := range attached {
			if cost.IsZero() {
				break
			}
			poolUsage := takeRHP4Usage(&usage, p.Balance)
			if taken := poolUsage.RenterCost(); !taken.IsZero() {
				if _, err := updatePoolStmt.Exec(encode(p.Balance.Sub(taken)), p.ID); err != nil {
					return fmt.Errorf("failed to update pool balance: %w", err)
				} else if err := distributeRHP4PoolUsage(tx, p.ID, poolUsage); err != nil {
					return fmt.Errorf("failed to update pool funding: %w", err)
				}
				cost = cost.Sub(taken)
			}
		}
		if !cost.IsZero() {
			return proto4.ErrNotEnoughFunds
		}
		return nil
	})
}

// RHP4CreditAccounts credits the accounts with the given deposits and revises
// the contract.
func (s *Store) RHP4CreditAccounts(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract, usage proto4.Usage) (balances []types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		getBalanceStmt, err := tx.Prepare(`SELECT balance FROM accounts WHERE account_id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare get balance statement: %w", err)
		}
		defer getBalanceStmt.Close()

		updateBalanceStmt, err := tx.Prepare(`INSERT INTO accounts (account_id, balance, expiration_timestamp) VALUES ($1, $2, $3) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance, expiration_timestamp=EXCLUDED.expiration_timestamp RETURNING id`)
		if err != nil {
			return fmt.Errorf("failed to prepare update balance statement: %w", err)
		}
		defer updateBalanceStmt.Close()

		getFundingAmountStmt, err := tx.Prepare(`SELECT amount FROM contract_v2_account_funding WHERE contract_id=$1 AND account_id=$2`)
		if err != nil {
			return fmt.Errorf("failed to prepare get funding amount statement: %w", err)
		}
		defer getFundingAmountStmt.Close()

		updateFundingAmountStmt, err := tx.Prepare(`INSERT INTO contract_v2_account_funding (contract_id, account_id, amount) VALUES ($1, $2, $3) ON CONFLICT (contract_id, account_id) DO UPDATE SET amount=EXCLUDED.amount`)
		if err != nil {
			return fmt.Errorf("failed to prepare update funding amount statement: %w", err)
		}
		defer updateFundingAmountStmt.Close()

		var contractDBID int64
		err = tx.QueryRow(`SELECT id FROM contracts_v2 WHERE contract_id=$1`, encode(contractID)).Scan(&contractDBID)
		if err != nil {
			return fmt.Errorf("failed to get contract ID: %w", err)
		}

		var createdAccounts int
		for _, deposit := range deposits {
			var balance types.Currency
			err := getBalanceStmt.QueryRow(encode(deposit.Account)).Scan(decode(&balance))
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to get balance: %w", err)
			} else if errors.Is(err, sql.ErrNoRows) {
				createdAccounts++
			}

			balance = balance.Add(deposit.Amount)

			var accountDBID int64
			err = updateBalanceStmt.QueryRow(encode(deposit.Account), encode(balance), encode(time.Now().Add(accountExpirationTime))).Scan(&accountDBID)
			if err != nil {
				return fmt.Errorf("failed to update balance: %w", err)
			}
			balances = append(balances, balance)

			var fundAmount types.Currency
			if err := getFundingAmountStmt.QueryRow(contractDBID, accountDBID).Scan(decode(&fundAmount)); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to get funding amount: %w", err)
			}
			fundAmount = fundAmount.Add(deposit.Amount)
			if _, err := updateFundingAmountStmt.Exec(contractDBID, accountDBID, encode(fundAmount)); err != nil {
				return fmt.Errorf("failed to update funding amount: %w", err)
			}
		}

		if _, err = reviseV2Contract(tx, contractID, revision, usage); err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		}

		if err := incrementCurrencyStat(tx, metricAccountBalance, usage.AccountFunding, false, time.Now()); err != nil {
			return fmt.Errorf("failed to increment balance metric: %w", err)
		} else if err := incrementNumericStat(tx, metricActiveAccounts, createdAccounts, time.Now()); err != nil {
			return fmt.Errorf("failed to increment active accounts metric: %w", err)
		}

		return nil
	})
	return
}

// RHP4AccountBalances returns the balances of the accounts with the given IDs. The
// balances are returned in the same order as the input accounts. If an account does
// not exist, the balance at that index will be types.ZeroCurrency.
func (s *Store) RHP4AccountBalances(accounts []proto4.Account) (balances []types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`SELECT balance FROM accounts WHERE account_id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare get balance statement: %w", err)
		}
		defer stmt.Close()

		for _, account := range accounts {
			var balance types.Currency
			err := stmt.QueryRow(encode(account)).Scan(decode(&balance))
			if err != nil && !errors.Is(err, sql.ErrNoRows) { // missing accounts have a balance of 0
				return fmt.Errorf("failed to get balance: %w", err)
			}
			balances = append(balances, balance)
		}
		return nil
	})
	return
}

// AccountBalance returns the balance of the account with the given ID.
func (s *Store) AccountBalance(accountID rhp3.Account) (balance types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		_, balance, err = accountBalance(tx, accountID)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
			return nil
		}
		return err
	})
	return
}

// CreditAccountWithContract adds the specified amount to the account with the given ID.
func (s *Store) CreditAccountWithContract(fund accounts.FundAccountWithContract) error {
	return s.transaction(func(tx *txn) error {
		// get current balance
		accountID, balance, err := accountBalance(tx, fund.Account)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed to query balance: %w", err)
		}
		// update balance
		balance = balance.Add(fund.Amount)
		const query = `INSERT INTO accounts (account_id, balance, expiration_timestamp) VALUES ($1, $2, $3) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance, expiration_timestamp=EXCLUDED.expiration_timestamp RETURNING id`
		err = tx.QueryRow(query, encode(fund.Account), encode(balance), encode(fund.Expiration)).Scan(&accountID)
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}

		// update balance metric
		if err := incrementCurrencyStat(tx, metricAccountBalance, fund.Amount, false, time.Now()); err != nil {
			return fmt.Errorf("failed to increment balance metric: %w", err)
		}

		// update the number of active accounts
		if !exists {
			if err := incrementNumericStat(tx, metricActiveAccounts, 1, time.Now()); err != nil {
				return fmt.Errorf("failed to increment active accounts metric: %w", err)
			}
		}

		// revise the contract and update the usage
		usage := contracts.Usage{
			RPCRevenue:     fund.Cost,
			AccountFunding: fund.Amount,
		}
		contractID, err := reviseContract(tx, fund.Revision, usage)
		if err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		}

		// update the funding source
		if err := incrementContractAccountFunding(tx, accountID, contractID, fund.Amount); err != nil {
			return fmt.Errorf("failed to update funding source: %w", err)
		}
		return nil
	})
}

// DebitAccount subtracts the specified amount from the account with the given
// ID. Returns the remaining balance of the account.
func (s *Store) DebitAccount(accountID rhp3.Account, usage accounts.Usage) error {
	amount := usage.Total()
	return s.transaction(func(tx *txn) error {
		dbID, balance, err := accountBalance(tx, accountID)
		if err != nil {
			return fmt.Errorf("failed to query balance: %w", err)
		} else if balance.Cmp(amount) < 0 {
			return fmt.Errorf("insufficient balance")
		}

		// update balance
		balance = balance.Sub(amount)
		const query = `UPDATE accounts SET balance=$1 WHERE id=$8 RETURNING id`
		err = tx.QueryRow(query, encode(balance), dbID).Scan(&dbID)
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		} else if err := distributeRHP3AccountUsage(tx, dbID, usage, s.log); err != nil {
			return fmt.Errorf("failed to update contract usage: %w", err)
		}

		// update balance metric
		if err := incrementCurrencyStat(tx, metricAccountBalance, amount, true, time.Now()); err != nil {
			return fmt.Errorf("failed to increment balance metric: %w", err)
		}

		return nil
	})
}

// Accounts returns all accounts in the database paginated.
func (s *Store) Accounts(limit, offset int) (acc []accounts.Account, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT account_id, balance, expiration_timestamp FROM accounts LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var a accounts.Account
			if err := rows.Scan(decode(&a.ID), decode(&a.Balance), decode(&a.Expiration)); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			acc = append(acc, a)
		}
		return rows.Err()
	})
	return
}

// AccountFunding returns all contracts that were used to fund the account.
func (s *Store) AccountFunding(account rhp3.Account) (srcs []accounts.FundingSource, err error) {
	const query = `SELECT c.contract_id, caf.amount
FROM contract_account_funding caf
INNER JOIN accounts a ON a.id=caf.account_id
INNER JOIN contracts c ON c.id=caf.contract_id
WHERE a.account_id=$1`

	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(query, encode(account))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var src accounts.FundingSource
			if err := rows.Scan(decode(&src.ContractID), decode(&src.Amount)); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			srcs = append(srcs, src)
		}
		return rows.Err()
	})
	return
}

// PruneAccounts removes all accounts that have expired
func (s *Store) PruneAccounts(height uint64) error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`DELETE FROM accounts WHERE expiration_height<$1`, height)
		return err
	})
}

func incrementContractAccountFunding(tx *txn, accountID, contractID int64, amount types.Currency) error {
	var fundingValue types.Currency
	err := tx.QueryRow(`SELECT amount FROM contract_account_funding WHERE contract_id=$1 AND account_id=$2`, contractID, accountID).Scan(decode(&fundingValue))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get fund amount: %w", err)
	}
	fundingValue = fundingValue.Add(amount)
	_, err = tx.Exec(`INSERT INTO contract_account_funding (contract_id, account_id, amount) VALUES ($1, $2, $3) ON CONFLICT (contract_id, account_id) DO UPDATE SET amount=EXCLUDED.amount`, contractID, accountID, encode(fundingValue))
	if err != nil {
		return fmt.Errorf("failed to update funding source: %w", err)
	}
	return nil
}

func accountBalance(tx *txn, accountID rhp3.Account) (dbID int64, balance types.Currency, err error) {
	err = tx.QueryRow(`SELECT id, balance FROM accounts WHERE account_id=$1`, encode(accountID)).Scan(&dbID, decode(&balance))
	return
}

type fundAmount struct {
	ID         int64
	ContractID int64
	Amount     types.Currency
}

// contractV2Funding returns all contracts that were used to fund the account.
func contractV2Funding(tx *txn, accountID int64) (fund []fundAmount, err error) {
	rows, err := tx.Query(`SELECT id, contract_id, amount FROM contract_v2_account_funding WHERE account_id=$1`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var f fundAmount
		if err := rows.Scan(&f.ID, &f.ContractID, decode(&f.Amount)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		} else if f.Amount.IsZero() {
			continue
		}
		fund = append(fund, f)
	}
	return
}

// contractFunding returns all contracts that were used to fund the account.
func contractFunding(tx *txn, accountID int64) (fund []fundAmount, err error) {
	rows, err := tx.Query(`SELECT id, contract_id, amount FROM contract_account_funding WHERE account_id=$1`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var f fundAmount
		if err := rows.Scan(&f.ID, &f.ContractID, decode(&f.Amount)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		} else if f.Amount.IsZero() {
			continue
		}
		fund = append(fund, f)
	}
	return
}

// distributeRHP4AccountUsage distributes account usage to the contracts that funded
// the account.
func distributeRHP4AccountUsage(tx *txn, accountID int64, usage proto4.Usage) error {
	funding, err := contractV2Funding(tx, accountID)
	if err != nil {
		return fmt.Errorf("failed to get contract funding: %w", err)
	}

	for _, f := range funding {
		remainder := f.Amount

		var additionalUsage proto4.Usage
		distributeFunds(&usage.Storage, &additionalUsage.Storage, &remainder)
		distributeFunds(&usage.Ingress, &additionalUsage.Ingress, &remainder)
		distributeFunds(&usage.Egress, &additionalUsage.Egress, &remainder)
		distributeFunds(&usage.RPC, &additionalUsage.RPC, &remainder)

		if remainder.IsZero() {
			if _, err := tx.Exec(`DELETE FROM contract_v2_account_funding WHERE id=$1`, f.ID); err != nil {
				return fmt.Errorf("failed to delete account funding: %w", err)
			}
		} else {
			_, err := tx.Exec(`UPDATE contract_v2_account_funding SET amount=$1 WHERE id=$2`, encode(remainder), f.ID)
			if err != nil {
				return fmt.Errorf("failed to update account funding: %w", err)
			}
		}

		var contractExistingFunding types.Currency
		if err := tx.QueryRow(`SELECT account_funding FROM contracts_v2 WHERE id=$1`, f.ContractID).Scan(decode(&contractExistingFunding)); err != nil {
			return fmt.Errorf("failed to get contract usage: %w", err)
		}
		contractExistingFunding = contractExistingFunding.Sub(f.Amount.Sub(remainder))
		if _, err := tx.Exec(`UPDATE contracts_v2 SET account_funding=$1 WHERE id=$2`, encode(contractExistingFunding), f.ContractID); err != nil {
			return fmt.Errorf("failed to update contract account funding: %w", err)
		} else if err := updateV2ContractUsage(tx, f.ContractID, additionalUsage); err != nil {
			return fmt.Errorf("failed to update contract usage: %w", err)
		}
	}
	return nil
}

// distributeRHP3AccountUsage distributes account usage to the contracts that funded
// the account.
func distributeRHP3AccountUsage(tx *txn, accountID int64, usage accounts.Usage, log *zap.Logger) error {
	funding, err := contractFunding(tx, accountID)
	if err != nil {
		return fmt.Errorf("failed to get contract funding: %w", err)
	}

	for _, f := range funding {
		remainder := f.Amount

		var additionalUsage contracts.Usage
		distributeFunds(&usage.StorageRevenue, &additionalUsage.StorageRevenue, &remainder)
		distributeFunds(&usage.IngressRevenue, &additionalUsage.IngressRevenue, &remainder)
		distributeFunds(&usage.EgressRevenue, &additionalUsage.EgressRevenue, &remainder)
		distributeFunds(&usage.RegistryRead, &additionalUsage.RegistryRead, &remainder)
		distributeFunds(&usage.RegistryWrite, &additionalUsage.RegistryWrite, &remainder)
		distributeFunds(&usage.RPCRevenue, &additionalUsage.RPCRevenue, &remainder)

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
		} else if err := updateContractUsage(tx, f.ContractID, types.ZeroCurrency, additionalUsage); err != nil {
			return fmt.Errorf("failed to update contract usage: %w", err)
		}
	}

	if !usage.Total().IsZero() {
		// note: any accounts funded before the v0.2.0 upgrade will have
		// unallocated usage.
		log.Debug("account usage not fully distributed", zap.Int64("account", accountID), zap.String("remainder", usage.Total().ExactString()))
	}
	return nil
}

func setContractRemainingFunds(tx *txn, contractID int64, amount types.Currency) error {
	return tx.QueryRow(`UPDATE contracts SET account_funding=$1 WHERE id=$2 RETURNING id`, encode(amount), contractID).Scan(&contractID)
}

func setContractAccountFunding(tx *txn, fundingID int64, amount types.Currency) error {
	if amount.IsZero() {
		_, err := tx.Exec(`DELETE FROM contract_account_funding WHERE id=$1`, fundingID)
		return err
	}

	_, err := tx.Exec(`UPDATE contract_account_funding SET amount=$1 WHERE id=$2`, encode(amount), fundingID)
	return err
}

// takeRHP4Usage pulls up to max from remaining and returns what was taken.
func takeRHP4Usage(remaining *proto4.Usage, limit types.Currency) (taken proto4.Usage) {
	distributeFunds(&remaining.Storage, &taken.Storage, &limit)
	distributeFunds(&remaining.Ingress, &taken.Ingress, &limit)
	distributeFunds(&remaining.Egress, &taken.Egress, &limit)
	distributeFunds(&remaining.RPC, &taken.RPC, &limit)
	return
}

// distributeFunds moves up to *limit from src into dst, decrementing both src
// and limit by the amount transferred.
func distributeFunds(src, dst, limit *types.Currency) {
	if limit.IsZero() || src.IsZero() {
		return
	}
	v := *src
	if v.Cmp(*limit) > 0 {
		v = *limit
	}
	*src = src.Sub(v)
	*dst = dst.Add(v)
	*limit = limit.Sub(v)
}
