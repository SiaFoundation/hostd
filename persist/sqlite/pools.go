package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type attachedPool struct {
	ID      int64
	Balance types.Currency
}

// RHP4PoolBalances returns the balances of the pools with the given IDs.
func (s *Store) RHP4PoolBalances(pools []proto4.Account) (balances []types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`SELECT balance FROM rhp4_pools WHERE pool_id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare get balance statement: %w", err)
		}
		defer stmt.Close()

		balances = make([]types.Currency, 0, len(pools))
		for _, pool := range pools {
			var balance types.Currency
			err := stmt.QueryRow(encode(pool)).Scan(decode(&balance))
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to get balance: %w", err)
			}
			balances = append(balances, balance)
		}
		return nil
	})
	return
}

// RHP4CreditPools credits the pools with the given deposits and revises the
// contract.
func (s *Store) RHP4CreditPools(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract, usage proto4.Usage) (balances []types.Currency, err error) {
	err = s.transaction(func(tx *txn) error {
		getBalanceStmt, err := tx.Prepare(`SELECT balance FROM rhp4_pools WHERE pool_id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare get balance statement: %w", err)
		}
		defer getBalanceStmt.Close()

		updateBalanceStmt, err := tx.Prepare(`INSERT INTO rhp4_pools (pool_id, balance) VALUES ($1, $2) ON CONFLICT (pool_id) DO UPDATE SET balance=EXCLUDED.balance RETURNING id`)
		if err != nil {
			return fmt.Errorf("failed to prepare update balance statement: %w", err)
		}
		defer updateBalanceStmt.Close()

		getFundingAmountStmt, err := tx.Prepare(`SELECT amount FROM contract_v2_pool_funding WHERE contract_id=$1 AND pool_id=$2`)
		if err != nil {
			return fmt.Errorf("failed to prepare get funding amount statement: %w", err)
		}
		defer getFundingAmountStmt.Close()

		updateFundingAmountStmt, err := tx.Prepare(`INSERT INTO contract_v2_pool_funding (contract_id, pool_id, amount) VALUES ($1, $2, $3) ON CONFLICT (contract_id, pool_id) DO UPDATE SET amount=EXCLUDED.amount`)
		if err != nil {
			return fmt.Errorf("failed to prepare update funding amount statement: %w", err)
		}
		defer updateFundingAmountStmt.Close()

		var contractDBID int64
		err = tx.QueryRow(`SELECT id FROM contracts_v2 WHERE contract_id=$1`, encode(contractID)).Scan(&contractDBID)
		if err != nil {
			return fmt.Errorf("failed to get contract ID: %w", err)
		}

		for _, deposit := range deposits {
			var balance types.Currency
			if err := getBalanceStmt.QueryRow(encode(deposit.Account)).Scan(decode(&balance)); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to get balance: %w", err)
			}

			balance = balance.Add(deposit.Amount)

			var poolDBID int64
			if err := updateBalanceStmt.QueryRow(encode(deposit.Account), encode(balance)).Scan(&poolDBID); err != nil {
				return fmt.Errorf("failed to update balance: %w", err)
			}
			balances = append(balances, balance)

			var fundAmount types.Currency
			if err := getFundingAmountStmt.QueryRow(contractDBID, poolDBID).Scan(decode(&fundAmount)); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to get funding amount: %w", err)
			} else if _, err := updateFundingAmountStmt.Exec(contractDBID, poolDBID, encode(fundAmount.Add(deposit.Amount))); err != nil {
				return fmt.Errorf("failed to update funding amount: %w", err)
			}
		}

		if _, err := reviseV2Contract(tx, contractID, revision, usage); err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		}
		return nil
	})
	return
}

// RHP4AttachPools attaches accounts to pools atomically.
func (s *Store) RHP4AttachPools(attachments []proto4.PoolAttachment) error {
	return s.transaction(func(tx *txn) error {
		getPoolStmt, err := tx.Prepare(`SELECT id FROM rhp4_pools WHERE pool_id=$1`)
		if err != nil {
			return fmt.Errorf("failed to prepare get pool statement: %w", err)
		}
		defer getPoolStmt.Close()

		poolIDs := make([]int64, len(attachments))
		for i, a := range attachments {
			err := getPoolStmt.QueryRow(encode(a.Pool)).Scan(&poolIDs[i])
			if errors.Is(err, sql.ErrNoRows) {
				return proto4.ErrPoolNotFound
			} else if err != nil {
				return fmt.Errorf("failed to get pool: %w", err)
			}
		}

		upsertAccountStmt, err := tx.Prepare(`INSERT INTO accounts (account_id, balance, expiration_timestamp) VALUES ($1, $2, $3) ON CONFLICT (account_id) DO UPDATE SET account_id=EXCLUDED.account_id RETURNING id`)
		if err != nil {
			return fmt.Errorf("failed to prepare upsert account statement: %w", err)
		}
		defer upsertAccountStmt.Close()

		insertAttachmentStmt, err := tx.Prepare(`INSERT INTO rhp4_account_pool_attachments (account_id, pool_id) VALUES ($1, $2) ON CONFLICT (account_id, pool_id) DO NOTHING`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert attachment statement: %w", err)
		}
		defer insertAttachmentStmt.Close()

		now := time.Now()
		for i, a := range attachments {
			var accountDBID int64
			if err := upsertAccountStmt.QueryRow(encode(a.Account), encode(types.ZeroCurrency), encode(now.Add(accountExpirationTime))).Scan(&accountDBID); err != nil {
				return fmt.Errorf("failed to upsert account: %w", err)
			} else if _, err := insertAttachmentStmt.Exec(accountDBID, poolIDs[i]); err != nil {
				return fmt.Errorf("failed to insert attachment: %w", err)
			}
		}
		return nil
	})
}

// RHP4DetachPools detaches accounts from pools atomically.
func (s *Store) RHP4DetachPools(detachments []proto4.PoolDetachment) error {
	return s.transaction(func(tx *txn) error {
		stmt, err := tx.Prepare(`DELETE FROM rhp4_account_pool_attachments
WHERE account_id=(SELECT id FROM accounts WHERE account_id=$1)
  AND pool_id=(SELECT id FROM rhp4_pools WHERE pool_id=$2)`)
		if err != nil {
			return fmt.Errorf("failed to prepare delete attachment statement: %w", err)
		}
		defer stmt.Close()

		for _, d := range detachments {
			if _, err := stmt.Exec(encode(d.Account), encode(d.Pool)); err != nil {
				return fmt.Errorf("failed to delete attachment: %w", err)
			}
		}
		return nil
	})
}

func attachedPools(tx *txn, accountDBID int64) (pools []attachedPool, err error) {
	rows, err := tx.Query(`SELECT p.id, p.balance FROM rhp4_account_pool_attachments a
INNER JOIN rhp4_pools p ON p.id=a.pool_id
WHERE a.account_id=$1
ORDER BY a.id ASC`, accountDBID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p attachedPool
		if err := rows.Scan(&p.ID, decode(&p.Balance)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		pools = append(pools, p)
	}
	return pools, rows.Err()
}

func contractV2PoolFunding(tx *txn, poolDBID int64) (fund []fundAmount, err error) {
	rows, err := tx.Query(`SELECT id, contract_id, amount FROM contract_v2_pool_funding WHERE pool_id=$1`, poolDBID)
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
	return fund, rows.Err()
}

func distributeRHP4PoolUsage(tx *txn, poolDBID int64, usage proto4.Usage) error {
	funding, err := contractV2PoolFunding(tx, poolDBID)
	if err != nil {
		return fmt.Errorf("failed to get pool funding: %w", err)
	}

	for _, f := range funding {
		remainder := f.Amount
		var additionalUsage proto4.Usage
		distributeFunds(&usage.Storage, &additionalUsage.Storage, &remainder)
		distributeFunds(&usage.Ingress, &additionalUsage.Ingress, &remainder)
		distributeFunds(&usage.Egress, &additionalUsage.Egress, &remainder)
		distributeFunds(&usage.RPC, &additionalUsage.RPC, &remainder)

		if remainder.IsZero() {
			if _, err := tx.Exec(`DELETE FROM contract_v2_pool_funding WHERE id=$1`, f.ID); err != nil {
				return fmt.Errorf("failed to delete pool funding: %w", err)
			}
		} else if _, err := tx.Exec(`UPDATE contract_v2_pool_funding SET amount=$1 WHERE id=$2`, encode(remainder), f.ID); err != nil {
			return fmt.Errorf("failed to update pool funding: %w", err)
		}

		var contractExistingFunding types.Currency
		if err := tx.QueryRow(`SELECT account_funding FROM contracts_v2 WHERE id=$1`, f.ContractID).Scan(decode(&contractExistingFunding)); err != nil {
			return fmt.Errorf("failed to get contract account funding: %w", err)
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
