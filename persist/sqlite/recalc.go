package sqlite

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func checkContractAccountFunding(tx txn, log *zap.Logger) error {
	rows, err := tx.Query(`SELECT contract_id, amount FROM contract_account_funding`)
	if err != nil {
		return fmt.Errorf("failed to query contract account funding: %w", err)
	}
	defer rows.Close()

	contractFunding := make(map[int64]types.Currency)
	for rows.Next() {
		var contractID int64
		var amount types.Currency
		if err := rows.Scan(&contractID, (*sqlCurrency)(&amount)); err != nil {
			return fmt.Errorf("failed to scan contract account funding: %w", err)
		}
		contractFunding[contractID] = contractFunding[contractID].Add(amount)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate contract account funding: %w", err)
	} else if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close contract account funding: %w", err)
	}

	for contractID, amount := range contractFunding {
		var actualAmount types.Currency
		err := tx.QueryRow(`SELECT account_funding FROM contracts WHERE id=$1`, contractID).Scan((*sqlCurrency)(&actualAmount))
		if err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		}

		if !actualAmount.Equals(amount) {
			log.Debug("incorrect contract account funding", zap.Int64("contractID", contractID), zap.Stringer("expected", amount), zap.Stringer("actual", actualAmount))
		}
	}
	return nil
}

func recalcContractAccountFunding(tx txn, _ *zap.Logger) error {
	rows, err := tx.Query(`SELECT contract_id, amount FROM contract_account_funding`)
	if err != nil {
		return fmt.Errorf("failed to query contract account funding: %w", err)
	}
	defer rows.Close()

	contractFunding := make(map[int64]types.Currency)
	for rows.Next() {
		var contractID int64
		var amount types.Currency
		if err := rows.Scan(&contractID, (*sqlCurrency)(&amount)); err != nil {
			return fmt.Errorf("failed to scan contract account funding: %w", err)
		}
		contractFunding[contractID] = contractFunding[contractID].Add(amount)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate contract account funding: %w", err)
	} else if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close contract account funding: %w", err)
	}

	for contractID, amount := range contractFunding {
		res, err := tx.Exec(`UPDATE contracts SET account_funding=$1 WHERE id=$2`, sqlCurrency(amount), contractID)
		if err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		} else if rowsAffected, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to query contract account funding: %w", err)
		} else if rowsAffected != 1 {
			return fmt.Errorf("failed to update contract account funding: %w", err)
		}
	}
	return nil
}

// CheckContractAccountFunding checks that the contract account funding table
// is correct.
func (s *Store) CheckContractAccountFunding() error {
	return s.transaction(func(tx txn) error {
		return checkContractAccountFunding(tx, s.log)
	})
}

// RecalcContractAccountFunding recalculates the contract account funding table.
func (s *Store) RecalcContractAccountFunding() error {
	return s.transaction(func(tx txn) error {
		return recalcContractAccountFunding(tx, s.log)
	})
}

// Vacuum runs the VACUUM command on the database.
func (s *Store) Vacuum() error {
	_, err := s.db.Exec(`VACUUM`)
	return err
}
