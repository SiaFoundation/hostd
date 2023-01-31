package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// An updateContractTxn atomically updates a single contract and its
	// associated sector roots.
	updateContractTxn struct {
		contractID types.FileContractID
		tx         txn
	}

	// An updateContractsTxn atomically updates the contract manager's state
	updateContractsTxn struct {
		tx txn
	}
)

// AppendSector appends a sector root to the end of the contract
func (u *updateContractTxn) AppendSector(root crypto.Hash) error {
	const query = `INSERT INTO contract_sector_roots (contract_id, sector_root, root_index) SELECT $1, $2, COALESCE(MAX(root_index) + 1, 0) FROM contract_sector_roots WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, valueHash(u.contractID), valueHash(root))
	return err
}

// ReviseContract updates the current revision associated with a contract.
func (u *updateContractTxn) ReviseContract(revision contracts.SignedRevision) error {
	const query = `UPDATE contracts SET (revision_number, window_start, window_end, raw_revision, host_sig, renter_sig) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING id;`
	var buf bytes.Buffer
	if err := revision.Revision.MarshalSia(&buf); err != nil {
		return fmt.Errorf("failed to encode revision: %w", err)
	}
	var updatedID [32]byte
	err := u.tx.QueryRow(query, revision.Revision.NewRevisionNumber, revision.Revision.NewWindowStart, revision.Revision.NewWindowEnd, buf.Bytes(), revision.HostSignature, revision.RenterSignature, valueHash(revision.Revision.ParentID)).Scan(scanHash(&updatedID))
	if err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	} else if updatedID != u.contractID {
		panic("contract ID mismatch")
	}
	return nil
}

// SwapSectors swaps the sector roots at the given indices.
func (u *updateContractTxn) SwapSectors(i, j uint64) error {
	var root1ID int64
	// clear the first index to satisfy the unique constraint
	err := u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=-1 WHERE contract_id=$1 AND root_index=$2 RETURNING id;`, valueHash(u.contractID), i).Scan(&root1ID)
	if err != nil {
		return fmt.Errorf("failed to clear sector %v: %w", i, err)
	}
	// update the second index
	var root2ID int64
	err = u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING id;`, i, valueHash(u.contractID), j).Scan(&root2ID)
	if err != nil {
		return fmt.Errorf("failed to update sector %v: %w", j, err)
	}
	// update the first index
	_, err = u.tx.Exec(`UPDATE contract_sector_roots SET root_index=$1 WHERE id=$2;`, j, root1ID)
	if err != nil {
		return fmt.Errorf("failed to update sector %v: %w", i, err)
	}
	return err
}

// UpdateSector updates the sector root at the given index.
func (u *updateContractTxn) UpdateSector(index uint64, root crypto.Hash) error {
	const query = `UPDATE contract_sector_roots SET sector_root=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING id;`
	var id uint64
	return u.tx.QueryRow(query, valueHash(root), valueHash(u.contractID), index).Scan(&id)
}

// TrimSectors removes the last n sector roots from the contract.
func (u *updateContractTxn) TrimSectors(n uint64) error {
	var maxIndex uint64
	err := u.tx.QueryRow(`SELECT COALESCE(MAX(root_index), 0) FROM contract_sector_roots WHERE contract_id=$1;`, valueHash(u.contractID)).Scan(&maxIndex)
	if err != nil {
		return fmt.Errorf("failed to get max index: %w", err)
	} else if n > maxIndex {
		return fmt.Errorf("cannot trim %v sectors from contract with %v sectors", n, maxIndex)
	}
	_, err = u.tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1 AND root_index > $2;`, valueHash(u.contractID), maxIndex-n)
	return err
}

// ApplyContractFormation sets the formation_confirmed flag to true.
func (u *updateContractsTxn) ApplyContractFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=true WHERE id=$2;`
	_, err := u.tx.Exec(query, valueHash(id))
	return err
}

// ApplyFinalRevision sets the confirmed revision number.
func (u *updateContractsTxn) ApplyFinalRevision(id types.FileContractID, revision types.FileContractRevision) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE id=$2;`
	_, err := u.tx.Exec(query, valueUint64(revision.NewRevisionNumber), valueHash(id))
	return err
}

// ApplyContractResolution sets the resolution_confirmed flag to true.
func (u *updateContractsTxn) ApplyContractResolution(id types.FileContractID, sp types.StorageProof) error {
	const query = `UPDATE contracts SET resolution_confirmed=true WHERE id=$2;`
	_, err := u.tx.Exec(query, valueHash(id))
	return err
}

// RevertFormationConfirmed sets the formation_confirmed flag to false.
func (u *updateContractsTxn) RevertFormationConfirmed(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=false WHERE id=$2;`
	_, err := u.tx.Exec(query, valueHash(id))
	return err
}

// RevertFinalRevision sets the confirmed revision number to 0.
func (u *updateContractsTxn) RevertFinalRevision(id types.FileContractID) error {
	const query = `UPDATE contracts SET confirmed_revision_number="0" WHERE id=$2;`
	_, err := u.tx.Exec(query, valueHash(id))
	return err
}

// RevertContractResolution sets the resolution_confirmed flag to false.
func (u *updateContractsTxn) RevertContractResolution(id types.FileContractID) error {
	const query = `UPDATE contracts SET resolution_confirmed=false WHERE id=$2;`
	_, err := u.tx.Exec(query, valueHash(id))
	return err
}

// SetLastChangeID sets the last processed consensus change ID.
func (u *updateContractsTxn) SetLastChangeID(ccID modules.ConsensusChangeID) error {
	const query = `INSERT INTO global_settings (contracts_last_processed_change) VALUES ($1) ON CONFLICT (id) DO UPDATE SET contracts_last_processed_change=exluded.contracts_last_processed_change;`
	_, err := u.tx.Exec(query, valueHash(ccID))
	return err
}

// Contract returns the contract with the given ID.
func (s *Store) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	var revisionBuf []byte
	const query = `SELECT id, contract_error, negotiation_height, formation_confirmed, revision_number=confirmed_revision_number AS revision_confirmed, resolution_confirmed, locked_collateral, raw_revision, host_sig, renter_sig FROM contracts WHERE id=$1;`
	var contractID [32]byte
	var errorStr sql.NullString
	err = s.db.QueryRow(query, valueHash(id)).Scan(scanHash(&contractID), &errorStr, &contract.NegotiationHeight, &contract.FormationConfirmed, &contract.RevisionConfirmed, &contract.ResolutionConfirmed, scanCurrency(&contract.LockedCollateral), &revisionBuf, &contract.HostSignature, &contract.RenterSignature)
	if errors.Is(err, sql.ErrNoRows) {
		return contract, contracts.ErrNotFound
	} else if err != nil {
		return contract, fmt.Errorf("failed to get contract: %w", err)
	} else if contractID != id {
		return contract, fmt.Errorf("contract id mismatch: expected %v but got %v", id, contractID)
	}
	if errorStr.Valid {
		contract.Error = errors.New(errorStr.String)
	}
	if err := contract.Revision.UnmarshalSia(bytes.NewReader(revisionBuf)); err != nil {
		return contract, fmt.Errorf("failed to decode revision: %w", err)
	} else if contract.Revision.ParentID != id {
		panic("contract data corruption: revision parent id does not match contract id")
	}
	return contract, nil
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	const query = `INSERT INTO contracts (id, locked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, raw_revision, host_sig, renter_sig) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;`
	var revisionBuf bytes.Buffer
	if err := revision.Revision.MarshalSia(&revisionBuf); err != nil {
		return fmt.Errorf("failed to encode revision: %w", err)
	}
	// encode the renewal txn set
	formationTxnBuf := encoding.Marshal(&formationSet)
	var id [32]byte
	err := s.db.QueryRow(query, valueHash(revision.Revision.ParentID), valueCurrency(lockedCollateral), valueUint64(revision.Revision.NewRevisionNumber), negotationHeight, revision.Revision.NewWindowStart, revision.Revision.NewWindowEnd, formationTxnBuf, revisionBuf.Bytes(), revision.HostSignature, revision.RenterSignature).Scan(scanHash(&id))
	if err != nil {
		return fmt.Errorf("failed to insert contract: %w", err)
	} else if id != revision.Revision.ParentID {
		return fmt.Errorf("contract id mismatch: %v != %v", id, revision.Revision.ParentID)
	}
	return nil
}

// RenewContract adds a new contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract.
func (s *Store) RenewContract(renewal contracts.SignedRevision, existing contracts.SignedRevision, renewalTxnSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	return s.exclusiveTransaction(func(tx txn) error {
		// update the existing contract
		const clearQuery = `UPDATE contracts SET (renewed_to, revision_number, host_sig, renter_sig, raw_revision) = ($1, $2, $3, $4, $5) WHERE id=$6 RETURNING id;`
		var clearingBuf bytes.Buffer
		if err := existing.Revision.MarshalSia(&clearingBuf); err != nil {
			return fmt.Errorf("failed to encode revision: %w", err)
		}
		var clearingID [32]byte
		err := tx.QueryRow(clearQuery, valueHash(renewal.Revision.ParentID), valueUint64(existing.Revision.NewRevisionNumber), renewal.HostSignature, renewal.RenterSignature, clearingBuf.Bytes(), valueHash(existing.Revision.ParentID)).Scan(scanHash(&clearingID))
		if err != nil {
			return fmt.Errorf("failed to update existing contract: %w", err)
		} else if clearingID != existing.Revision.ParentID {
			return fmt.Errorf("failed to clear existing contract: expected id %v, got %v", existing.Revision.ParentID, clearingID)
		}

		// add the new contract
		const renewQuery = `INSERT INTO contracts (id, renewed_from, locked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, raw_revision, host_sig, renter_sig) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id;`
		var revisionBuf bytes.Buffer
		if err := renewal.Revision.MarshalSia(&revisionBuf); err != nil {
			return fmt.Errorf("failed to encode revision: %w", err)
		}
		// encode the renewal txn set
		renewalTxnBuf := encoding.Marshal(&renewalTxnSet)
		var renewalID [32]byte
		err = tx.QueryRow(renewQuery, valueHash(renewal.Revision.ParentID), valueHash(existing.Revision.ParentID), valueCurrency(lockedCollateral), valueUint64(renewal.Revision.NewRevisionNumber), valueUint64(negotationHeight), valueUint64(uint64(renewal.Revision.NewWindowStart)), valueUint64(uint64(renewal.Revision.NewWindowEnd)), renewalTxnBuf, revisionBuf.Bytes(), renewal.HostSignature, renewal.RenterSignature).Scan(scanHash(&renewalID))
		if err != nil {
			return fmt.Errorf("failed to add renewed contract: %w", err)
		} else if renewalID != renewal.Revision.ParentID {
			return fmt.Errorf("failed to add renewed contract: expected id %v, got %v", renewal.Revision.ParentID, renewalID)
		}

		// copy the sector roots from the old contract to the new contract
		const transferQuery = `INSERT INTO contract_sector_roots (contract_id, sector_root, root_index) SELECT $1, sector_root, root_index FROM contract_sector_roots WHERE contract_id=$2;`
		_, err = tx.Exec(transferQuery, valueHash(renewal.Revision.ParentID), valueHash(existing.Revision.ParentID))
		return err
	})
}

// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
// are returned.
func (s *Store) SectorRoots(contractID types.FileContractID, offset, limit uint64) ([]crypto.Hash, error) {
	var query string
	if limit <= 0 {
		query = `SELECT sector_root FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index ASC;`
	} else {
		query = `SELECT sector_root FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index ASC LIMIT $2 OFFSET $3;`
	}

	rows, err := s.db.Query(query, valueHash(contractID), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roots []crypto.Hash
	for rows.Next() {
		var root crypto.Hash
		if err := rows.Scan(scanHash((*[32]byte)(&root))); err != nil {
			return nil, err
		}
		roots = append(roots, root)
	}
	return roots, nil
}

// ContractAction calls contractFn on every contract in the store that
// needs a lifecycle action performed.
func (s *Store) ContractAction(cc *modules.ConsensusChange, contractFn func(types.FileContractID, contracts.LifecycleAction) error) error {
	return nil
}

// ContractFormationSet returns the set of transactions that were created during
// contract formation.
func (s *Store) ContractFormationSet(id types.FileContractID) ([]types.Transaction, error) {
	var buf []byte
	err := s.db.QueryRow(`SELECT formation_txn_set FROM contracts WHERE id=$1;`, valueHash(id)).Scan(&buf)
	if err != nil {
		return nil, err
	}
	var txnSet []types.Transaction
	if err := encoding.Unmarshal(buf, &txnSet); err != nil {
		return nil, err
	}
	return txnSet, nil
}

// LastContractChange gets the last consensus change processed by the
// contractor.
func (s *Store) LastContractChange() (id modules.ConsensusChangeID, err error) {
	err = s.db.QueryRow(`SELECT contracts_last_processed_change FROM global_settings`).Scan(scanHash((*[32]byte)(&id)))
	if errors.Is(err, sql.ErrNoRows) {
		return modules.ConsensusChangeBeginning, nil
	}
	return
}

// UpdateContract atomically updates a contract and its sector roots.
func (s *Store) UpdateContract(id types.FileContractID, fn func(contracts.UpdateContractTransaction) error) error {
	return s.exclusiveTransaction(func(tx txn) error {
		return fn(&updateContractTxn{
			contractID: id,
			tx:         tx,
		})
	})
}

// UpdateContractState atomically updates the contractor's state.
func (s *Store) UpdateContractState(fn func(contracts.UpdateStateTransaction) error) error {
	return s.exclusiveTransaction(func(tx txn) error {
		return fn(&updateContractsTxn{tx: tx})
	})
}
