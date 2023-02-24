package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/siad/modules"
)

func encodeRevision(fcr types.FileContractRevision) []byte {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	fcr.EncodeTo(e)
	e.Flush()
	return buf.Bytes()
}

func decodeRevision(b []byte, fcr *types.FileContractRevision) error {
	d := types.NewBufDecoder(b)
	fcr.DecodeFrom(d)
	return d.Err()
}

func encodeTxnSet(txns []types.Transaction) []byte {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	e.WritePrefix(len(txns))
	for i := range txns {
		txns[i].EncodeTo(e)
	}
	e.Flush()
	return buf.Bytes()
}

func decodeTxnSet(b []byte, txns *[]types.Transaction) error {
	d := types.NewBufDecoder(b)
	*txns = make([]types.Transaction, d.ReadPrefix())
	for i := range *txns {
		(*txns)[i].DecodeFrom(d)
	}
	return d.Err()
}

type (
	// An updateContractTxn atomically updates a single contract and its
	// associated sector roots.
	updateContractTxn struct {
		contractDBID int64
		tx           txn
	}

	// An updateContractsTxn atomically updates the contract manager's state
	updateContractsTxn struct {
		tx txn
	}
)

// AppendSector appends a sector root to the end of the contract
func (u *updateContractTxn) AppendSector(root types.Hash256) error {
	const query = `INSERT INTO contract_sector_roots (contract_id, sector_root, root_index) SELECT $1, $2, COALESCE(MAX(root_index) + 1, 0) FROM contract_sector_roots WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, u.contractDBID, sqlHash256(root))
	return err
}

// ReviseContract updates the current revision associated with a contract.
func (u *updateContractTxn) ReviseContract(revision contracts.SignedRevision) error {
	const query = `UPDATE contracts SET (revision_number, window_start, window_end, raw_revision, host_sig, renter_sig) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING contract_id;`
	var updatedID types.FileContractID
	err := u.tx.QueryRow(query,
		revision.Revision.RevisionNumber,
		revision.Revision.WindowStart,
		revision.Revision.WindowEnd,
		encodeRevision(revision.Revision),
		sqlHash512(revision.HostSignature),
		sqlHash512(revision.RenterSignature),
		u.contractDBID,
	).Scan((*sqlHash256)(&updatedID))
	if err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	} else if updatedID != revision.Revision.ParentID {
		panic("contract ID mismatch")
	}
	return nil
}

// SwapSectors swaps the sector roots at the given indices.
func (u *updateContractTxn) SwapSectors(i, j uint64) error {
	var root1ID int64
	// clear the first index to satisfy the unique constraint
	err := u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=-1 WHERE contract_id=$1 AND root_index=$2 RETURNING id;`, u.contractDBID, i).Scan(&root1ID)
	if err != nil {
		return fmt.Errorf("failed to clear sector %v: %w", i, err)
	}
	// update the second index
	var root2ID int64
	err = u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING id;`, i, u.contractDBID, j).Scan(&root2ID)
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
func (u *updateContractTxn) UpdateSector(index uint64, root types.Hash256) error {
	const query = `UPDATE contract_sector_roots SET sector_root=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING id;`
	var dbID int64
	return u.tx.QueryRow(query, sqlHash256(root), u.contractDBID, index).Scan(&dbID)
}

// TrimSectors removes the last n sector roots from the contract.
func (u *updateContractTxn) TrimSectors(n uint64) error {
	var maxIndex uint64
	err := u.tx.QueryRow(`SELECT COALESCE(MAX(root_index), 0) FROM contract_sector_roots WHERE contract_id=$1;`, u.contractDBID).Scan(&maxIndex)
	if err != nil {
		return fmt.Errorf("failed to get max index: %w", err)
	} else if n > maxIndex {
		return fmt.Errorf("cannot trim %v sectors from contract with %v sectors", n, maxIndex)
	}
	_, err = u.tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1 AND root_index > $2;`, u.contractDBID, maxIndex-n)
	return err
}

// ApplyContractFormation sets the formation_confirmed flag to true.
func (u *updateContractsTxn) ApplyContractFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=true WHERE contract_id=$2;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// ApplyFinalRevision sets the confirmed revision number.
func (u *updateContractsTxn) ApplyFinalRevision(id types.FileContractID, revision types.FileContractRevision) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2;`
	_, err := u.tx.Exec(query, sqlUint64(revision.RevisionNumber), sqlHash256(id))
	return err
}

// ApplyContractResolution sets the resolution_confirmed flag to true.
func (u *updateContractsTxn) ApplyContractResolution(id types.FileContractID, sp types.StorageProof) error {
	const query = `UPDATE contracts SET resolution_confirmed=true WHERE id=$2;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// RevertFormationConfirmed sets the formation_confirmed flag to false.
func (u *updateContractsTxn) RevertFormationConfirmed(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=false WHERE id=$2;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// RevertFinalRevision sets the confirmed revision number to 0.
func (u *updateContractsTxn) RevertFinalRevision(id types.FileContractID) error {
	const query = `UPDATE contracts SET confirmed_revision_number="0" WHERE id=$2;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// RevertContractResolution sets the resolution_confirmed flag to false.
func (u *updateContractsTxn) RevertContractResolution(id types.FileContractID) error {
	const query = `UPDATE contracts SET resolution_confirmed=false WHERE id=$2;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// SetLastChangeID sets the last processed consensus change ID.
func (u *updateContractsTxn) SetLastChangeID(ccID modules.ConsensusChangeID) error {
	var dbID int64 // unused, but required by QueryRow to ensure exactly one row is updated
	err := u.tx.QueryRow(`UPDATE global_settings SET contracts_last_processed_change=$1 RETURNING id`, sqlHash256(ccID)).Scan(&dbID)
	return err
}

// Contracts returns a paginated list of contracts.
func (s *Store) Contracts(limit, offset int) ([]contracts.Contract, error) {
	const query = `SELECT contract_error, negotiation_height, formation_confirmed, revision_number=confirmed_revision_number AS revision_confirmed, resolution_confirmed, locked_collateral, raw_revision, host_sig, renter_sig FROM contracts ORDER BY window_end ASC LIMIT $1 OFFSET $2;`
	rows, err := s.query(query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	var results []contracts.Contract
	var revisionBuf []byte
	for rows.Next() {
		revisionBuf = revisionBuf[:0]
		var contract contracts.Contract
		var errorStr sql.NullString

		err = rows.Scan(&errorStr,
			&contract.NegotiationHeight,
			&contract.FormationConfirmed,
			&contract.RevisionConfirmed,
			&contract.ResolutionConfirmed,
			(*sqlCurrency)(&contract.LockedCollateral),
			&revisionBuf,
			(*sqlHash512)(&contract.HostSignature),
			(*sqlHash512)(&contract.RenterSignature))
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}

		if errorStr.Valid {
			contract.Error = errors.New(errorStr.String)
		}

		if err := decodeRevision(revisionBuf, &contract.Revision); err != nil {
			return nil, fmt.Errorf("failed to decode revision: %w", err)
		}
		results = append(results, contract)
	}
	return results, nil
}

// Contract returns the contract with the given ID.
func (s *Store) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	var revisionBuf []byte
	const query = `SELECT contract_id, contract_error, negotiation_height, formation_confirmed, revision_number=confirmed_revision_number AS revision_confirmed, resolution_confirmed, locked_collateral, raw_revision, host_sig, renter_sig FROM contracts WHERE contract_id=$1;`
	var contractID [32]byte
	var errorStr sql.NullString
	err = s.queryRow(query,
		sqlHash256(id)).Scan((*sqlHash256)(&contractID),
		&errorStr,
		&contract.NegotiationHeight,
		&contract.FormationConfirmed,
		&contract.RevisionConfirmed,
		&contract.ResolutionConfirmed,
		(*sqlCurrency)(&contract.LockedCollateral),
		&revisionBuf,
		(*sqlHash512)(&contract.HostSignature),
		(*sqlHash512)(&contract.RenterSignature),
	)
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
	if err := decodeRevision(revisionBuf, &contract.Revision); err != nil {
		return contract, fmt.Errorf("failed to decode revision: %w", err)
	} else if contract.Revision.ParentID != id {
		panic("contract data corruption: revision parent id does not match contract id")
	}
	return contract, nil
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	_, err := insertContract(&dbTxn{s}, revision, formationSet, lockedCollateral, negotationHeight)
	return err
}

// RenewContract adds a new contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract.
func (s *Store) RenewContract(renewal contracts.SignedRevision, existing contracts.SignedRevision, renewalTxnSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error {
	return s.transaction(func(tx txn) error {
		// add the new contract
		createdDBID, err := insertContract(tx, renewal, renewalTxnSet, lockedCollateral, negotationHeight)
		if err != nil {
			return fmt.Errorf("failed to insert renewed contract: %w", err)
		}

		// update the existing contract
		const clearQuery = `UPDATE contracts SET (renewed_to, revision_number, host_sig, renter_sig, raw_revision) = ($1, $2, $3, $4, $5) WHERE contract_id=$6 RETURNING id;`
		var renewedDBID int64
		err = tx.QueryRow(clearQuery,
			createdDBID,
			sqlUint64(existing.Revision.RevisionNumber),
			sqlHash512(existing.HostSignature),
			sqlHash512(existing.RenterSignature),
			encodeRevision(existing.Revision),
			sqlHash256(existing.Revision.ParentID),
		).Scan(&renewedDBID)
		if err != nil {
			return fmt.Errorf("failed to update existing contract: %w", err)
		}

		// move the sector roots from the old contract to the new contract
		const transferQuery = `UPDATE contract_sector_roots SET contract_id=$1 WHERE contract_id=$2;`
		_, err = tx.Exec(transferQuery, createdDBID, renewedDBID)
		return err
	})
}

// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
// are returned.
func (s *Store) SectorRoots(contractID types.FileContractID, offset, limit uint64) ([]types.Hash256, error) {
	var dbID int64
	err := s.queryRow(`SELECT id FROM contracts WHERE contract_id=$1;`, sqlHash256(contractID)).Scan(&dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract id: %w", err)
	}

	var query string
	if limit <= 0 {
		query = `SELECT sector_root FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index ASC;`
	} else {
		query = `SELECT sector_root FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index ASC LIMIT $2 OFFSET $3;`
	}

	rows, err := s.query(query, dbID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query sector roots: %w", err)
	}
	defer rows.Close()

	var roots []types.Hash256
	for rows.Next() {
		var root types.Hash256
		if err := rows.Scan((*sqlHash256)(&root)); err != nil {
			return nil, fmt.Errorf("failed to scan sector root: %w", err)
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
	err := s.queryRow(`SELECT formation_txn_set FROM contracts WHERE id=$1;`, sqlHash256(id)).Scan(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to query formation txn set: %w", err)
	}
	var txnSet []types.Transaction
	if err := decodeTxnSet(buf, &txnSet); err != nil {
		return nil, fmt.Errorf("failed to decode formation txn set: %w", err)
	}
	return txnSet, nil
}

// LastContractChange gets the last consensus change processed by the
// contractor.
func (s *Store) LastContractChange() (id modules.ConsensusChangeID, err error) {
	err = s.queryRow(`SELECT contracts_last_processed_change FROM global_settings`).Scan(nullable((*sqlHash256)(&id)))
	if errors.Is(err, sql.ErrNoRows) {
		return modules.ConsensusChangeBeginning, nil
	} else if err != nil {
		return modules.ConsensusChangeBeginning, fmt.Errorf("failed to query last contract change: %w", err)
	}
	return
}

// UpdateContract atomically updates a contract and its sector roots.
func (s *Store) UpdateContract(id types.FileContractID, fn func(contracts.UpdateContractTransaction) error) error {
	return s.transaction(func(tx txn) error {
		var dbID int64
		err := tx.QueryRow(`SELECT id FROM contracts WHERE contract_id=$1;`, sqlHash256(id)).Scan(&dbID)
		if err != nil {
			return fmt.Errorf("failed to get contract: %w", err)
		}
		return fn(&updateContractTxn{
			contractDBID: dbID,
			tx:           tx,
		})
	})
}

// UpdateContractState atomically updates the contractor's state.
func (s *Store) UpdateContractState(fn func(contracts.UpdateStateTransaction) error) error {
	return s.transaction(func(tx txn) error {
		return fn(&updateContractsTxn{tx: tx})
	})
}

func insertContract(tx txn, revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) (dbID int64, err error) {
	const query = `INSERT INTO contracts (contract_id, locked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, raw_revision, host_sig, renter_sig, confirmed_revision_number, formation_confirmed, resolution_confirmed) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id;`
	err = tx.QueryRow(query,
		sqlHash256(revision.Revision.ParentID),
		sqlCurrency(lockedCollateral),
		sqlUint64(revision.Revision.RevisionNumber),
		negotationHeight,              // stored as int64 for queries, should never overflow
		revision.Revision.WindowStart, // stored as int64 for queries, should never overflow
		revision.Revision.WindowEnd,   // stored as int64 for queries, should never overflow
		encodeTxnSet(formationSet),
		encodeRevision(revision.Revision),
		sqlHash512(revision.HostSignature),
		sqlHash512(revision.RenterSignature),
		sqlUint64(0), // confirmed_revision_number
		false,        // formation_confirmed
		false,        // resolution_confirmed
	).Scan(&dbID)
	return
}
