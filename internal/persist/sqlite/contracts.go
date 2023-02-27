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

// setLastChangeID sets the last processed consensus change ID.
func (u *updateContractsTxn) setLastChangeID(ccID modules.ConsensusChangeID, height uint64) error {
	var dbID int64 // unused, but required by QueryRow to ensure exactly one row is updated
	err := u.tx.QueryRow(`UPDATE global_settings SET contracts_last_processed_change=$1, contracts_height=$2 RETURNING id`, sqlHash256(ccID), sqlUint64(height)).Scan(&dbID)
	return err
}

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

// ConfirmFormation sets the formation_confirmed flag to true.
func (u *updateContractsTxn) ConfirmFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=true WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// ConfirmRevision sets the confirmed revision number.
func (u *updateContractsTxn) ConfirmRevision(revision types.FileContractRevision) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2;`
	_, err := u.tx.Exec(query, sqlUint64(revision.RevisionNumber), sqlHash256(revision.ParentID))
	return err
}

// ConfirmResolution sets the resolution_confirmed flag to true.
func (u *updateContractsTxn) ConfirmResolution(id types.FileContractID) error {
	const query = `UPDATE contracts SET resolution_confirmed=true WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// RevertFormation sets the formation_confirmed flag to false.
func (u *updateContractsTxn) RevertFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=false WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// RevertRevision sets the confirmed revision number to 0.
func (u *updateContractsTxn) RevertRevision(id types.FileContractID) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2;`
	_, err := u.tx.Exec(query, sqlUint64(0), sqlHash256(id))
	return err
}

// RevertResolution sets the resolution_confirmed flag to false.
func (u *updateContractsTxn) RevertResolution(id types.FileContractID) error {
	const query = `UPDATE contracts SET resolution_confirmed=false WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	return err
}

// ContractRevelant returns true if the contract is relevant to the host.
func (u *updateContractsTxn) ContractRelevant(id types.FileContractID) (bool, error) {
	const query = `SELECT id FROM contracts WHERE contract_id=$1`
	var dbID int64
	err := u.tx.QueryRow(query, sqlHash256(id)).Scan(&dbID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Contracts returns a paginated list of contracts.
func (s *Store) Contracts(limit, offset int) ([]contracts.Contract, error) {
	const query = `SELECT c.contract_id, r.contract_id AS renewed_to, c.contract_error, 
	c.negotiation_height, c.formation_confirmed, c.revision_number=confirmed_revision_number AS revision_confirmed, 
	c.resolution_confirmed, c.locked_collateral, c.raw_revision, c.host_sig, c.renter_sig 
FROM contracts c
LEFT JOIN contracts r ON (c.renewed_to=r.id)
ORDER BY c.window_end ASC LIMIT $1 OFFSET $2;`
	rows, err := s.query(query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	var results []contracts.Contract
	for rows.Next() {
		contract, err := scanContract(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		results = append(results, contract)
	}
	return results, nil
}

// Contract returns the contract with the given ID.
func (s *Store) Contract(id types.FileContractID) (contracts.Contract, error) {
	const query = `SELECT c.contract_id, r.contract_id AS renewed_to, c.contract_error, c.negotiation_height, c.formation_confirmed, 
	c.revision_number=c.confirmed_revision_number AS revision_confirmed, c.resolution_confirmed, c.locked_collateral, 
	c.raw_revision, c.host_sig, c.renter_sig 
FROM contracts c
LEFT JOIN contracts r ON (c.id = r.renewed_to)
WHERE c.contract_id=$1;`
	row := s.queryRow(query, sqlHash256(id))
	return scanContract(row)
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
func (s *Store) UpdateContractState(ccID modules.ConsensusChangeID, height uint64, fn func(contracts.UpdateStateTransaction) error) error {
	return s.transaction(func(tx txn) error {
		utx := &updateContractsTxn{tx: tx}
		if err := fn(utx); err != nil {
			return err
		} else if err := utx.setLastChangeID(ccID, height); err != nil {
			return fmt.Errorf("failed to update last change id: %w", err)
		}
		return nil
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

func scanContract(row scanner) (c contracts.Contract, err error) {
	var revisionBuf []byte
	var errorStr sql.NullString
	var contractID types.FileContractID
	err = row.Scan((*sqlHash256)(&contractID),
		nullable((*sqlHash256)(&c.RenewedTo)),
		&errorStr,
		&c.NegotiationHeight,
		&c.FormationConfirmed,
		&c.RevisionConfirmed,
		&c.ResolutionConfirmed,
		(*sqlCurrency)(&c.LockedCollateral),
		&revisionBuf,
		(*sqlHash512)(&c.HostSignature),
		(*sqlHash512)(&c.RenterSignature),
	)
	if err != nil {
		return contracts.Contract{}, fmt.Errorf("failed to scan contract: %w", err)
	} else if err := decodeRevision(revisionBuf, &c.Revision); err != nil {
		return contracts.Contract{}, fmt.Errorf("failed to decode revision: %w", err)
	} else if c.Revision.ParentID != contractID {
		panic("contract id mismatch")
	} else if errorStr.Valid {
		c.Error = errors.New(errorStr.String)
	}
	return
}
