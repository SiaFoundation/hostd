package sqlite

import (
	"bytes"
	"fmt"

	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

type updateContractTxn struct {
	tx txn
}

// ReviseContract updates the current revision associated with a contract.
func (u *updateContractTxn) ReviseContract(revision types.FileContractRevision, hostSig, renterSig []byte) error {
	const query = `UPDATE contracts SET (revision_number, window_start, window_end, raw_revision) = ($1, $2, $3, $4) WHERE contract_id=$5;`
	var buf bytes.Buffer
	if err := revision.MarshalSia(&buf); err != nil {
		return fmt.Errorf("failed to encode revision: %w", err)
	}
	_, err := u.tx.Exec(query, revision.NewRevisionNumber, revision.NewWindowStart, revision.NewWindowEnd, buf.Bytes(), valueHash(revision.ParentID))
	return err
}

// SwapSectors swaps the sector roots at the given indices.
func (u *updateContractTxn) SwapSectors(contractID types.FileContractID, i, j uint64) error {
	var root1ID int64
	// clear the first index to satisfy the unique constraint
	err := u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=-1 WHERE contract_id=$1 AND root_index=$2 RETURNING id;`, valueHash(contractID), i).Scan(&root1ID)
	if err != nil {
		return fmt.Errorf("failed to clear sector %v: %w", i, err)
	}
	// update the second index
	var root2ID int64
	err = u.tx.QueryRow(`UPDATE contract_sector_roots SET root_index=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING id;`, i, valueHash(contractID), j).Scan(&root2ID)
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

// AppendSector appends a sector root to the end of the contract
func (u *updateContractTxn) AppendSector(contractID types.FileContractID, root crypto.Hash) error {
	const query = `INSERT INTO contract_sector_roots (contract_id, sector_root, root_index) SELECT $1, $2, COALESCE(MAX(root_index) + 1, 0) FROM contract_sector_roots WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, valueHash(contractID), valueHash(root))
	return err
}

// UpdateSector updates the sector root at the given index.
func (u *updateContractTxn) UpdateSector(contractID types.FileContractID, index uint64, root crypto.Hash) error {
	const query = `UPDATE contract_sector_roots SET sector_root=$1 WHERE contract_id=$2 AND root_index=$3;`
	_, err := u.tx.Exec(query, valueHash(root), valueHash(contractID), index)
	return err
}

// TrimSectors removes the last n sector roots from the contract.
func (u *updateContractTxn) TrimSectors(contractID types.FileContractID, n uint64) error {
	var maxIndex uint64
	err := u.tx.QueryRow(`SELECT COALESCE(MAX(root_index), 0) FROM contract_sector_roots WHERE contract_id=$1;`, valueHash(contractID)).Scan(&maxIndex)
	if err != nil {
		return fmt.Errorf("failed to get max index: %w", err)
	} else if n > maxIndex {
		return fmt.Errorf("cannot trim %v sectors from contract with %v sectors", n, maxIndex)
	}
	_, err = u.tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1 AND root_index > $2;`, valueHash(contractID), maxIndex-n)
	return err
}

// RenewContract adds a new contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract.
func (u *updateContractTxn) RenewContract(parentID types.FileContractID, renewed types.FileContractRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64, hostSig, renterSig []byte) error {
	// add the new contract
	const addQuery = `INSERT INTO contracts (id, renewed_from, locked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, raw_revision, host_sig, renter_sig) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`
	var revisionBuf bytes.Buffer
	if err := renewed.MarshalSia(&revisionBuf); err != nil {
		return fmt.Errorf("failed to encode revision: %w", err)
	}
	// TODO: encode txn set
	_, err := u.tx.Exec(addQuery, valueHash(renewed.ParentID), valueHash(parentID), valueCurrency(lockedCollateral), valueUint64(renewed.NewRevisionNumber), valueUint64(negotationHeight), valueUint64(uint64(renewed.NewWindowStart)), valueUint64(uint64(renewed.NewWindowEnd)), revisionBuf.Bytes(), []byte{}, hostSig, renterSig)
	if err != nil {
		return fmt.Errorf("failed to add renewed contract: %w", err)
	}

	// update the old contract's renewed_to field
	const renewedToQuery = `UPDATE contracts SET renewed_to=$1 WHERE contract_id=$2;`
	if _, err := u.tx.Exec(renewedToQuery, valueHash(renewed.ParentID), valueHash(parentID)); err != nil {
		return fmt.Errorf("failed to update renewed_to field: %w", err)
	}

	// copy the sector roots from the old contract to the new contract
	const transferQuery = `INSERT INTO contract_sector_roots (contract_id, sector_root, root_index) SELECT $1, sector_root, root_index FROM contract_sector_roots WHERE contract_id=$2;`
	_, err = u.tx.Exec(transferQuery, valueHash(renewed.ParentID), valueHash(parentID))
	return err
}

// UpdateContract atomically updates a contract and its sector roots.
func (s *Store) UpdateContracts(fn func(contracts.UpdateContractTransaction) error) error {
	return s.exclusiveTransaction(func(tx txn) error {
		return fn(&updateContractTxn{tx})
	})
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision types.FileContractRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64, hostSig, renterSig []byte) error {
	const query = `INSERT INTO contracts (id, locked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, raw_revision, host_sig, renter_sig) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`
	var revisionBuf bytes.Buffer
	if err := revision.MarshalSia(&revisionBuf); err != nil {
		return fmt.Errorf("failed to encode revision: %w", err)
	}
	// TODO: encode txn set
	_, err := s.db.Exec(query, valueHash(revision.ParentID), valueCurrency(lockedCollateral), revision.NewRevisionNumber, negotationHeight, revision.NewWindowStart, revision.NewWindowEnd, revisionBuf.Bytes(), []byte{}, hostSig, renterSig)
	return err
}

func (s *Store) SectorRoots(contractID types.FileContractID, offset, limit uint64) ([]crypto.Hash, error) {
	const query = `SELECT sector_root FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index ASC LIMIT $2 OFFSET $3;`
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
