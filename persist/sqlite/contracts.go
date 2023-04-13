package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
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

	// A contractAction pairs a contract's ID with a lifecycle action.
	contractAction struct {
		ID     types.FileContractID
		Action string
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
	var nextIndex int64
	err := u.tx.QueryRow(`SELECT COALESCE(MAX(root_index)+1, 0) FROM contract_sector_roots WHERE contract_id=$1`, u.contractDBID).Scan(&nextIndex)
	if err != nil {
		return fmt.Errorf("failed to get next sector index: %w", err)
	}
	var sectorID int64
	err = u.tx.QueryRow(`INSERT INTO contract_sector_roots (contract_id, root_index, sector_id) SELECT $1, $2, id FROM stored_sectors WHERE sector_root=$3 RETURNING sector_id;`, u.contractDBID, nextIndex, sqlHash256(root)).Scan(&sectorID)
	if err != nil {
		return fmt.Errorf("failed to append sector: %w", err)
	} else if err := incrementNumericStat(u.tx, metricContractSectors, 1, time.Now()); err != nil {
		return fmt.Errorf("failed to track contract sectors: %w", err)
	}
	return nil
}

// ReviseContract updates the current revision associated with a contract.
func (u *updateContractTxn) ReviseContract(revision contracts.SignedRevision) error {
	const query = `UPDATE contracts SET (revision_number, window_start, window_end, raw_revision, host_sig, renter_sig) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING contract_id;`
	var updatedID types.FileContractID
	err := u.tx.QueryRow(query,
		sqlUint64(revision.Revision.RevisionNumber),
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

// AddUsage adds the revenue to the contract's existing revenue.
func (u *updateContractTxn) AddUsage(revenue contracts.Usage) error {
	const query = `SELECT rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral FROM contracts WHERE id=$1;`
	var total contracts.Usage
	err := u.tx.QueryRow(query, u.contractDBID).Scan(
		(*sqlCurrency)(&total.RPCRevenue),
		(*sqlCurrency)(&total.StorageRevenue),
		(*sqlCurrency)(&total.IngressRevenue),
		(*sqlCurrency)(&total.EgressRevenue),
		(*sqlCurrency)(&total.AccountFunding),
		(*sqlCurrency)(&total.RiskedCollateral))
	if err != nil {
		return fmt.Errorf("failed to get existing revenue: %w", err)
	}
	total = total.Add(revenue)

	var dbID int64 // unused, but required by QueryRow to ensure exactly one row is updated
	return u.tx.QueryRow(`UPDATE contracts SET (rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING id;`,
		sqlCurrency(total.RPCRevenue),
		sqlCurrency(total.StorageRevenue),
		sqlCurrency(total.IngressRevenue),
		sqlCurrency(total.EgressRevenue),
		sqlCurrency(total.AccountFunding),
		sqlCurrency(total.RiskedCollateral),
		u.contractDBID).Scan(&dbID)
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
	var sectorID int64
	err := u.tx.QueryRow(`SELECT id FROM stored_sectors WHERE sector_root=$1`, sqlHash256(root)).Scan(&sectorID)
	if err != nil {
		return fmt.Errorf("failed to get sector ID: %w", err)
	}
	var dbID int64
	return u.tx.QueryRow(`UPDATE contract_sector_roots SET sector_id=$1 WHERE contract_id=$2 AND root_index=$3 RETURNING sector_id`, sectorID, u.contractDBID, index).Scan(&dbID)
}

// TrimSectors removes the last n sector roots from the contract.
func (u *updateContractTxn) TrimSectors(n int) error {
	if n < 0 {
		panic("negative sector count")
	}
	var maxIndex uint64
	err := u.tx.QueryRow(`SELECT COALESCE(MAX(root_index), 0) FROM contract_sector_roots WHERE contract_id=$1;`, u.contractDBID).Scan(&maxIndex)
	if err != nil {
		return fmt.Errorf("failed to get max index: %w", err)
	} else if uint64(n) > maxIndex {
		return fmt.Errorf("cannot trim %v sectors from contract with %v sectors", n, maxIndex)
	}
	_, err = u.tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1 AND root_index > $2;`, u.contractDBID, maxIndex-uint64(n))
	if err != nil {
		return fmt.Errorf("failed to trim sectors: %w", err)
	} else if err := incrementNumericStat(u.tx, metricContractSectors, -n, time.Now()); err != nil {
		return fmt.Errorf("failed to update contract metric: %w", err)
	}
	return err
}

// SetStatus sets the contract's status.
func (u *updateContractsTxn) SetStatus(id types.FileContractID, status contracts.ContractStatus) error {
	return setContractStatus(u.tx, id, status)
}

// ConfirmFormation sets the formation_confirmed flag to true.
func (u *updateContractsTxn) ConfirmFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=true WHERE contract_id=$1;`
	_, err := u.tx.Exec(query, sqlHash256(id))
	if err != nil {
		return fmt.Errorf("failed to confirm formation: %w", err)
	}
	return err
}

// ConfirmRevision sets the confirmed revision number.
func (u *updateContractsTxn) ConfirmRevision(revision types.FileContractRevision) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2 RETURNING id;`
	var dbID int64
	return u.tx.QueryRow(query, sqlUint64(revision.RevisionNumber), sqlHash256(revision.ParentID)).Scan(&dbID)
}

// ConfirmResolution sets the resolution height.
func (u *updateContractsTxn) ConfirmResolution(id types.FileContractID, height uint64) error {
	const query = `UPDATE contracts SET resolution_height=$1 WHERE contract_id=$2 RETURNING id;`
	var dbID int64
	if err := u.tx.QueryRow(query, height, sqlHash256(id)).Scan(&dbID); err != nil {
		return fmt.Errorf("failed to confirm resolution: %w", err)
	}
	return nil
}

// RevertFormation sets the formation_confirmed flag to false.
func (u *updateContractsTxn) RevertFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=false WHERE contract_id=$1 RETURNING id;`
	var dbID int64
	if err := u.tx.QueryRow(query, sqlHash256(id)).Scan(&dbID); err != nil {
		return fmt.Errorf("failed to revert formation: %w", err)
	}
	return nil
}

// RevertRevision sets the confirmed revision number to 0.
func (u *updateContractsTxn) RevertRevision(id types.FileContractID) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2 RETURNING id;`
	var dbID int64
	return u.tx.QueryRow(query, sqlUint64(0), sqlHash256(id)).Scan(&dbID)
}

// RevertResolution sets the resolution height to null
func (u *updateContractsTxn) RevertResolution(id types.FileContractID) error {
	const query = `UPDATE contracts SET resolution_height=NULL WHERE contract_id=$1 RETURNING id;`
	var dbID int64
	if err := u.tx.QueryRow(query, sqlHash256(id)).Scan(&dbID); err != nil {
		return fmt.Errorf("failed to revert resolution: %w", err)
	}
	return nil
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
func (s *Store) Contracts(filter contracts.ContractFilter) ([]contracts.Contract, error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 100
	}

	whereClause, whereParams, err := buildContractFilter(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to build where clause: %w", err)
	}

	s.log.Debug("querying contracts", zap.String("clause", whereClause), zap.Any("params", append(whereParams, filter.Limit, filter.Offset)))

	query := fmt.Sprintf(`SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.formation_confirmed, 
	c.revision_number=c.confirmed_revision_number AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
	c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision, c.host_sig, c.renter_sig 
FROM contracts c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts rf ON (c.renewed_from=rf.id) %s %s LIMIT ? OFFSET ?`, whereClause, buildOrderBy(filter))
	rows, err := s.query(query, append(whereParams, filter.Limit, filter.Offset)...)
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
	const query = `SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.formation_confirmed, 
	c.revision_number=c.confirmed_revision_number AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
	c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision, c.host_sig, c.renter_sig 
FROM contracts c
LEFT JOIN contracts rt ON (c.renewed_to = rt.id)
LEFT JOIN contracts rf ON (c.renewed_from = rf.id)
WHERE c.contract_id=$1;`
	row := s.queryRow(query, sqlHash256(id))
	return scanContract(row)
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage, negotationHeight uint64) error {
	_, err := insertContract(&dbTxn{s}, revision, formationSet, lockedCollateral, initialUsage, negotationHeight)
	return err
}

func clearContract(tx txn, revision contracts.SignedRevision, renewedDBID int64, usage contracts.Usage) (dbID int64, err error) {
	// get the existing contract's current usage
	var total contracts.Usage
	err = tx.QueryRow(`SELECT id, rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral FROM contracts WHERE contract_id=$1`, sqlHash256(revision.Revision.ParentID)).Scan(
		&dbID,
		(*sqlCurrency)(&total.RPCRevenue),
		(*sqlCurrency)(&total.StorageRevenue),
		(*sqlCurrency)(&total.IngressRevenue),
		(*sqlCurrency)(&total.EgressRevenue),
		(*sqlCurrency)(&total.AccountFunding),
		(*sqlCurrency)(&total.RiskedCollateral))
	if err != nil {
		return 0, fmt.Errorf("failed to get existing usage: %w", err)
	}
	total = total.Add(usage)

	// update the existing contract
	const clearQuery = `UPDATE contracts SET (renewed_to, revision_number, host_sig, renter_sig, raw_revision, rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral) = ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) WHERE id=$12 RETURNING id;`
	err = tx.QueryRow(clearQuery,
		renewedDBID,
		sqlUint64(revision.Revision.RevisionNumber),
		sqlHash512(revision.HostSignature),
		sqlHash512(revision.RenterSignature),
		encodeRevision(revision.Revision),
		sqlCurrency(total.RPCRevenue),
		sqlCurrency(total.StorageRevenue),
		sqlCurrency(total.IngressRevenue),
		sqlCurrency(total.EgressRevenue),
		sqlCurrency(total.AccountFunding),
		sqlCurrency(total.RiskedCollateral),
		dbID,
	).Scan(&dbID)
	return
}

// RenewContract adds a new contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract.
func (s *Store) RenewContract(renewal contracts.SignedRevision, clearing contracts.SignedRevision, renewalTxnSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, renewalUsage contracts.Usage, negotationHeight uint64) error {
	return s.transaction(func(tx txn) error {
		// add the new contract
		renewedDBID, err := insertContract(tx, renewal, renewalTxnSet, lockedCollateral, renewalUsage, negotationHeight)
		if err != nil {
			return fmt.Errorf("failed to insert renewed contract: %w", err)
		}

		clearedDBID, err := clearContract(tx, clearing, renewedDBID, clearingUsage)
		if err != nil {
			return fmt.Errorf("faile to clear contract: %w", err)
		}

		err = tx.QueryRow(`UPDATE contracts SET renewed_from=$1 WHERE id=$2 RETURNING id;`, clearedDBID, renewedDBID).Scan(&renewedDBID)
		if err != nil {
			return fmt.Errorf("failed to update renewed contract: %w", err)
		}

		// copy the sector roots from the old contract to the new contract
		_, err = tx.Exec(`INSERT INTO contract_sector_roots (contract_id, sector_id, root_index) SELECT $1, sector_id, root_index FROM contract_sector_roots WHERE contract_id=$2;`, renewedDBID, clearedDBID)
		if err != nil {
			return fmt.Errorf("failed to copy sector roots: %w", err)
		}
		return nil
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
		query = `SELECT s.sector_root FROM contract_sector_roots c INNER JOIN stored_sectors s ON (c.sector_id = s.id) WHERE c.contract_id=$1 ORDER BY root_index ASC;`
	} else {
		query = `SELECT s.sector_root FROM contract_sector_roots c INNER JOIN stored_sectors s ON (c.sector_id = s.id) WHERE c.contract_id=$1 ORDER BY root_index ASC LIMIT $2 OFFSET $3;`
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
func (s *Store) ContractAction(height uint64, contractFn func(types.FileContractID, uint64, string)) error {
	actions, err := contractsForAction(&dbTxn{s}, height)
	if err != nil {
		return fmt.Errorf("failed to get contracts for action: %w", err)
	}

	for _, action := range actions {
		contractFn(action.ID, height, action.Action)
	}
	return nil
}

// ContractFormationSet returns the set of transactions that were created during
// contract formation.
func (s *Store) ContractFormationSet(id types.FileContractID) ([]types.Transaction, error) {
	var buf []byte
	err := s.queryRow(`SELECT formation_txn_set FROM contracts WHERE contract_id=$1;`, sqlHash256(id)).Scan(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to query formation txn set: %w", err)
	}
	var txnSet []types.Transaction
	if err := decodeTxnSet(buf, &txnSet); err != nil {
		return nil, fmt.Errorf("failed to decode formation txn set: %w", err)
	}
	return txnSet, nil
}

// SetContractStatus sets the contract's status.
func (s *Store) SetContractStatus(id types.FileContractID, status contracts.ContractStatus) error {
	return setContractStatus(&dbTxn{s}, id, status)
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

// ExpireContractSectors expires all sectors that are no longer covered by an
// active contract.
func (s *Store) ExpireContractSectors(height uint64) error {
	// delete in batches to avoid holding a lock on the database for too long
	var done bool
	for {
		if done {
			return nil
		}
		err := s.transaction(func(tx txn) error {
			sectorIDs, err := expiredContractSectors(tx, height, pruneBatchSize)
			if err != nil {
				return fmt.Errorf("failed to select sectors: %w", err)
			} else if len(sectorIDs) == 0 {
				done = true
				return nil
			}

			s.log.Debug("removing contract sectors", zap.Int("count", len(sectorIDs)))

			query := `DELETE FROM contract_sector_roots WHERE id IN (` + queryPlaceHolders(len(sectorIDs)) + `);`
			if _, err := tx.Exec(query, queryArgs(sectorIDs)...); err != nil {
				return fmt.Errorf("failed to delete sectors: %w", err)
			} else if err := incrementNumericStat(tx, metricContractSectors, len(sectorIDs), time.Now()); err != nil {
				return fmt.Errorf("failed to track contract sectors: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		}
	}
}

func contractsForAction(tx txn, height uint64) (actions []contractAction, _ error) {
	const query = `
-- formation not confirmed, within rebroadcast window (rebroadcast)
SELECT contract_id, 'formation' AS action FROM contracts WHERE formation_confirmed=false AND negotiation_height >= $1
UNION
-- formation not confirmed, outside rebroadcast window (reject)
SELECT contract_id, 'reject' AS action FROM contracts WHERE formation_confirmed=false AND negotiation_height < $1
UNION
-- formation confirmed, revision not confirmed, just outside proof window (broadcast revision)
SELECT contract_id, 'revision' AS action FROM contracts WHERE formation_confirmed=true AND confirmed_revision_number != revision_number AND window_start BETWEEN $2 AND $3
UNION
-- formation confirmed, resolution not confirmed, status active, in proof window (broadcast storage proof)
SELECT contract_id, 'resolve' AS action FROM contracts WHERE formation_confirmed=true AND resolution_height IS NULL AND window_start <= $4 AND window_end > $4 AND contract_status=$5
UNION
-- formation confirmed, status active, outside proof window (mark as failed)
SELECT contract_id, 'expire' AS action FROM contracts WHERE formation_confirmed=true AND window_end < $4 AND contract_status=$5;`

	var maxRebroadcastHeight uint64
	if height >= contracts.RebroadcastBuffer {
		maxRebroadcastHeight = height - contracts.RebroadcastBuffer
	}
	minRevisionHeight := height + contracts.RevisionSubmissionBuffer

	rows, err := tx.Query(query, maxRebroadcastHeight, height, minRevisionHeight, height, contracts.ContractStatusActive)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var action contractAction
		if err := rows.Scan((*sqlHash256)(&action.ID), &action.Action); err != nil {
			return nil, fmt.Errorf("failed to scan contract action: %w", err)
		}
		actions = append(actions, action)
	}
	return
}

func renterDBID(tx txn, renterKey types.PublicKey) (int64, error) {
	var dbID int64
	err := tx.QueryRow(`SELECT id FROM contract_renters WHERE public_key=$1;`, sqlHash256(renterKey)).Scan(&dbID)
	if err == nil {
		return dbID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to get renter: %w", err)
	}
	err = tx.QueryRow(`INSERT INTO contract_renters (public_key) VALUES ($1) RETURNING id;`, sqlHash256(renterKey)).Scan(&dbID)
	return dbID, err
}

func insertContract(tx txn, revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage, negotationHeight uint64) (dbID int64, err error) {
	const query = `INSERT INTO contracts (contract_id, renter_id, locked_collateral, rpc_revenue, storage_revenue, ingress_revenue, 
egress_revenue, account_funding, risked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, 
raw_revision, host_sig, renter_sig, confirmed_revision_number, formation_confirmed, contract_status) VALUES
 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING id;`
	renterID, err := renterDBID(tx, revision.RenterKey())
	if err != nil {
		return 0, fmt.Errorf("failed to get renter id: %w", err)
	}
	err = tx.QueryRow(query,
		sqlHash256(revision.Revision.ParentID),
		renterID,
		sqlCurrency(lockedCollateral),
		sqlCurrency(initialUsage.RPCRevenue),
		sqlCurrency(initialUsage.StorageRevenue),
		sqlCurrency(initialUsage.IngressRevenue),
		sqlCurrency(initialUsage.EgressRevenue),
		sqlCurrency(initialUsage.AccountFunding),
		sqlCurrency(initialUsage.RiskedCollateral),
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
		contracts.ContractStatusPending,
	).Scan(&dbID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert contract: %w", err)
	} else if err := incrementNumericStat(tx, metricPendingContracts, 1, time.Now()); err != nil {
		return 0, fmt.Errorf("failed to track pending contracts: %w", err)
	}
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

func buildContractFilter(filter contracts.ContractFilter) (string, []any, error) {
	var whereClause []string
	var queryParams []any

	if len(filter.Statuses) != 0 {
		whereClause = append(whereClause, `c.contract_status IN (`+queryPlaceHolders(len(filter.Statuses))+`)`)
		queryParams = append(queryParams, queryArgs(filter.Statuses)...)
	}

	if len(filter.ContractIDs) != 0 {
		whereClause = append(whereClause, `c.contract_id IN (`+queryPlaceHolders(len(filter.ContractIDs))+`)`)
		for _, value := range filter.ContractIDs {
			queryParams = append(queryParams, sqlHash256(value))
		}
	}

	if len(filter.RenewedFrom) != 0 {
		whereClause = append(whereClause, `rf.contract_id IN (`+queryPlaceHolders(len(filter.RenewedFrom))+`)`)
		for _, value := range filter.RenewedFrom {
			queryParams = append(queryParams, sqlHash256(value))
		}
	}

	if len(filter.RenewedTo) != 0 {
		whereClause = append(whereClause, `rt.contract_id IN (`+queryPlaceHolders(len(filter.RenewedTo))+`)`)
		for _, value := range filter.RenewedTo {
			queryParams = append(queryParams, sqlHash256(value))
		}
	}

	if len(filter.RenterKey) != 0 {
		whereClause = append(whereClause, `r.public_key IN (`+queryPlaceHolders(len(filter.RenterKey))+`)`)
		for _, value := range filter.RenterKey {
			queryParams = append(queryParams, sqlHash256(value))
		}
	}

	if filter.MinNegotiationHeight > 0 && filter.MaxNegotiationHeight > 0 {
		if filter.MinNegotiationHeight < filter.MaxNegotiationHeight {
			return "", nil, errors.New("min negotiation height must be less than max negotiation height")
		}
		whereClause = append(whereClause, `c.negotiation_height BETWEEN ? AND ?`)
		queryParams = append(queryParams, filter.MinNegotiationHeight, filter.MaxNegotiationHeight)
	} else if filter.MinNegotiationHeight > 0 {
		whereClause = append(whereClause, `c.negotiation_height >= ?`)
		queryParams = append(queryParams, filter.MinNegotiationHeight)
	} else if filter.MaxNegotiationHeight > 0 {
		whereClause = append(whereClause, `c.negotiation_height <= ?`)
		queryParams = append(queryParams, filter.MaxNegotiationHeight)
	}

	if filter.MinExpirationHeight > 0 && filter.MaxExpirationHeight > 0 {
		if filter.MinExpirationHeight < filter.MaxExpirationHeight {
			return "", nil, errors.New("min expiration height must be less than max expiration height")
		}
		whereClause = append(whereClause, `c.window_start BETWEEN ? AND ?`)
		queryParams = append(queryParams, filter.MinExpirationHeight, filter.MaxExpirationHeight)
	} else if filter.MinExpirationHeight > 0 {
		whereClause = append(whereClause, `c.window_start >= ?`)
		queryParams = append(queryParams, filter.MinExpirationHeight)
	} else if filter.MaxExpirationHeight > 0 {
		whereClause = append(whereClause, `c.window_start <= ?`)
		queryParams = append(queryParams, filter.MaxExpirationHeight)
	}
	if len(whereClause) == 0 {
		return "", nil, nil
	}
	return "WHERE " + strings.Join(whereClause, " AND "), queryParams, nil
}

func buildOrderBy(filter contracts.ContractFilter) string {
	dir := "ASC"
	if filter.SortDesc {
		dir = "DESC"
	}
	switch filter.SortField {
	case contracts.ContractSortStatus:
		return `ORDER BY c.contract_status ` + dir
	case contracts.ContractSortNegotiationHeight:
		return `ORDER BY c.negotiation_height ` + dir
	default:
		return `ORDER BY c.window_start ` + dir
	}
}

func scanContract(row scanner) (c contracts.Contract, err error) {
	var revisionBuf []byte
	var contractID types.FileContractID
	var resolutionHeight sql.NullInt64
	err = row.Scan((*sqlHash256)(&contractID),
		nullable((*sqlHash256)(&c.RenewedTo)),
		nullable((*sqlHash256)(&c.RenewedFrom)),
		&c.Status,
		&c.NegotiationHeight,
		&c.FormationConfirmed,
		&c.RevisionConfirmed,
		&resolutionHeight,
		(*sqlCurrency)(&c.LockedCollateral),
		(*sqlCurrency)(&c.Usage.RPCRevenue),
		(*sqlCurrency)(&c.Usage.StorageRevenue),
		(*sqlCurrency)(&c.Usage.IngressRevenue),
		(*sqlCurrency)(&c.Usage.EgressRevenue),
		(*sqlCurrency)(&c.Usage.AccountFunding),
		(*sqlCurrency)(&c.Usage.RiskedCollateral),
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
	} else if resolutionHeight.Valid {
		c.ResolutionHeight = uint64(resolutionHeight.Int64)
	}
	return
}

func updateContractMetrics(tx txn, prev, current contracts.ContractStatus) error {
	var initialMetric, finalMetric string
	switch prev {
	case contracts.ContractStatusPending:
		initialMetric = metricPendingContracts
	case contracts.ContractStatusRejected:
		initialMetric = metricRejectedContracts
	case contracts.ContractStatusActive:
		initialMetric = metricActiveContracts
	case contracts.ContractStatusSuccessful:
		initialMetric = metricSuccessfulContracts
	case contracts.ContractStatusFailed:
		initialMetric = metricFailedContracts
	default:
		return fmt.Errorf("invalid prev contract status: %v", current)
	}
	switch current {
	case contracts.ContractStatusPending:
		finalMetric = metricPendingContracts
	case contracts.ContractStatusRejected:
		finalMetric = metricRejectedContracts
	case contracts.ContractStatusActive:
		finalMetric = metricActiveContracts
	case contracts.ContractStatusSuccessful:
		finalMetric = metricSuccessfulContracts
	case contracts.ContractStatusFailed:
		finalMetric = metricFailedContracts
	default:
		return fmt.Errorf("invalid contract status: %v", current)
	}

	if err := incrementNumericStat(tx, initialMetric, -1, time.Now()); err != nil {
		return fmt.Errorf("failed to decrement initial contract metric: %w", err)
	} else if err := incrementNumericStat(tx, finalMetric, 1, time.Now()); err != nil {
		return fmt.Errorf("failed to increment final contract metric: %w", err)
	}
	return nil
}

func setContractStatus(tx txn, id types.FileContractID, status contracts.ContractStatus) error {
	var current contracts.ContractStatus
	if err := tx.QueryRow(`SELECT contract_status FROM contracts WHERE contract_id=$1`, sqlHash256(id)).Scan(&current); err != nil {
		return fmt.Errorf("failed to query contract status: %w", err)
	}

	var dbID int64
	if err := tx.QueryRow(`UPDATE contracts SET contract_status=$1 WHERE contract_id=$2 RETURNING id;`, status, sqlHash256(id)).Scan(&dbID); err != nil {
		return fmt.Errorf("failed to update contract status: %w", err)
	} else if err := updateContractMetrics(tx, current, status); err != nil {
		return fmt.Errorf("failed to update contract metrics: %w", err)
	}
	return nil
}

func expiredContractSectors(tx txn, height uint64, batchSize int64) (ids []int64, _ error) {
	const query = `SELECT csr.id FROM contract_sector_roots csr 
INNER JOIN contracts c ON (csr.contract_id=c.id)
-- past proof window or not confirmed and past the rebroadcast height
WHERE c.window_end < $1 OR c.contract_status=$2 LIMIT $3;`
	rows, err := tx.Query(query, height, contracts.ContractStatusRejected, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query expired sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan expired contract: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
