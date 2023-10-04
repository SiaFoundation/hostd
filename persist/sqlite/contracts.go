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
	// An updateContractsTxn atomically updates the contract manager's state
	updateContractsTxn struct {
		tx txn
	}

	// A contractAction pairs a contract's ID with a lifecycle action.
	contractAction struct {
		ID     types.FileContractID
		Action string
	}

	contractSectorRef struct {
		ID         int64
		SectorID   int64
		ContractID types.FileContractID
	}

	contractSectorRootRef struct {
		dbID     int64
		sectorID int64
	}
)

// setLastChangeID sets the last processed consensus change ID.
func (u *updateContractsTxn) setLastChangeID(ccID modules.ConsensusChangeID, height uint64) error {
	var dbID int64 // unused, but required by QueryRow to ensure exactly one row is updated
	err := u.tx.QueryRow(`UPDATE global_settings SET contracts_last_processed_change=$1, contracts_height=$2 RETURNING id`, sqlHash256(ccID), sqlUint64(height)).Scan(&dbID)
	return err
}

// ConfirmFormation sets the formation_confirmed flag to true.
func (u *updateContractsTxn) ConfirmFormation(id types.FileContractID) error {
	const query = `UPDATE contracts SET formation_confirmed=true WHERE contract_id=$1 RETURNING id;`
	var dbID int64
	err := u.tx.QueryRow(query, sqlHash256(id)).Scan(&dbID)
	if err != nil {
		return fmt.Errorf("failed to confirm formation: %w", err)
	}

	// check if the contract is currently "rejected"
	contract, err := getContract(u.tx, dbID)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	} else if contract.Status == contracts.ContractStatusRejected {
		// rejected contracts have already had their collateral and revenue
		// removed, need to re-add it if the contract is now confirmed
		if err := incrementCurrencyStat(u.tx, metricLockedCollateral, contract.LockedCollateral, false, time.Now()); err != nil {
			return fmt.Errorf("failed to increment locked collateral stat: %w", err)
		} else if err := incrementCurrencyStat(u.tx, metricRiskedCollateral, contract.Usage.RiskedCollateral, false, time.Now()); err != nil {
			return fmt.Errorf("failed to increment risked collateral stat: %w", err)
		}
	}

	// skip updating the status for contracts that are already marked as
	// successful or failed
	if contract.Status != contracts.ContractStatusSuccessful && contract.Status != contracts.ContractStatusFailed {
		if err := setContractStatus(u.tx, id, contracts.ContractStatusActive); err != nil {
			return fmt.Errorf("failed to set contract status to active: %w", err)
		}
	}
	return nil
}

// ConfirmRevision sets the confirmed revision number.
func (u *updateContractsTxn) ConfirmRevision(revision types.FileContractRevision) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2 RETURNING id;`
	var dbID int64
	err := u.tx.QueryRow(query, sqlUint64(revision.RevisionNumber), sqlHash256(revision.ParentID)).Scan(&dbID)
	if err != nil {
		return fmt.Errorf("failed to confirm revision: %w", err)
	} else if err := setContractStatus(u.tx, revision.ParentID, contracts.ContractStatusActive); err != nil {
		return fmt.Errorf("failed to set contract status to active: %w", err)
	}
	return nil
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
	return u.tx.QueryRow(query, sqlHash256(id)).Scan(&dbID)
}

// RevertRevision sets the confirmed revision number to 0.
func (u *updateContractsTxn) RevertRevision(id types.FileContractID) error {
	const query = `UPDATE contracts SET confirmed_revision_number=$1 WHERE contract_id=$2 RETURNING id;`
	var dbID int64
	return u.tx.QueryRow(query, sqlUint64(0), sqlHash256(id)).Scan(&dbID) // TODO: revert to the previous revision number
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

func (s *Store) batchExpireContractSectors(height uint64) (removed []contractSectorRef, pruned int, err error) {
	err = s.transaction(func(tx txn) error {
		sectors, err := expiredContractSectors(tx, height, sqlSectorBatchSize)
		if err != nil {
			return fmt.Errorf("failed to select sectors: %w", err)
		} else if len(sectors) == 0 {
			return nil
		}

		contractSectorIDs := make([]int64, 0, len(sectors))
		for _, sector := range sectors {
			contractSectorIDs = append(contractSectorIDs, sector.ID)
		}
		// delete the sector roots
		query := `DELETE FROM contract_sector_roots WHERE id IN (` + queryPlaceHolders(len(contractSectorIDs)) + `);`
		res, err := tx.Exec(query, queryArgs(contractSectorIDs)...)
		if err != nil {
			return fmt.Errorf("failed to delete sectors: %w", err)
		} else if rows, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if rows != int64(len(contractSectorIDs)) {
			return fmt.Errorf("failed to delete all sectors: %w", err)
		}

		// decrement the number of contract sectors
		if err := incrementNumericStat(tx, metricContractSectors, -len(contractSectorIDs), time.Now()); err != nil {
			return fmt.Errorf("failed to track contract sectors: %w", err)
		}

		for _, ref := range sectors {
			err := pruneSectorRef(tx, ref.SectorID)
			if errors.Is(err, errSectorHasRefs) {
				continue
			} else if err != nil {
				return fmt.Errorf("failed to prune sector ref: %w", err)
			}
			pruned++
		}
		removed = sectors
		return nil
	})
	return
}

// Contracts returns a paginated list of contracts.
func (s *Store) Contracts(filter contracts.ContractFilter) (contracts []contracts.Contract, count int, err error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 100
	}

	whereClause, whereParams, err := buildContractFilter(filter)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build where clause: %w", err)
	}

	contractQuery := fmt.Sprintf(`SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.formation_confirmed, 
	c.revision_number=c.confirmed_revision_number AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
	c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision, c.host_sig, c.renter_sig 
FROM contracts c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts rf ON (c.renewed_from=rf.id) %s %s LIMIT ? OFFSET ?`, whereClause, buildOrderBy(filter))

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM contracts c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts rf ON (c.renewed_from=rf.id) %s`, whereClause)

	if err := s.queryRow(countQuery, whereParams...).Scan(&count); err != nil {
		return nil, 0, fmt.Errorf("failed to query contract count: %w", err)
	}

	rows, err := s.query(contractQuery, append(whereParams, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		contract, err := scanContract(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan contract: %w", err)
		}
		contracts = append(contracts, contract)
	}
	return
}

// Contract returns the contract with the given ID.
func (s *Store) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	err = s.transaction(func(tx txn) error {
		const query = `SELECT id FROM contracts WHERE contract_id=$1;`
		var dbID int64
		err := tx.QueryRow(query, sqlHash256(id)).Scan(&dbID)
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get contract id: %w", err)
		}
		contract, err = getContract(tx, dbID)
		return err
	})
	return
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage, negotationHeight uint64) error {
	return s.transaction(func(tx txn) error {
		_, err := insertContract(tx, revision, formationSet, lockedCollateral, initialUsage, negotationHeight)
		return err
	})
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

		// move the sector roots from the old contract to the new contract
		_, err = tx.Exec(`UPDATE contract_sector_roots SET contract_id=$1 WHERE contract_id=$2`, renewedDBID, clearedDBID)
		if err != nil {
			return fmt.Errorf("failed to copy sector roots: %w", err)
		}
		return nil
	})
}

func contractSectorRoots(tx txn, contractID int64) (uint64, error) {
	var index uint64
	err := tx.QueryRow(`SELECT COUNT(*) FROM contract_sector_roots WHERE contract_id=$1`, contractID).Scan(&index)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return index, err
}

// ReviseContract atomically updates a contract's revision and sectors
func (s *Store) ReviseContract(revision contracts.SignedRevision, usage contracts.Usage, sectorChanges []contracts.SectorChange) error {
	return s.transaction(func(tx txn) error {
		contractID, err := reviseContract(tx, revision)
		if err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		} else if err := incrementContractUsage(tx, contractID, usage); err != nil {
			return fmt.Errorf("failed to update contract usage: %w", err)
		}

		var delta int
		sectors, err := contractSectorRoots(tx, contractID)
		if err != nil {
			return fmt.Errorf("failed to get sector index: %w", err)
		}

		for _, change := range sectorChanges {
			switch change.Action {
			case contracts.SectorActionAppend:
				if err := appendSector(tx, contractID, change.Root, sectors); err != nil {
					return fmt.Errorf("failed to append sector: %w", err)
				}
				sectors++
				delta++
			case contracts.SectorActionTrim:
				if sectors < change.A {
					return fmt.Errorf("cannot trim %v sectors from contract with %v sectors", change.A, sectors)
				}

				if err := trimSectors(tx, contractID, change.A, s.log); err != nil {
					return fmt.Errorf("failed to trim sectors: %w", err)
				}
				sectors -= change.A
				delta -= int(change.A)
			case contracts.SectorActionUpdate:
				if err := updateSector(tx, contractID, change.Root, change.A); err != nil {
					return fmt.Errorf("failed to update sector: %w", err)
				}
			case contracts.SectorActionSwap:
				if err := swapSectors(tx, contractID, change.A, change.B); err != nil {
					return fmt.Errorf("failed to swap sectors: %w", err)
				}
			}
		}

		// update global stats
		if err := incrementNumericStat(tx, metricContractSectors, delta, time.Now()); err != nil {
			return fmt.Errorf("failed to track contract sectors: %w", err)
		} else if err := incrementCurrencyStat(tx, metricRiskedCollateral, usage.RiskedCollateral, false, time.Now()); err != nil {
			return fmt.Errorf("failed to track risked collateral: %w", err)
		} else if err := incrementPotentialRevenueMetrics(tx, usage, false); err != nil {
			return fmt.Errorf("failed to track potential revenue: %w", err)
		}
		return nil
	})
}

// SectorRoots returns the sector roots for a contract. If limit is 0, all roots
// are returned.
func (s *Store) SectorRoots(contractID types.FileContractID) (roots []types.Hash256, err error) {
	err = s.transaction(func(tx txn) error {
		var dbID int64
		err := tx.QueryRow(`SELECT id FROM contracts WHERE contract_id=$1;`, sqlHash256(contractID)).Scan(&dbID)
		if err != nil {
			return fmt.Errorf("failed to get contract id: %w", err)
		}

		const query = `SELECT s.sector_root FROM contract_sector_roots c
INNER JOIN stored_sectors s ON (c.sector_id = s.id)
WHERE c.contract_id=$1
ORDER BY root_index ASC`

		rows, err := tx.Query(query, dbID)
		if err != nil {
			return fmt.Errorf("failed to query sector roots: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				return fmt.Errorf("failed to scan sector root: %w", err)
			}
			roots = append(roots, root)
		}
		return nil
	})
	return
}

// ContractAction calls contractFn on every contract in the store that
// needs a lifecycle action performed.
func (s *Store) ContractAction(height uint64, contractFn func(types.FileContractID, uint64, string)) error {
	tx := &dbTxn{s}
	actions, err := rebroadcastContractActions(tx, height)
	if err != nil {
		return fmt.Errorf("failed to get rebroadcast actions: %w", err)
	}
	for _, action := range actions {
		contractFn(action.ID, height, action.Action)
	}
	actions, err = rejectContractActions(tx, height)
	if err != nil {
		return fmt.Errorf("failed to get reject actions: %w", err)
	}
	for _, action := range actions {
		contractFn(action.ID, height, action.Action)
	}
	actions, err = revisionContractActions(tx, height)
	if err != nil {
		return fmt.Errorf("failed to get revision actions: %w", err)
	}
	for _, action := range actions {
		contractFn(action.ID, height, action.Action)
	}
	actions, err = resolveContractActions(tx, height)
	if err != nil {
		return fmt.Errorf("failed to get resolve actions: %w", err)
	}
	for _, action := range actions {
		contractFn(action.ID, height, action.Action)
	}
	actions, err = expireContractActions(tx, height)
	if err != nil {
		return fmt.Errorf("failed to get expire actions: %w", err)
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

// ExpireContract expires a contract and updates its status. Should only be used
// if the contract is active or pending.
func (s *Store) ExpireContract(id types.FileContractID, status contracts.ContractStatus) error {
	return s.transaction(func(tx txn) error {
		var contractID int64
		err := tx.QueryRow(`SELECT id FROM contracts WHERE contract_id=$1;`, sqlHash256(id)).Scan(&contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract id: %w", err)
		}
		// get the contract and check if the status is already set
		contract, err := getContract(tx, contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract: %w", err)
		} else if contract.Status == status {
			return nil
		}

		// successful, failed, and rejected contracts should have already had their
		// collateral removed from the metrics
		if contract.Status == contracts.ContractStatusActive || contract.Status == contracts.ContractStatusPending {
			// successful, failed and rejected contracts should have already had
			// their collateral removed from the metrics
			if err := incrementCurrencyStat(tx, metricLockedCollateral, contract.LockedCollateral, true, time.Now()); err != nil {
				return fmt.Errorf("failed to increment locked collateral stat: %w", err)
			} else if err := incrementCurrencyStat(tx, metricRiskedCollateral, contract.Usage.RiskedCollateral, true, time.Now()); err != nil {
				return fmt.Errorf("failed to increment risked collateral stat: %w", err)
			} else if err := incrementPotentialRevenueMetrics(tx, contract.Usage, true); err != nil {
				return fmt.Errorf("failed to decrement potential revenue: %w", err)
			}
		}

		// if the contract is successful and the final revision is confirmed,
		// increment the earned revenue metrics
		//
		// note: if the final revision is not confirmed, the earned revenue
		// may be incorrect.
		if status == contracts.ContractStatusSuccessful && contract.RevisionConfirmed {
			if err := incrementEarnedRevenueMetrics(tx, contract.Usage, false); err != nil {
				return fmt.Errorf("failed to increment earned revenue: %w", err)
			}
		}
		// update the contract status
		if err := setContractStatus(tx, id, status); err != nil {
			return fmt.Errorf("failed to set contract status: %w", err)
		}
		return nil
	})
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
	var totalRemoved int
	contractExpired := make(map[types.FileContractID]int)
	defer func() {
		for contractID, removed := range contractExpired {
			s.log.Debug("expired contract sectors", zap.Stringer("contractID", contractID), zap.Uint64("height", height), zap.Int("expired", removed))
		}
		if totalRemoved > 0 {
			s.log.Debug("removed contract sectors", zap.Uint64("height", height), zap.Int("removed", totalRemoved))
		}
	}()
	// delete in batches to avoid holding a lock on the database for too long
	for i := 0; ; i++ {
		expired, removed, err := s.batchExpireContractSectors(height)
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if len(expired) == 0 {
			return nil
		}
		for _, ref := range expired {
			contractExpired[ref.ContractID]++
		}
		totalRemoved += removed
		jitterSleep(time.Millisecond) // allow other transactions to run
	}
}

func getContract(tx txn, contractID int64) (contracts.Contract, error) {
	const query = `SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.formation_confirmed, 
	c.revision_number=c.confirmed_revision_number AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
	c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision, c.host_sig, c.renter_sig 
	FROM contracts c
	LEFT JOIN contracts rt ON (c.renewed_to = rt.id)
	LEFT JOIN contracts rf ON (c.renewed_from = rf.id)
	WHERE c.id=$1;`
	row := tx.QueryRow(query, contractID)
	contract, err := scanContract(row)
	if errors.Is(err, sql.ErrNoRows) {
		err = contracts.ErrNotFound
	}
	return contract, err
}

func appendSector(tx txn, contractID int64, root types.Hash256, index uint64) error {
	var sectorID int64
	err := tx.QueryRow(`INSERT INTO contract_sector_roots (contract_id, sector_id, root_index) SELECT $1, id, $2 FROM stored_sectors WHERE sector_root=$3 RETURNING sector_id`, contractID, index, sqlHash256(root)).Scan(&sectorID)
	return err
}

func updateSector(tx txn, contractID int64, root types.Hash256, index uint64) error {
	var oldSectorID int64
	if err := tx.QueryRow(`SELECT sector_id FROM contract_sector_roots WHERE contract_id=$1 AND root_index=$2`, contractID, index).Scan(&oldSectorID); err != nil {
		return fmt.Errorf("failed to get old sector id: %w", err)
	}

	const query = `WITH sector AS (
	SELECT id FROM stored_sectors WHERE sector_root=$1
)
UPDATE contract_sector_roots
SET sector_id=sector.id
FROM sector
WHERE contract_id=$2 AND root_index=$3
RETURNING sector_id;`
	var newSectorID int64
	err := tx.QueryRow(query, sqlHash256(root), contractID, index).Scan(&newSectorID)
	if err != nil {
		return err
	} else if err := pruneSectorRef(tx, oldSectorID); err != nil {
		return fmt.Errorf("failed to prune sector ref: %w", err)
	}
	return nil
}

func swapSectors(tx txn, contractID int64, i, j uint64) error {
	if i == j {
		return nil
	}

	var records []contractSectorRootRef
	rows, err := tx.Query(`SELECT id, sector_id FROM contract_sector_roots WHERE contract_id=$1 AND root_index IN ($2, $3);`, contractID, i, j)
	if err != nil {
		return fmt.Errorf("failed to query sector IDs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var record contractSectorRootRef
		if err := rows.Scan(&record.dbID, &record.sectorID); err != nil {
			return fmt.Errorf("failed to scan sector ID: %w", err)
		}
		records = append(records, record)
	}

	if len(records) != 2 {
		return errors.New("failed to find both sectors")
	}

	res, err := tx.Exec(`UPDATE contract_sector_roots SET sector_id=$1 WHERE id=$2`, records[1].sectorID, records[0].dbID)
	if err != nil {
		return fmt.Errorf("failed to update sector ID: %w", err)
	} else if rows, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %v", rows)
	}

	res, err = tx.Exec(`UPDATE contract_sector_roots SET sector_id=$1 WHERE id=$2`, records[0].sectorID, records[1].dbID)
	if err != nil {
		return fmt.Errorf("failed to update sector ID: %w", err)
	} else if rows, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("expected 1 row affected, got %v", rows)
	}

	func() {
		rows, err := tx.Query(`SELECT sector_id, root_index FROM contract_sector_roots WHERE contract_id=$1 AND root_index IN ($2, $3) ORDER BY root_index ASC;`, contractID, i, j)
		if err != nil {
			panic(fmt.Errorf("failed to query sector IDs: %w", err))
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var index int64
			if err := rows.Scan(&id, &index); err != nil {
				panic(fmt.Errorf("failed to scan sector ID: %w", err))
			}
		}
	}()

	return nil
}

// lastContractSectors returns the last n sector IDs for a contract.
func lastContractSectors(tx txn, contractID int64, n uint64) (roots []contractSectorRootRef, err error) {
	const query = `SELECT id, sector_id FROM contract_sector_roots WHERE contract_id=$1 ORDER BY root_index DESC LIMIT $2;`
	rows, err := tx.Query(query, contractID, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var ref contractSectorRootRef
		if err := rows.Scan(&ref.dbID, &ref.sectorID); err != nil {
			return nil, err
		}
		roots = append(roots, ref)
	}
	return
}

// trimSectors deletes the last n sector roots for a contract.
func trimSectors(tx txn, contractID int64, n uint64, log *zap.Logger) error {
	refs, err := lastContractSectors(tx, contractID, n)
	if err != nil {
		return fmt.Errorf("failed to get sector IDs: %w", err)
	}

	var rootIDs, sectorIDs []int64
	for _, ref := range refs {
		rootIDs = append(rootIDs, ref.dbID)
		sectorIDs = append(sectorIDs, ref.sectorID)
	}

	// delete the sector roots
	query := `DELETE FROM contract_sector_roots WHERE id IN (` + queryPlaceHolders(len(rootIDs)) + `);`
	res, err := tx.Exec(query, queryArgs(rootIDs)...)
	if err != nil {
		return fmt.Errorf("failed to delete sectors: %w", err)
	} else if rows, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if rows != int64(n) {
		return fmt.Errorf("failed to delete all sectors: %w", err)
	}

	// attempt to prune the deleted sectors
	for _, sectorID := range sectorIDs {
		if err := pruneSectorRef(tx, sectorID); err != nil && !errors.Is(err, errSectorHasRefs) {
			return fmt.Errorf("failed to prune sector ref: %w", err)
		}
	}
	return nil
}

// clearContract clears a contract and returns its ID
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

// reviseContract revises a contract and returns its ID
func reviseContract(tx txn, revision contracts.SignedRevision) (dbID int64, err error) {
	err = tx.QueryRow(`UPDATE contracts SET (revision_number, window_start, window_end, raw_revision, host_sig, renter_sig) = ($1, $2, $3, $4, $5, $6) WHERE contract_id=$7 RETURNING id;`,
		sqlUint64(revision.Revision.RevisionNumber),
		revision.Revision.WindowStart,
		revision.Revision.WindowEnd,
		encodeRevision(revision.Revision),
		sqlHash512(revision.HostSignature),
		sqlHash512(revision.RenterSignature),
		sqlHash256(revision.Revision.ParentID),
	).Scan(&dbID)
	return
}

func incrementContractUsage(tx txn, dbID int64, usage contracts.Usage) error {
	const query = `SELECT rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral FROM contracts WHERE id=$1;`
	var total contracts.Usage
	err := tx.QueryRow(query, dbID).Scan(
		(*sqlCurrency)(&total.RPCRevenue),
		(*sqlCurrency)(&total.StorageRevenue),
		(*sqlCurrency)(&total.IngressRevenue),
		(*sqlCurrency)(&total.EgressRevenue),
		(*sqlCurrency)(&total.AccountFunding),
		(*sqlCurrency)(&total.RiskedCollateral))
	if err != nil {
		return fmt.Errorf("failed to get existing revenue: %w", err)
	}
	total = total.Add(usage)
	var updatedID int64
	err = tx.QueryRow(`UPDATE contracts SET (rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING id;`,
		sqlCurrency(total.RPCRevenue),
		sqlCurrency(total.StorageRevenue),
		sqlCurrency(total.IngressRevenue),
		sqlCurrency(total.EgressRevenue),
		sqlCurrency(total.AccountFunding),
		sqlCurrency(total.RiskedCollateral),
		dbID).Scan(&updatedID)
	if err != nil {
		return fmt.Errorf("failed to update contract revenue: %w", err)
	}
	return nil
}

func rebroadcastContractActions(tx txn, height uint64) (actions []contractAction, _ error) {
	// formation not confirmed, within rebroadcast window
	const query = `SELECT contract_id FROM contracts WHERE formation_confirmed=false AND negotiation_height BETWEEN $1 AND $2`

	var minNegotiationHeight uint64
	if height >= contracts.RebroadcastBuffer {
		minNegotiationHeight = height - contracts.RebroadcastBuffer
	}

	rows, err := tx.Query(query, minNegotiationHeight, height)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		action := contractAction{
			Action: contracts.ActionBroadcastFormation,
		}
		if err := rows.Scan((*sqlHash256)(&action.ID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		actions = append(actions, action)
	}
	return
}

func rejectContractActions(tx txn, height uint64) (actions []contractAction, _ error) {
	// formation not confirmed, not rejected, outside rebroadcast window
	const query = `SELECT contract_id FROM contracts WHERE formation_confirmed=false AND negotiation_height < $1 AND contract_status != $2`

	var maxRebroadcastHeight uint64
	if height >= contracts.RebroadcastBuffer {
		maxRebroadcastHeight = height - contracts.RebroadcastBuffer
	}

	rows, err := tx.Query(query, maxRebroadcastHeight, contracts.ContractStatusRejected)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		action := contractAction{
			Action: contracts.ActionReject,
		}
		if err := rows.Scan((*sqlHash256)(&action.ID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		actions = append(actions, action)
	}
	return
}

func revisionContractActions(tx txn, height uint64) (actions []contractAction, _ error) {
	// formation confirmed, revision not confirmed, just outside proof window
	const query = `SELECT contract_id FROM contracts WHERE formation_confirmed=true AND confirmed_revision_number != revision_number AND window_start BETWEEN $1 AND $2`
	minRevisionHeight := height + contracts.RevisionSubmissionBuffer
	rows, err := tx.Query(query, height, minRevisionHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		action := contractAction{
			Action: contracts.ActionBroadcastFinalRevision,
		}
		if err := rows.Scan((*sqlHash256)(&action.ID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		actions = append(actions, action)
	}
	return
}

func resolveContractActions(tx txn, height uint64) (actions []contractAction, _ error) {
	// formation confirmed, resolution not confirmed, status active, in proof window
	const query = `SELECT contract_id FROM contracts WHERE formation_confirmed=true AND resolution_height IS NULL AND window_start <= $1 AND window_end > $1`
	rows, err := tx.Query(query, height)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		action := contractAction{
			Action: contracts.ActionBroadcastResolution,
		}
		if err := rows.Scan((*sqlHash256)(&action.ID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		actions = append(actions, action)
	}
	return
}

func expireContractActions(tx txn, height uint64) (actions []contractAction, _ error) {
	const query = `SELECT contract_id FROM contracts WHERE window_end < $1 AND contract_status = $2;`
	rows, err := tx.Query(query, height, contracts.ContractStatusActive)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		action := contractAction{
			Action: contracts.ActionExpire,
		}
		if err := rows.Scan((*sqlHash256)(&action.ID)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
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
egress_revenue, registry_read, registry_write, account_funding, risked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set, 
raw_revision, host_sig, renter_sig, confirmed_revision_number, formation_confirmed, contract_status) VALUES
 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22) RETURNING id;`
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
		sqlCurrency(initialUsage.RegistryRead),
		sqlCurrency(initialUsage.RegistryWrite),
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
	}
	// increment the contract count metric
	if err := incrementNumericStat(tx, metricPendingContracts, 1, time.Now()); err != nil {
		return 0, fmt.Errorf("failed to track pending contracts: %w", err)
	}
	// increment the collateral metrics
	if err := incrementCurrencyStat(tx, metricLockedCollateral, lockedCollateral, false, time.Now()); err != nil {
		return 0, fmt.Errorf("failed to track locked collateral: %w", err)
	} else if err := incrementCurrencyStat(tx, metricRiskedCollateral, initialUsage.RiskedCollateral, false, time.Now()); err != nil {
		return 0, fmt.Errorf("failed to track risked collateral: %w", err)
	}
	// increment the potential revenue metrics
	if err := incrementPotentialRevenueMetrics(tx, initialUsage, false); err != nil {
		return 0, fmt.Errorf("failed to increment potential revenue: %w", err)
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

func updateContractMetrics(tx txn, current, next contracts.ContractStatus) error {
	if current == next {
		return nil
	}

	var initialMetric, finalMetric string
	switch current {
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
	switch next {
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

func expiredContractSectors(tx txn, height uint64, batchSize int64) (sectors []contractSectorRef, _ error) {
	const query = `SELECT csr.id, c.contract_id, csr.sector_id FROM contract_sector_roots csr 
INNER JOIN contracts c ON (csr.contract_id=c.id)
-- past proof window or not confirmed and past the rebroadcast height
WHERE c.window_end < $1 OR c.contract_status=$2 LIMIT $3;`
	rows, err := tx.Query(query, height, contracts.ContractStatusRejected, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query expired sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref contractSectorRef
		if err := rows.Scan(&ref.ID, (*sqlHash256)(&ref.ContractID), &ref.SectorID); err != nil {
			return nil, fmt.Errorf("failed to scan expired contract: %w", err)
		}
		sectors = append(sectors, ref)
	}
	return
}

func incrementPotentialRevenueMetrics(tx txn, usage contracts.Usage, negative bool) error {
	if err := incrementCurrencyStat(tx, metricPotentialRPCRevenue, usage.RPCRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment rpc revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricPotentialStorageRevenue, usage.StorageRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment storage revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricPotentialEgressRevenue, usage.EgressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment egress revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricPotentialIngressRevenue, usage.IngressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment ingress revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricPotentialRegistryReadRevenue, usage.RegistryRead, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment registry read revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricPotentialRegistryWriteRevenue, usage.RegistryWrite, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment registry write revenue stat: %w", err)
	}
	return nil
}

func incrementEarnedRevenueMetrics(tx txn, usage contracts.Usage, negative bool) error {
	if err := incrementCurrencyStat(tx, metricEarnedRPCRevenue, usage.RPCRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment rpc revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricEarnedStorageRevenue, usage.StorageRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment storage revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricEarnedEgressRevenue, usage.EgressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment egress revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricEarnedIngressRevenue, usage.IngressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment ingress revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricEarnedRegistryReadRevenue, usage.RegistryRead, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment registry read revenue stat: %w", err)
	} else if err := incrementCurrencyStat(tx, metricEarnedRegistryWriteRevenue, usage.RegistryWrite, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to increment registry write revenue stat: %w", err)
	}
	return nil
}
