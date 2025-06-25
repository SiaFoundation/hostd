package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.uber.org/zap"
)

type (
	contractSectorRootRef struct {
		dbID     int64
		sectorID int64
		root     types.Hash256
	}
)

var _ contracts.ContractStore = (*Store)(nil)

func (s *Store) batchExpireContractSectors(height uint64) (expired int, err error) {
	err = s.transaction(func(tx *txn) (err error) {
		sectorIDs, err := deleteExpiredContractSectors(tx, height)
		if err != nil {
			return fmt.Errorf("failed to delete contract sectors: %w", err)
		}
		expired = len(sectorIDs)

		// decrement the contract metrics
		if err := incrementNumericStat(tx, metricContractSectors, -len(sectorIDs), time.Now()); err != nil {
			return fmt.Errorf("failed to decrement contract sectors: %w", err)
		}
		return nil
	})
	return
}

func (s *Store) batchExpireV2ContractSectors(height uint64) (expired int, err error) {
	err = s.transaction(func(tx *txn) (err error) {
		sectorIDs, err := deleteExpiredV2ContractSectors(tx, height)
		if err != nil {
			return fmt.Errorf("failed to delete contract sectors: %w", err)
		}
		expired = len(sectorIDs)

		// decrement the contract metrics
		if err := incrementNumericStat(tx, metricContractSectors, -len(sectorIDs), time.Now()); err != nil {
			return fmt.Errorf("failed to decrement contract sectors: %w", err)
		}
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
	COALESCE(c.revision_number=c.confirmed_revision_number, false) AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
	c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision, c.host_sig, c.renter_sig
FROM contracts c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts rf ON (c.renewed_from=rf.id) %s %s LIMIT ? OFFSET ?`, whereClause, buildOrderBy(filter))

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM contracts c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts rf ON (c.renewed_from=rf.id) %s`, whereClause)

	err = s.transaction(func(tx *txn) error {
		if err := tx.QueryRow(countQuery, whereParams...).Scan(&count); err != nil {
			return fmt.Errorf("failed to query contract count: %w", err)
		}

		rows, err := tx.Query(contractQuery, append(whereParams, filter.Limit, filter.Offset)...)
		if err != nil {
			return fmt.Errorf("failed to query contracts: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			contract, err := scanContract(rows)
			if err != nil {
				return fmt.Errorf("failed to scan contract: %w", err)
			}
			contracts = append(contracts, contract)
		}
		return rows.Err()
	})
	return
}

// Contract returns the contract with the given ID.
func (s *Store) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT id FROM contracts WHERE contract_id=$1;`
		var dbID int64
		err := tx.QueryRow(query, encode(id)).Scan(&dbID)
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

// V2ContractElement returns the latest v2 state element with the given ID.
func (s *Store) V2ContractElement(contractID types.FileContractID) (basis types.ChainIndex, ele types.V2FileContractElement, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT cs.raw_contract, cs.leaf_index, cs.merkle_proof, g.last_scanned_index AS basis
FROM contracts_v2 c
INNER JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
CROSS JOIN global_settings g
WHERE c.contract_id=?`

		err := tx.QueryRow(query, encode(contractID)).Scan(decode(&ele.V2FileContract), decode(&ele.StateElement.LeafIndex), decode(&ele.StateElement.MerkleProof), decode(&basis))
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.ErrNotFound
		}
		ele.ID = contractID
		return err
	})
	return
}

// V2Contract returns the contract with the given ID.
func (s *Store) V2Contract(id types.FileContractID) (contract contracts.V2Contract, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.confirmation_index,
COALESCE(c.revision_number=cs.revision_number, false) AS revision_confirmed, c.resolution_index, c.rpc_revenue,
c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision
FROM contracts_v2 c
LEFT JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
LEFT JOIN contracts_v2 rt ON (c.renewed_to = rt.id)
LEFT JOIN contracts_v2 rf ON (c.renewed_from = rf.id)
WHERE c.contract_id=$1;`
		contract, err = scanV2Contract(tx.QueryRow(query, encode(id)))
		return err
	})
	return
}

// RebroadcastFormationSets returns formation sets that should be rebroadcast
func (s *Store) RebroadcastFormationSets(minNegotiationheight uint64) (rebroadcast [][]types.Transaction, err error) {
	err = s.transaction(func(tx *txn) error {
		rows, err := tx.Query(`SELECT formation_txn_set FROM contracts WHERE formation_confirmed=false AND negotiation_height > $1`, minNegotiationheight)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var buf []byte
			if err := rows.Scan(&buf); err != nil {
				return fmt.Errorf("failed to scan formation set: %w", err)
			}
			var formationSet []types.Transaction
			if err := decodeTxnSet(buf, &formationSet); err != nil {
				return fmt.Errorf("failed to decode formation txn set: %w", err)
			}
			rebroadcast = append(rebroadcast, formationSet)
		}
		return rows.Err()
	})
	return
}

// V2Contracts returns a paginated list of v2 contracts.
func (s *Store) V2Contracts(filter contracts.V2ContractFilter) (contracts []contracts.V2Contract, count int, err error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 100
	}

	whereClause, whereParams, err := buildV2ContractFilter(filter)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build where clause: %w", err)
	}

	contractQuery := fmt.Sprintf(`SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.confirmation_index,
COALESCE(c.revision_number=cs.revision_number, false) AS revision_confirmed, c.resolution_index, c.rpc_revenue,
c.storage_revenue, c.ingress_revenue, c.egress_revenue, c.account_funding, c.risked_collateral, c.raw_revision
FROM contracts_v2 c
LEFT JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts_v2 rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts_v2 rf ON (c.renewed_from=rf.id) %s %s LIMIT ? OFFSET ?`, whereClause, buildV2OrderBy(filter))

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM contracts_v2 c
INNER JOIN contract_renters r ON (c.renter_id=r.id)
LEFT JOIN contracts_v2 rt ON (c.renewed_to=rt.id)
LEFT JOIN contracts_v2 rf ON (c.renewed_from=rf.id) %s`, whereClause)

	err = s.transaction(func(tx *txn) error {
		if err := tx.QueryRow(countQuery, whereParams...).Scan(&count); err != nil {
			return fmt.Errorf("failed to query contract count: %w", err)
		}

		rows, err := tx.Query(contractQuery, append(whereParams, filter.Limit, filter.Offset)...)
		if err != nil {
			return fmt.Errorf("failed to query contracts: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			contract, err := scanV2Contract(rows)
			if err != nil {
				return fmt.Errorf("failed to scan contract: %w", err)
			}
			contracts = append(contracts, contract)
		}
		return rows.Err()
	})
	return
}

// AddV2Contract adds a new contract to the database.
func (s *Store) AddV2Contract(contract contracts.V2Contract, formationSet rhp4.TransactionSet) error {
	return s.transaction(func(tx *txn) error {
		_, err := insertV2Contract(tx, contract, formationSet)
		return err
	})
}

// RenewV2Contract adds a new v2 contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract. The status of the old contract should continue
// to be active until the renewal is confirmed
func (s *Store) RenewV2Contract(renewal contracts.V2Contract, renewalSet rhp4.TransactionSet, renewedID types.FileContractID, roots []types.Hash256) error {
	return s.transaction(func(tx *txn) error {
		// add the new contract
		renewedDBID, err := insertV2Contract(tx, renewal, renewalSet)
		if err != nil {
			return fmt.Errorf("failed to insert renewed contract: %w", err)
		}

		clearedDBID, err := updateResolvedV2Contract(tx, renewedID, renewedDBID)
		if err != nil {
			return fmt.Errorf("failed to resolve existing contract: %w", err)
		}

		// update the renewed_from field
		err = tx.QueryRow(`UPDATE contracts_v2 SET renewed_from=$1 WHERE id=$2 RETURNING id;`, clearedDBID, renewedDBID).Scan(&renewedDBID)
		if err != nil {
			return fmt.Errorf("failed to update renewed contract: %w", err)
		}

		// move the sector roots from the old contract to the new contract
		_, err = tx.Exec(`UPDATE contract_v2_sector_roots SET contract_id=$1 WHERE contract_id=$2`, renewedDBID, clearedDBID)
		if err != nil {
			return fmt.Errorf("failed to copy sector roots: %w", err)
		}
		return nil
	})
}

// AddContract adds a new contract to the database.
func (s *Store) AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage, negotationHeight uint64) error {
	return s.transaction(func(tx *txn) error {
		_, err := insertContract(tx, revision, formationSet, lockedCollateral, initialUsage, negotationHeight)
		if err != nil {
			return fmt.Errorf("failed to add contract: %w", err)
		}
		return nil
	})
}

// RenewContract adds a new contract to the database and sets the old
// contract's renewed_from field. The old contract's sector roots are
// copied to the new contract.
func (s *Store) RenewContract(renewal contracts.SignedRevision, clearing contracts.SignedRevision, renewalTxnSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, renewalUsage contracts.Usage, negotationHeight uint64) error {
	return s.transaction(func(tx *txn) error {
		// add the new contract
		renewedDBID, err := insertContract(tx, renewal, renewalTxnSet, lockedCollateral, renewalUsage, negotationHeight)
		if err != nil {
			return fmt.Errorf("failed to insert renewed contract: %w", err)
		}

		clearedDBID, err := clearContract(tx, clearing, renewedDBID, clearingUsage)
		if err != nil {
			return fmt.Errorf("failed to clear contract: %w", err)
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

func incrementV2ContractUsage(tx *txn, dbID int64, usage proto4.Usage) error {
	const query = `SELECT rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral FROM contracts_v2 WHERE id=$1;`
	var existing proto4.Usage
	err := tx.QueryRow(query, dbID).Scan(
		decode(&existing.RPC),
		decode(&existing.Storage),
		decode(&existing.Ingress),
		decode(&existing.Egress),
		decode(&existing.AccountFunding),
		decode(&existing.RiskedCollateral))
	if err != nil {
		return fmt.Errorf("failed to get existing revenue: %w", err)
	}

	total := existing.Add(usage)
	if total == existing {
		return nil
	}

	var updatedID int64
	err = tx.QueryRow(`UPDATE contracts_v2 SET (rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING id;`,
		encode(total.RPC),
		encode(total.Storage),
		encode(total.Ingress),
		encode(total.Egress),
		encode(total.AccountFunding),
		encode(total.RiskedCollateral),
		dbID).Scan(&updatedID)
	if err != nil {
		return fmt.Errorf("failed to update contract revenue: %w", err)
	}
	return nil
}

// ReviseV2Contract atomically updates a contract's revision and sectors
func (s *Store) ReviseV2Contract(id types.FileContractID, revision types.V2FileContract, oldRoots, newRoots []types.Hash256, usage proto4.Usage) error {
	return s.transaction(func(tx *txn) error {
		contractDBID, err := reviseV2Contract(tx, id, revision, usage)
		if err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		} else if err := updateV2ContractSectors(tx, contractDBID, oldRoots, newRoots); err != nil {
			return fmt.Errorf("failed to update contract sectors: %w", err)
		}
		return nil
	})
}

// ReviseContract atomically updates a contract's revision and sectors
func (s *Store) ReviseContract(revision contracts.SignedRevision, oldRoots, newRoots []types.Hash256, usage contracts.Usage) error {
	return s.transaction(func(tx *txn) error {
		// revise the contract
		contractID, err := reviseContract(tx, revision, usage)
		if err != nil {
			return fmt.Errorf("failed to revise contract: %w", err)
		} else if err := updateContractSectors(tx, contractID, oldRoots, newRoots); err != nil {
			return fmt.Errorf("failed to update contract sectors: %w", err)
		}
		return nil
	})
}

// SectorRoots returns the sector roots for all contracts.
func (s *Store) SectorRoots() (roots map[types.FileContractID][]types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT s.sector_root, c.contract_id FROM contract_sector_roots cr
INNER JOIN stored_sectors s ON (cr.sector_id = s.id)
INNER JOIN contracts c ON (cr.contract_id = c.id)
ORDER BY cr.contract_id, cr.root_index ASC;`

		rows, err := tx.Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()

		roots = make(map[types.FileContractID][]types.Hash256)
		for rows.Next() {
			var contractID types.FileContractID
			var root types.Hash256

			if err := rows.Scan(decode(&root), decode(&contractID)); err != nil {
				return fmt.Errorf("failed to scan sector root: %w", err)
			}
			roots[contractID] = append(roots[contractID], root)
		}
		return rows.Err()
	})
	return
}

// V2SectorRoots returns the sector roots for all v2 contracts.
func (s *Store) V2SectorRoots() (roots map[types.FileContractID][]types.Hash256, err error) {
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT s.sector_root, c.contract_id FROM contract_v2_sector_roots cr
INNER JOIN stored_sectors s ON (cr.sector_id = s.id)
INNER JOIN contracts_v2 c ON (cr.contract_id = c.id)
ORDER BY cr.contract_id, cr.root_index ASC;`

		rows, err := tx.Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()

		roots = make(map[types.FileContractID][]types.Hash256)
		for rows.Next() {
			var contractID types.FileContractID
			var root types.Hash256

			if err := rows.Scan(decode(&root), decode(&contractID)); err != nil {
				return fmt.Errorf("failed to scan sector root: %w", err)
			}
			roots[contractID] = append(roots[contractID], root)
		}
		return rows.Err()
	})
	return
}

// ContractActions returns the contract lifecycle actions for the given index.
func (s *Store) ContractActions(index types.ChainIndex, revisionBroadcastHeight uint64) (actions contracts.LifecycleActions, err error) {
	err = s.transaction(func(tx *txn) error {
		actions.RebroadcastFormation, err = rebroadcastContracts(tx)
		if err != nil {
			return fmt.Errorf("failed to get formation broadcast actions: %w", err)
		}
		actions.BroadcastRevision, err = broadcastRevision(tx, index, revisionBroadcastHeight)
		if err != nil {
			return fmt.Errorf("failed to get revision broadcast actions: %w", err)
		}
		actions.BroadcastProof, err = proofContracts(tx, index)
		if err != nil {
			return fmt.Errorf("failed to get proof broadcast actions: %w", err)
		}

		// v2
		actions.RebroadcastV2Formation, err = rebroadcastV2Contracts(tx)
		if err != nil {
			return fmt.Errorf("failed to get v2 formation broadcast actions: %w", err)
		}

		actions.BroadcastV2Revision, err = broadcastV2Revision(tx, index, revisionBroadcastHeight)
		if err != nil {
			return fmt.Errorf("failed to get v2 revision broadcast actions: %w", err)
		}

		actions.BroadcastV2Proof, err = proofV2Contracts(tx, index)
		if err != nil {
			return fmt.Errorf("failed to get v2 proof broadcast actions: %w", err)
		}
		actions.BroadcastV2Expiration, err = expireV2Contracts(tx, index)
		if err != nil {
			return fmt.Errorf("failed to get v2 expiration broadcast actions: %w", err)
		}
		return nil
	})
	return
}

// ContractChainIndexElement returns the chain index element for the given height.
func (s *Store) ContractChainIndexElement(index types.ChainIndex) (element types.ChainIndexElement, err error) {
	err = s.transaction(func(tx *txn) error {
		err := tx.QueryRow(`SELECT leaf_index, merkle_proof FROM contracts_v2_chain_index_elements WHERE id=? AND height=?`, encode(index.ID), index.Height).Scan(decode(&element.StateElement.LeafIndex), decode(&element.StateElement.MerkleProof))
		element.ChainIndex = index
		element.ID = index.ID
		return err
	})
	return
}

// ExpireContractSectors expires all sectors that are no longer covered by an
// active contract.
func (s *Store) ExpireContractSectors(height uint64) error {
	log := s.log.Named("ExpireContractSectors").With(zap.Uint64("height", height))
	// delete in batches to avoid holding a lock on the database for too long
	for i := 0; ; i++ {
		expired, err := s.batchExpireContractSectors(height)
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if expired == 0 {
			return nil
		}
		log.Debug("removed sectors", zap.Int("expired", expired), zap.Int("batch", i))
		jitterSleep(50 * time.Millisecond) // allow other transactions to run
	}
}

// ExpireV2ContractSectors expires all sectors that are no longer covered by an
// active contract.
func (s *Store) ExpireV2ContractSectors(height uint64) error {
	log := s.log.Named("ExpireV2ContractSectors").With(zap.Uint64("height", height))
	// delete in batches to avoid holding a lock on the database for too long
	for i := 0; ; i++ {
		expired, err := s.batchExpireV2ContractSectors(height)
		if err != nil {
			return fmt.Errorf("failed to prune sectors: %w", err)
		} else if expired == 0 {
			return nil
		}
		log.Debug("removed sectors", zap.Int("expired", expired), zap.Int("batch", i))
		jitterSleep(50 * time.Millisecond) // allow other transactions to run
	}
}

func getContract(tx *txn, contractID int64) (contracts.Contract, error) {
	const query = `SELECT c.contract_id, rt.contract_id AS renewed_to, rf.contract_id AS renewed_from, c.contract_status, c.negotiation_height, c.formation_confirmed,
	COALESCE(c.revision_number=c.confirmed_revision_number, false) AS revision_confirmed, c.resolution_height, c.locked_collateral, c.rpc_revenue,
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

// appendSector appends a new sector root to a contract.
func appendSector(tx *txn, contractID int64, root types.Hash256, index uint64) error {
	var sectorID int64
	err := tx.QueryRow(`INSERT INTO contract_sector_roots (contract_id, sector_id, root_index) SELECT $1, id, $2 FROM stored_sectors WHERE sector_root=$3 RETURNING sector_id`, contractID, index, encode(root)).Scan(&sectorID)
	if err != nil {
		return err
	} else if err := incrementNumericStat(tx, metricContractSectors, 1, time.Now()); err != nil {
		return fmt.Errorf("failed to track contract sectors: %w", err)
	}
	return nil
}

// updateSector updates a contract sector root in place and returns the old sector root
func updateSector(tx *txn, contractID int64, root types.Hash256, index uint64) (types.Hash256, error) {
	row := tx.QueryRow(`SELECT csr.id, csr.sector_id, ss.sector_root
FROM contract_sector_roots csr
INNER JOIN stored_sectors ss ON (csr.sector_id = ss.id)
WHERE contract_id=$1 AND root_index=$2`, contractID, index)
	ref, err := scanContractSectorRootRef(row)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to get old sector id: %w", err)
	}

	var newSectorID int64
	err = tx.QueryRow(`SELECT id FROM stored_sectors WHERE sector_root=$1`, encode(root)).Scan(&newSectorID)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to get new sector id: %w", err)
	}

	// update the sector ID
	err = tx.QueryRow(`UPDATE contract_sector_roots
	SET sector_id=$1
	WHERE id=$2
	RETURNING sector_id;`, newSectorID, ref.dbID).Scan(&newSectorID)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to update sector ID: %w", err)
	}
	return ref.root, nil
}

// swapSectors swaps two sector roots in a contract and returns the sector roots
func swapSectors(tx *txn, contractID int64, i, j uint64) (map[types.Hash256]bool, error) {
	if i == j {
		return nil, nil
	}

	var records []contractSectorRootRef
	rows, err := tx.Query(`SELECT csr.id, csr.sector_id, ss.sector_root
FROM contract_sector_roots csr
INNER JOIN stored_sectors ss ON (ss.id = csr.sector_id)
WHERE contract_id=$1 AND root_index IN ($2, $3)
ORDER BY root_index ASC;`, contractID, i, j)
	if err != nil {
		return nil, fmt.Errorf("failed to query sector IDs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ref, err := scanContractSectorRootRef(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan sector ref: %w", err)
		}
		records = append(records, ref)
	}

	if len(records) != 2 {
		return nil, errors.New("failed to find both sectors")
	}

	stmt, err := tx.Prepare(`UPDATE contract_sector_roots SET sector_id=$1 WHERE id=$2 RETURNING sector_id;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	var newSectorID int64
	err = stmt.QueryRow(records[1].sectorID, records[0].dbID).Scan(&newSectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to update sector ID: %w", err)
	} else if newSectorID != records[1].sectorID {
		return nil, fmt.Errorf("expected sector ID %v, got %v", records[0].sectorID, newSectorID)
	}

	err = stmt.QueryRow(records[0].sectorID, records[1].dbID).Scan(&newSectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to update sector ID: %w", err)
	} else if newSectorID != records[0].sectorID {
		return nil, fmt.Errorf("expected sector ID %v, got %v", records[0].sectorID, newSectorID)
	}

	return map[types.Hash256]bool{
		records[0].root: true,
		records[1].root: true,
	}, nil
}

// trimSectors deletes the last n sector roots for a contract and returns the
// deleted sector roots in order.
func trimSectors(tx *txn, contractID int64, n uint64, log *zap.Logger) ([]types.Hash256, error) {
	selectStmt, err := tx.Prepare(`SELECT csr.id, csr.sector_id, ss.sector_root FROM contract_sector_roots csr
INNER JOIN stored_sectors ss ON (csr.sector_id=ss.id)
WHERE csr.contract_id=$1
ORDER BY root_index DESC
LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer selectStmt.Close()

	deleteStmt, err := tx.Prepare(`DELETE FROM contract_sector_roots WHERE id=$1;`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	roots := make([]types.Hash256, n)
	for i := 0; i < int(n); i++ {
		var contractSectorID int64
		var root types.Hash256
		var sectorID int64

		if err := selectStmt.QueryRow(contractID).Scan(&contractSectorID, &sectorID, decode(&root)); err != nil {
			return nil, fmt.Errorf("failed to get sector root: %w", err)
		} else if res, err := deleteStmt.Exec(contractSectorID); err != nil {
			return nil, fmt.Errorf("failed to delete sector root: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return nil, fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return nil, fmt.Errorf("expected 1 row affected, got %v", n)
		}

		roots[len(roots)-i-1] = root // reverse order
	}

	if err := incrementNumericStat(tx, metricContractSectors, -int(n), time.Now()); err != nil {
		return nil, fmt.Errorf("failed to decrement contract sectors: %w", err)
	}

	log.Debug("trimmed sectors", zap.Stringers("trimmed", roots))
	return roots, nil
}

func deleteExpiredContractSectors(tx *txn, height uint64) (sectorIDs []int64, err error) {
	const query = `DELETE FROM contract_sector_roots
WHERE id IN (SELECT csr.id FROM contract_sector_roots csr
INNER JOIN contracts c ON (csr.contract_id=c.id)
-- past proof window or not confirmed and past the rebroadcast height
WHERE c.window_end < $1 OR c.contract_status=$2 LIMIT $3)
RETURNING sector_id;`
	rows, err := tx.Query(query, height, contracts.ContractStatusRejected, sqlSectorBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		sectorIDs = append(sectorIDs, id)
	}
	return sectorIDs, nil
}

func deleteExpiredV2ContractSectors(tx *txn, height uint64) (sectorIDs []int64, err error) {
	const query = `DELETE FROM contract_v2_sector_roots
WHERE id IN (SELECT csr.id FROM contract_v2_sector_roots csr
INNER JOIN contracts_v2 c ON (csr.contract_id=c.id)
-- past expiration or not confirmed and past the rebroadcast height
WHERE c.expiration_height < $1 OR c.contract_status=$2 LIMIT $3)
RETURNING sector_id;`
	rows, err := tx.Query(query, height, contracts.ContractStatusRejected, sqlSectorBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		sectorIDs = append(sectorIDs, id)
	}
	return sectorIDs, nil
}

// updateResolvedV2Contract clears a contract and returns its ID
func updateResolvedV2Contract(tx *txn, contractID types.FileContractID, renewedDBID int64) (dbID int64, err error) {
	const clearQuery = `UPDATE contracts_v2 SET renewed_to=$1 WHERE contract_id=$2 RETURNING id;`
	err = tx.QueryRow(clearQuery,
		renewedDBID,
		encode(contractID),
	).Scan(&dbID)
	return
}

// clearContract clears a contract and returns its ID
func clearContract(tx *txn, revision contracts.SignedRevision, renewedDBID int64, usage contracts.Usage) (dbID int64, err error) {
	// update the existing contract
	const clearQuery = `UPDATE contracts SET (renewed_to, revision_number, host_sig, renter_sig, raw_revision) = ($1, $2, $3, $4, $5) WHERE contract_id=$6 RETURNING id;`
	err = tx.QueryRow(clearQuery,
		renewedDBID,
		encode(revision.Revision.RevisionNumber),
		encode(revision.HostSignature),
		encode(revision.RenterSignature),
		encode(revision.Revision),
		encode(revision.Revision.ParentID),
	).Scan(&dbID)
	if err != nil {
		return 0, fmt.Errorf("failed to update contract %q: %w", revision.Revision.ParentID, err)
	} else if err := updateContractUsage(tx, dbID, types.ZeroCurrency, usage); err != nil {
		return 0, fmt.Errorf("failed to update usage: %w", err)
	}
	return
}

// reviseContract revises a contract and returns its ID
func reviseContract(tx *txn, revision contracts.SignedRevision, usage contracts.Usage) (int64, error) {
	var contractID int64
	err := tx.QueryRow(`UPDATE contracts SET (revision_number, raw_revision, host_sig, renter_sig) = ($1, $2, $3, $4) WHERE contract_id=$6 RETURNING id;`,
		encode(revision.Revision.RevisionNumber),
		encode(revision.Revision),
		encode(revision.HostSignature),
		encode(revision.RenterSignature),
		encode(revision.Revision.ParentID),
	).Scan(&contractID)
	if err != nil {
		return 0, fmt.Errorf("failed to update contract: %w", err)
	} else if err := updateContractUsage(tx, contractID, types.ZeroCurrency, usage); err != nil {
		return 0, fmt.Errorf("failed to update contract usage: %w", err)
	}
	return contractID, nil
}

func updateContractUsage(tx *txn, contractID int64, lockedCollateral types.Currency, usage contracts.Usage) error {
	if err := incrementContractUsage(tx, contractID, usage); err != nil {
		return fmt.Errorf("failed to update contract usage: %w", err)
	}

	var status contracts.ContractStatus
	err := tx.QueryRow(`SELECT contract_status FROM contracts WHERE id=$1`, contractID).Scan(&status)
	if err != nil {
		return fmt.Errorf("failed to get contract status: %w", err)
	}

	if status == contracts.ContractStatusActive {
		incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
		if err != nil {
			return fmt.Errorf("failed to prepare increment currency stat statement: %w", err)
		}
		defer done()

		if err := updatePotentialRevenueMetrics(usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue: %w", err)
		} else if err := updateCollateralMetrics(lockedCollateral, usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	} else if status == contracts.ContractStatusSuccessful {
		incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
		if err != nil {
			return fmt.Errorf("failed to prepare increment currency stat statement: %w", err)
		}
		defer done()

		if err := updateEarnedRevenueMetrics(usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue: %w", err)
		}
	}
	return nil
}

func rebroadcastContracts(tx *txn) (rebroadcast [][]types.Transaction, err error) {
	rows, err := tx.Query(`SELECT formation_txn_set FROM contracts WHERE formation_confirmed=false AND contract_status <> ?`, contracts.ContractStatusRejected)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var buf []byte
		if err := rows.Scan(&buf); err != nil {
			return nil, fmt.Errorf("failed to scan contract id: %w", err)
		}
		var formationSet []types.Transaction
		if err := decodeTxnSet(buf, &formationSet); err != nil {
			return nil, fmt.Errorf("failed to decode formation txn set: %w", err)
		}
		rebroadcast = append(rebroadcast, formationSet)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func broadcastRevision(tx *txn, index types.ChainIndex, revisionBroadcastHeight uint64) (revisions []contracts.SignedRevision, err error) {
	const query = `SELECT raw_revision, host_sig, renter_sig
	FROM contracts
	WHERE formation_confirmed=true AND confirmed_revision_number != revision_number AND window_start BETWEEN ? AND ?`

	rows, err := tx.Query(query, index.Height, revisionBroadcastHeight)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rev contracts.SignedRevision
		err = rows.Scan(
			decode(&rev.Revision),
			decode(&rev.HostSignature),
			decode(&rev.RenterSignature))
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		revisions = append(revisions, rev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func proofContracts(tx *txn, index types.ChainIndex) (revisions []contracts.SignedRevision, err error) {
	const query = `SELECT raw_revision, host_sig, renter_sig
	FROM contracts
	WHERE formation_confirmed AND resolution_height IS NULL AND window_start <= $1 AND window_end > $1`

	rows, err := tx.Query(query, index.Height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		contract, err := scanSignedRevision(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		revisions = append(revisions, contract)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func rebroadcastV2Contracts(tx *txn) (rebroadcast []rhp4.TransactionSet, err error) {
	rows, err := tx.Query(`SELECT formation_txn_set, formation_txn_set_basis FROM contracts_v2 WHERE confirmation_index IS NULL AND contract_status <> ?`, contracts.ContractStatusRejected)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var formationSet rhp4.TransactionSet
		var buf []byte
		if err := rows.Scan(&buf, decode(&formationSet.Basis)); err != nil {
			return nil, fmt.Errorf("failed to scan contract id: %w", err)
		}
		dec := types.NewBufDecoder(buf)
		types.DecodeSlice(dec, &formationSet.Transactions)
		if err := dec.Err(); err != nil {
			return nil, fmt.Errorf("failed to decode formation txn set: %w", err)
		}
		rebroadcast = append(rebroadcast, formationSet)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func broadcastV2Revision(tx *txn, index types.ChainIndex, revisionBroadcastHeight uint64) (revisions []types.V2FileContractRevision, err error) {
	const query = `SELECT c.raw_revision, c.contract_id, cs.leaf_index, cs.merkle_proof, cs.raw_contract
	FROM contracts_v2 c
	INNER JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
	WHERE c.confirmation_index IS NOT NULL AND c.resolution_index IS NULL AND cs.revision_number != c.revision_number AND c.proof_height BETWEEN ? AND ?`

	rows, err := tx.Query(query, index.Height, revisionBroadcastHeight)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rev types.V2FileContractRevision

		err = rows.Scan(decode(&rev.Revision),
			decode(&rev.Parent.ID),
			decode(&rev.Parent.StateElement.LeafIndex),
			decode(&rev.Parent.StateElement.MerkleProof),
			decode(&rev.Parent.V2FileContract))
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		revisions = append(revisions, rev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func proofV2Contracts(tx *txn, index types.ChainIndex) (elements []types.V2FileContractElement, err error) {
	const query = `SELECT c.contract_id, cs.raw_contract, cs.leaf_index, cs.merkle_proof
	FROM contracts_v2 c
	INNER JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
	WHERE c.confirmation_index IS NOT NULL AND c.resolution_index IS NULL AND c.proof_height <= $1 AND c.expiration_height > $1`

	rows, err := tx.Query(query, index.Height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var fce types.V2FileContractElement
		if err := rows.Scan(decode(&fce.ID), decode(&fce.V2FileContract), decode(&fce.StateElement.LeafIndex), decode(&fce.StateElement.MerkleProof)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		elements = append(elements, fce)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func expireV2Contracts(tx *txn, index types.ChainIndex) (elements []types.V2FileContractElement, err error) {
	const query = `SELECT c.contract_id, cs.raw_contract, cs.leaf_index, cs.merkle_proof
	FROM contracts_v2 c
	INNER JOIN contract_v2_state_elements cs ON (c.id = cs.contract_id)
	WHERE c.resolution_index IS NULL AND c.expiration_height <= $1`

	rows, err := tx.Query(query, index.Height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var fce types.V2FileContractElement
		if err := rows.Scan(decode(&fce.ID), decode(&fce.V2FileContract), decode(&fce.StateElement.LeafIndex), decode(&fce.StateElement.MerkleProof)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		elements = append(elements, fce)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return
}

func incrementContractUsage(tx *txn, dbID int64, usage contracts.Usage) error {
	const query = `SELECT rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral FROM contracts WHERE id=$1;`
	var total contracts.Usage
	err := tx.QueryRow(query, dbID).Scan(
		decode(&total.RPCRevenue),
		decode(&total.StorageRevenue),
		decode(&total.IngressRevenue),
		decode(&total.EgressRevenue),
		decode(&total.AccountFunding),
		decode(&total.RiskedCollateral))
	if err != nil {
		return fmt.Errorf("failed to get existing revenue: %w", err)
	}
	total = total.Add(usage)
	var updatedID int64
	err = tx.QueryRow(`UPDATE contracts SET (rpc_revenue, storage_revenue, ingress_revenue, egress_revenue, account_funding, risked_collateral) = ($1, $2, $3, $4, $5, $6) WHERE id=$7 RETURNING id;`,
		encode(total.RPCRevenue),
		encode(total.StorageRevenue),
		encode(total.IngressRevenue),
		encode(total.EgressRevenue),
		encode(total.AccountFunding),
		encode(total.RiskedCollateral),
		dbID).Scan(&updatedID)
	if err != nil {
		return fmt.Errorf("failed to update contract revenue: %w", err)
	}
	return nil
}

func renterDBID(tx *txn, renterKey types.PublicKey) (int64, error) {
	var dbID int64
	err := tx.QueryRow(`SELECT id FROM contract_renters WHERE public_key=$1;`, encode(renterKey)).Scan(&dbID)
	if err == nil {
		return dbID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to get renter: %w", err)
	}
	err = tx.QueryRow(`INSERT INTO contract_renters (public_key) VALUES ($1) RETURNING id;`, encode(renterKey)).Scan(&dbID)
	return dbID, err
}

func insertContract(tx *txn, revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage, negotationHeight uint64) (dbID int64, err error) {
	const query = `INSERT INTO contracts (contract_id, renter_id, locked_collateral, rpc_revenue, storage_revenue, ingress_revenue,
egress_revenue, registry_read, registry_write, account_funding, risked_collateral, revision_number, negotiation_height, window_start, window_end, formation_txn_set,
raw_revision, host_sig, renter_sig, confirmed_revision_number, contract_status, formation_confirmed) VALUES
 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, false) RETURNING id;`
	renterID, err := renterDBID(tx, revision.RenterKey())
	if err != nil {
		return 0, fmt.Errorf("failed to get renter id: %w", err)
	}
	err = tx.QueryRow(query,
		encode(revision.Revision.ParentID),
		renterID,
		encode(lockedCollateral),
		encode(initialUsage.RPCRevenue),
		encode(initialUsage.StorageRevenue),
		encode(initialUsage.IngressRevenue),
		encode(initialUsage.EgressRevenue),
		encode(initialUsage.RegistryRead),
		encode(initialUsage.RegistryWrite),
		encode(initialUsage.AccountFunding),
		encode(initialUsage.RiskedCollateral),
		encode(revision.Revision.RevisionNumber),
		negotationHeight,              // stored as int64 for queries, should never overflow
		revision.Revision.WindowStart, // stored as int64 for queries, should never overflow
		revision.Revision.WindowEnd,   // stored as int64 for queries, should never overflow
		encodeTxnSet(formationSet),
		encode(revision.Revision),
		encode(revision.HostSignature),
		encode(revision.RenterSignature),
		encode(0), // confirmed_revision_number
		contracts.ContractStatusPending,
	).Scan(&dbID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert contract: %w", err)
	}
	return
}

func insertV2Contract(tx *txn, contract contracts.V2Contract, formationSet rhp4.TransactionSet) (dbID int64, err error) {
	const query = `INSERT INTO contracts_v2 (contract_id, renter_id, locked_collateral, rpc_revenue, storage_revenue, ingress_revenue,
egress_revenue, account_funding, risked_collateral, revision_number, negotiation_height, proof_height, expiration_height, formation_txn_set,
formation_txn_set_basis, raw_revision, contract_status) VALUES
 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING id;`
	renterID, err := renterDBID(tx, contract.RenterPublicKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get renter id: %w", err)
	}

	err = tx.QueryRow(query,
		encode(contract.ID),
		renterID,
		encode(contract.V2FileContract.TotalCollateral),
		encode(contract.Usage.RPC),
		encode(contract.Usage.Storage),
		encode(contract.Usage.Ingress),
		encode(contract.Usage.Egress),
		encode(contract.Usage.AccountFunding),
		encode(contract.Usage.RiskedCollateral),
		encode(contract.RevisionNumber),
		contract.NegotiationHeight,          // stored as int64 for queries, should never overflow
		contract.V2FileContract.ProofHeight, // stored as int64 for queries, should never overflow
		contract.ExpirationHeight,           // stored as int64 for queries, should never overflow
		encodeSlice(formationSet.Transactions),
		encode(formationSet.Basis),
		encode(contract.V2FileContract),
		contracts.V2ContractStatusPending,
	).Scan(&dbID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert contract: %w", err)
	}
	return
}

func updateContractSectors(tx *txn, contractDBID int64, oldRoots, newRoots []types.Hash256) error {
	selectRootIDStmt, err := tx.Prepare(`SELECT id FROM stored_sectors WHERE sector_root=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare select root ID statement: %w", err)
	}
	defer selectRootIDStmt.Close()

	updateRootStmt, err := tx.Prepare(`INSERT INTO contract_sector_roots (contract_id, sector_id, root_index) VALUES (?, ?, ?) ON CONFLICT (contract_id, root_index) DO UPDATE SET sector_id=excluded.sector_id`)
	if err != nil {
		return fmt.Errorf("failed to prepare update root statement: %w", err)
	}
	defer updateRootStmt.Close()

	for i, root := range newRoots {
		if i < len(oldRoots) && oldRoots[i] == root {
			continue
		}

		var newSectorID int64
		if err := selectRootIDStmt.QueryRow(encode(root)).Scan(&newSectorID); err != nil {
			return fmt.Errorf("failed to get sector ID: %w", err)
		} else if _, err := updateRootStmt.Exec(contractDBID, newSectorID, i); err != nil {
			return fmt.Errorf("failed to update sector root: %w", err)
		}
	}

	if len(newRoots) < len(oldRoots) {
		_, err := tx.Exec(`DELETE FROM contract_sector_roots WHERE contract_id=$1 AND root_index >= $2`, contractDBID, len(newRoots))
		if err != nil {
			return fmt.Errorf("failed to remove old roots: %w", err)
		}
	}

	delta := len(newRoots) - len(oldRoots)
	if err := incrementNumericStat(tx, metricContractSectors, delta, time.Now()); err != nil {
		return fmt.Errorf("failed to update contract sectors: %w", err)
	}
	return nil
}

func updateV2ContractUsage(tx *txn, contractDBID int64, usage proto4.Usage) error {
	if err := incrementV2ContractUsage(tx, contractDBID, usage); err != nil {
		return fmt.Errorf("failed to update contract usage: %w", err)
	}

	var status contracts.V2ContractStatus
	err := tx.QueryRow(`SELECT contract_status FROM contracts_v2 WHERE id=$1`, contractDBID).Scan(&status)
	if err != nil {
		return fmt.Errorf("failed to get contract status: %w", err)
	}

	if status == contracts.V2ContractStatusActive {
		incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
		if err != nil {
			return fmt.Errorf("failed to prepare increment currency stat statement: %w", err)
		}
		defer done()

		if err := updateV2PotentialRevenueMetrics(usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue: %w", err)
		} else if err := updateCollateralMetrics(types.ZeroCurrency, usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	} else if status == contracts.V2ContractStatusSuccessful || status == contracts.V2ContractStatusRenewed {
		incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
		if err != nil {
			return fmt.Errorf("failed to prepare increment currency stat statement: %w", err)
		}
		defer done()

		if err := updateV2EarnedRevenueMetrics(usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue: %w", err)
		}
	}
	return nil
}

func reviseV2Contract(tx *txn, id types.FileContractID, revision types.V2FileContract, usage proto4.Usage) (int64, error) {
	const updateQuery = `UPDATE contracts_v2 SET raw_revision=?, revision_number=? WHERE contract_id=? RETURNING id`

	var contractDBID int64
	err := tx.QueryRow(updateQuery, encode(revision), encode(revision.RevisionNumber), encode(id)).Scan(&contractDBID)
	if err != nil {
		return 0, fmt.Errorf("failed to update contract: %w", err)
	} else if err := updateV2ContractUsage(tx, contractDBID, usage); err != nil {
		return 0, fmt.Errorf("failed to update contract usage: %w", err)
	}
	return contractDBID, nil
}

func updateV2ContractSectors(tx *txn, contractDBID int64, oldRoots, newRoots []types.Hash256) error {
	selectRootIDStmt, err := tx.Prepare(`SELECT id FROM stored_sectors WHERE sector_root=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare select root ID statement: %w", err)
	}
	defer selectRootIDStmt.Close()

	updateRootStmt, err := tx.Prepare(`INSERT INTO contract_v2_sector_roots (contract_id, sector_id, root_index) VALUES (?, ?, ?) ON CONFLICT (contract_id, root_index) DO UPDATE SET sector_id=excluded.sector_id`)
	if err != nil {
		return fmt.Errorf("failed to prepare update root statement: %w", err)
	}
	defer updateRootStmt.Close()

	for i, root := range newRoots {
		if i < len(oldRoots) && oldRoots[i] == root {
			continue
		}

		var newSectorID int64
		if err := selectRootIDStmt.QueryRow(encode(root)).Scan(&newSectorID); err != nil {
			return fmt.Errorf("failed to get sector ID: %w", err)
		} else if _, err := updateRootStmt.Exec(contractDBID, newSectorID, i); err != nil {
			return fmt.Errorf("failed to update sector root: %w", err)
		}
	}

	if len(newRoots) < len(oldRoots) {
		_, err := tx.Exec(`DELETE FROM contract_v2_sector_roots WHERE contract_id=$1 AND root_index >= $2`, contractDBID, len(newRoots))
		if err != nil {
			return fmt.Errorf("failed to remove old roots: %w", err)
		}
	}

	delta := len(newRoots) - len(oldRoots)
	if err := incrementNumericStat(tx, metricContractSectors, delta, time.Now()); err != nil {
		return fmt.Errorf("failed to update contract sectors: %w", err)
	}
	return nil
}

func encodeTxnSet(txns []types.Transaction) []byte {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	types.EncodeSlice(e, txns)
	e.Flush()
	return buf.Bytes()
}

func decodeTxnSet(b []byte, txns *[]types.Transaction) error {
	d := types.NewBufDecoder(b)
	types.DecodeSlice(d, txns)
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
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenewedFrom) != 0 {
		whereClause = append(whereClause, `rf.contract_id IN (`+queryPlaceHolders(len(filter.RenewedFrom))+`)`)
		for _, value := range filter.RenewedFrom {
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenewedTo) != 0 {
		whereClause = append(whereClause, `rt.contract_id IN (`+queryPlaceHolders(len(filter.RenewedTo))+`)`)
		for _, value := range filter.RenewedTo {
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenterKey) != 0 {
		whereClause = append(whereClause, `r.public_key IN (`+queryPlaceHolders(len(filter.RenterKey))+`)`)
		for _, value := range filter.RenterKey {
			queryParams = append(queryParams, encode(value))
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

func buildV2ContractFilter(filter contracts.V2ContractFilter) (string, []any, error) {
	var whereClause []string
	var queryParams []any

	if len(filter.Statuses) != 0 {
		whereClause = append(whereClause, `c.contract_status IN (`+queryPlaceHolders(len(filter.Statuses))+`)`)
		queryParams = append(queryParams, queryArgs(filter.Statuses)...)
	}

	if len(filter.ContractIDs) != 0 {
		whereClause = append(whereClause, `c.contract_id IN (`+queryPlaceHolders(len(filter.ContractIDs))+`)`)
		for _, value := range filter.ContractIDs {
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenewedFrom) != 0 {
		whereClause = append(whereClause, `rf.contract_id IN (`+queryPlaceHolders(len(filter.RenewedFrom))+`)`)
		for _, value := range filter.RenewedFrom {
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenewedTo) != 0 {
		whereClause = append(whereClause, `rt.contract_id IN (`+queryPlaceHolders(len(filter.RenewedTo))+`)`)
		for _, value := range filter.RenewedTo {
			queryParams = append(queryParams, encode(value))
		}
	}

	if len(filter.RenterKey) != 0 {
		whereClause = append(whereClause, `r.public_key IN (`+queryPlaceHolders(len(filter.RenterKey))+`)`)
		for _, value := range filter.RenterKey {
			queryParams = append(queryParams, encode(value))
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
		whereClause = append(whereClause, `c.expiration_height BETWEEN ? AND ?`)
		queryParams = append(queryParams, filter.MinExpirationHeight, filter.MaxExpirationHeight)
	} else if filter.MinExpirationHeight > 0 {
		whereClause = append(whereClause, `c.expiration_height >= ?`)
		queryParams = append(queryParams, filter.MinExpirationHeight)
	} else if filter.MaxExpirationHeight > 0 {
		whereClause = append(whereClause, `c.expiration_height <= ?`)
		queryParams = append(queryParams, filter.MaxExpirationHeight)
	}
	if len(whereClause) == 0 {
		return "", nil, nil
	}
	return "WHERE " + strings.Join(whereClause, " AND "), queryParams, nil
}

func buildV2OrderBy(filter contracts.V2ContractFilter) string {
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
		return `ORDER BY c.expiration_height ` + dir
	}
}

func scanContract(row scanner) (c contracts.Contract, err error) {
	var contractID types.FileContractID
	err = row.Scan(decode(&contractID),
		decodeNullable(&c.RenewedTo),
		decodeNullable(&c.RenewedFrom),
		&c.Status,
		&c.NegotiationHeight,
		&c.FormationConfirmed,
		&c.RevisionConfirmed,
		decodeNullable(&c.ResolutionHeight),
		decode(&c.LockedCollateral),
		decode(&c.Usage.RPCRevenue),
		decode(&c.Usage.StorageRevenue),
		decode(&c.Usage.IngressRevenue),
		decode(&c.Usage.EgressRevenue),
		decode(&c.Usage.AccountFunding),
		decode(&c.Usage.RiskedCollateral),
		decode(&c.Revision),
		decode(&c.HostSignature),
		decode(&c.RenterSignature),
	)
	if err != nil {
		return contracts.Contract{}, fmt.Errorf("failed to scan contract: %w", err)
	} else if c.Revision.ParentID != contractID {
		panic("contract id mismatch")
	}
	return
}

func scanV2Contract(row scanner) (c contracts.V2Contract, err error) {
	err = row.Scan(decode(&c.ID),
		decodeNullable(&c.RenewedTo),
		decodeNullable(&c.RenewedFrom),
		&c.Status,
		&c.NegotiationHeight,
		decodeNullable(&c.FormationIndex),
		&c.RevisionConfirmed,
		decodeNullable(&c.ResolutionIndex),
		decode(&c.Usage.RPC),
		decode(&c.Usage.Storage),
		decode(&c.Usage.Ingress),
		decode(&c.Usage.Egress),
		decode(&c.Usage.AccountFunding),
		decode(&c.Usage.RiskedCollateral),
		decode(&c.V2FileContract),
	)
	if errors.Is(err, sql.ErrNoRows) {
		err = contracts.ErrNotFound
	}
	return
}

func scanSignedRevision(row scanner) (rev contracts.SignedRevision, err error) {
	err = row.Scan(
		decode(&rev.Revision),
		decode(&rev.HostSignature),
		decode(&rev.RenterSignature))
	return
}

func scanContractSectorRootRef(s scanner) (ref contractSectorRootRef, err error) {
	err = s.Scan(&ref.dbID, &ref.sectorID, decode(&ref.root))
	return
}
