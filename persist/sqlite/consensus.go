package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/index"
	"go.uber.org/zap"
)

type (
	updateTx struct {
		tx *txn

		relevant map[types.Hash256]bool // map to prevent rare duplicate selects
	}

	contractState struct {
		ID               int64
		LockedCollateral types.Currency
		Usage            contracts.Usage
		Status           contracts.ContractStatus
	}

	v2ContractState struct {
		ID               int64
		LockedCollateral types.Currency
		Usage            proto4.Usage
		Status           contracts.V2ContractStatus
	}

	stateElement struct {
		ID types.Hash256
		types.StateElement
	}

	contractStateElement struct {
		ID int64
		types.StateElement
	}
)

var _ index.UpdateTx = (*updateTx)(nil)

// ResetChainState resets the consensus state of the store. This
// should only occur if the user has reset their consensus database to
// sync from scratch.
func (s *Store) ResetChainState() error {
	return s.transaction(func(tx *txn) error {
		_, err := tx.Exec(`
-- v2 contracts
DELETE FROM contracts_v2_chain_index_elements;
DELETE FROM contract_v2_state_elements;
-- wallet
DELETE FROM wallet_siacoin_elements;
DELETE FROM wallet_events;
DELETE FROM host_stats WHERE stat IN (?,?); -- reset wallet stats since they are derived from the chain
-- settings
UPDATE global_settings SET last_scanned_index=NULL, last_announce_index=NULL, last_announce_address=NULL`, metricWalletBalance, metricWalletImmatureBalance)
		return err
	})
}

func getSiacoinStateElements(tx *txn) (elements []stateElement, err error) {
	const query = `SELECT id, leaf_index, merkle_proof FROM wallet_siacoin_elements`
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query siacoin elements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var se stateElement
		if err := rows.Scan(decode(&se.ID), decode(&se.LeafIndex), decode(&se.MerkleProof)); err != nil {
			return nil, fmt.Errorf("failed to scan siacoin element: %w", err)
		}
		elements = append(elements, se)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan siacoin elements: %w", err)
	}
	return elements, nil
}

func updateSiacoinStateElements(tx *txn, elements []stateElement) error {
	stmt, err := tx.Prepare(`UPDATE wallet_siacoin_elements SET merkle_proof=?, leaf_index=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	for _, se := range elements {
		if res, err := stmt.Exec(encode(se.MerkleProof), encode(se.LeafIndex), encode(se.ID)); err != nil {
			return fmt.Errorf("failed to update siacoin element: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update siacoin element %q: not found", se.ID)
		}
	}
	return nil
}

func getContractStateElements(tx *txn) (elements []contractStateElement, err error) {
	const query = `SELECT contract_id, leaf_index, merkle_proof FROM contract_v2_state_elements`
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query siacoin elements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var se contractStateElement
		if err := rows.Scan(&se.ID, decode(&se.LeafIndex), decode(&se.MerkleProof)); err != nil {
			return nil, fmt.Errorf("failed to scan siacoin element: %w", err)
		}
		elements = append(elements, se)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan siacoin elements: %w", err)
	}
	return elements, nil
}

func updateContractStateElements(tx *txn, elements []contractStateElement) error {
	stmt, err := tx.Prepare(`UPDATE contract_v2_state_elements SET merkle_proof=?, leaf_index=? WHERE contract_id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	for _, se := range elements {
		if res, err := stmt.Exec(encode(se.MerkleProof), encode(se.LeafIndex), se.ID); err != nil {
			return fmt.Errorf("failed to update siacoin element: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update siacoin element %q: not found", se.ID)
		}
	}
	return nil
}

func getChainStateElements(tx *txn) (elements []stateElement, err error) {
	const query = `SELECT id, leaf_index, merkle_proof FROM contracts_v2_chain_index_elements`
	rows, err := tx.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query siacoin elements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var se stateElement
		if err := rows.Scan(decode(&se.ID), decode(&se.LeafIndex), decode(&se.MerkleProof)); err != nil {
			return nil, fmt.Errorf("failed to scan siacoin element: %w", err)
		}
		elements = append(elements, se)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan siacoin elements: %w", err)
	}
	return elements, nil
}

func updateChainStateElements(tx *txn, elements []stateElement) error {
	stmt, err := tx.Prepare(`UPDATE contracts_v2_chain_index_elements SET merkle_proof=?, leaf_index=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	for _, se := range elements {
		if res, err := stmt.Exec(encode(se.MerkleProof), encode(se.LeafIndex), encode(se.ID)); err != nil {
			return fmt.Errorf("failed to update siacoin element: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update siacoin element %q: not found", se.ID)
		}
	}
	return nil
}

// UpdateWalletSiacoinElementProofs updates the proofs of all state elements
// affected by the update. ProofUpdater.UpdateElementProof must be called
// for each state element in the database.
func (ux *updateTx) UpdateWalletSiacoinElementProofs(updater wallet.ProofUpdater) error {
	se, err := getSiacoinStateElements(ux.tx)
	if err != nil {
		return fmt.Errorf("failed to get siacoin state elements: %w", err)
	}
	for i := range se {
		updater.UpdateElementProof(&se[i].StateElement)
	}
	return updateSiacoinStateElements(ux.tx, se)
}

// UpdateContractElementProofs updates the proofs of all state elements
// affected by the update. ProofUpdater.UpdateElementProof must be called
// for each state element in the database.
func (ux *updateTx) UpdateContractElementProofs(updater wallet.ProofUpdater) error {
	se, err := getContractStateElements(ux.tx)
	if err != nil {
		return fmt.Errorf("failed to get contract state elements: %w", err)
	}
	for i := range se {
		updater.UpdateElementProof(&se[i].StateElement)
	}
	return updateContractStateElements(ux.tx, se)
}

// UpdateChainIndexElementProofs updates the proofs of all state elements affected
// by the update. ProofUpdater.UpdateElementProof must be called for each state
// element in the database.
func (ux *updateTx) UpdateChainIndexElementProofs(updater wallet.ProofUpdater) error {
	se, err := getChainStateElements(ux.tx)
	if err != nil {
		return fmt.Errorf("failed to get contract state elements: %w", err)
	}
	for i := range se {
		updater.UpdateElementProof(&se[i].StateElement)
	}
	return updateChainStateElements(ux.tx, se)
}

// WalletApplyIndex is called with the chain index that is being applied.
// Any transactions and siacoin elements that were created by the index
// should be added and any siacoin elements that were spent should be
// removed.
func (ux *updateTx) WalletApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event, timestamp time.Time) error {
	matureOutflow, immatureOutflow, err := deleteSiacoinElements(ux.tx, index, spent)
	if err != nil {
		return fmt.Errorf("failed to delete siacoin elements: %w", err)
	}
	matureInflow, immatureInflow, err := createSiacoinElements(ux.tx, index, created)
	if err != nil {
		return fmt.Errorf("failed to create siacoin elements: %w", err)
	} else if err := createWalletEvents(ux.tx, events); err != nil {
		return fmt.Errorf("failed to create wallet events: %w", err)
	}

	// get the matured balance
	matured, err := maturedSiacoinBalance(ux.tx, index)
	if err != nil {
		return fmt.Errorf("failed to query matured siacoin balance: %w", err)
	}
	// apply the maturation by adding the matured balance to the matured inflow
	// and the immature outflow
	matureInflow = matureInflow.Add(matured)
	immatureOutflow = immatureOutflow.Add(matured)
	// update the balance metrics
	if err := updateBalanceMetric(ux.tx, matureInflow, matureOutflow, immatureInflow, immatureOutflow, timestamp); err != nil {
		return fmt.Errorf("failed to update wallet balance: %w", err)
	}
	return nil
}

// WalletRevertIndex is called with the chain index that is being reverted.
// Any transactions that were added by the index should be removed
//
// removed contains the siacoin elements that were created by the index
// and should be deleted.
//
// unspent contains the siacoin elements that were spent and should be
// recreated. They are not necessarily created by the index and should
// not be associated with it.
func (ux *updateTx) WalletRevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement, timestamp time.Time) error {
	matureOutflow, immatureOutflow, err := deleteSiacoinElements(ux.tx, index, removed)
	if err != nil {
		return fmt.Errorf("failed to delete siacoin elements: %w", err)
	}
	matureInflow, immatureInflow, err := createSiacoinElements(ux.tx, index, unspent)
	if err != nil {
		return fmt.Errorf("failed to create siacoin elements: %w", err)
	} else if _, err := ux.tx.Exec(`DELETE FROM wallet_events WHERE chain_index=?`, encode(index)); err != nil {
		return fmt.Errorf("failed to delete wallet events: %w", err)
	}

	// get the matured balance
	matured, err := maturedSiacoinBalance(ux.tx, index)
	if err != nil {
		return fmt.Errorf("failed to query matured siacoin balance: %w", err)
	}
	// revert the maturation by adding the matured balance to the matured outflow
	// and the immature inflow
	matureOutflow = matureOutflow.Add(matured)
	immatureInflow = immatureInflow.Add(matured)
	// update the balance metrics
	if err := updateBalanceMetric(ux.tx, matureInflow, matureOutflow, immatureInflow, immatureOutflow, timestamp); err != nil {
		return fmt.Errorf("failed to increment wallet balance: %w", err)
	}
	return nil
}

// RevertContractChainIndexElement removes a reverted chain index from the store
func (ux *updateTx) RevertContractChainIndexElement(index types.ChainIndex) error {
	_, err := ux.tx.Exec(`DELETE FROM contracts_v2_chain_index_elements WHERE height=? AND id=?`, index.Height, encode(index.ID))
	return err
}

// AddContractChainIndexElement adds or updates the merkle proof of
// chain index state elements
func (ux *updateTx) AddContractChainIndexElement(ci types.ChainIndexElement) error {
	_, err := ux.tx.Exec(`INSERT INTO contracts_v2_chain_index_elements (id, height, merkle_proof, leaf_index) VALUES (?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET merkle_proof=EXCLUDED.merkle_proof, leaf_index=EXCLUDED.leaf_index, height=EXCLUDED.height`, encode(ci.ChainIndex.ID), ci.ChainIndex.Height, encode(ci.StateElement.MerkleProof), encode(ci.StateElement.LeafIndex))
	return err
}

// DeleteExpiredChainIndexElements deletes chain index state
// elements that are no long necessary
func (ux *updateTx) DeleteExpiredChainIndexElements(height uint64) error {
	_, err := ux.tx.Exec(`DELETE FROM contracts_v2_chain_index_elements WHERE height <= ?`, height)
	return err
}

// ApplyContracts applies relevant contract changes to the contract
// store
func (ux *updateTx) ApplyContracts(index types.ChainIndex, state contracts.StateChanges) error {
	log := ux.tx.log.Named("ApplyV1Contracts")
	if err := applyContractFormation(ux.tx, state.Confirmed, log.Named("formation")); err != nil {
		return fmt.Errorf("failed to apply contract formation: %w", err)
	} else if err := applyContractRevision(ux.tx, state.Revised); err != nil {
		return fmt.Errorf("failed to apply contract revisions: %w", err)
	} else if err := applySuccessfulContracts(ux.tx, index, state.Successful, log.Named("successful")); err != nil {
		return fmt.Errorf("failed to apply contract resolution: %w", err)
	} else if err := applyFailedContracts(ux.tx, state.Failed, log.Named("failed")); err != nil {
		return fmt.Errorf("failed to apply contract failures: %w", err)
	}

	// v2
	log = ux.tx.log.Named("ApplyV2Contracts")
	if err := applyV2ContractFormation(ux.tx, index, state.ConfirmedV2, log.Named("formation")); err != nil {
		return fmt.Errorf("failed to apply v2 contract formation: %w", err)
	} else if err := applyV2ContractRevision(ux.tx, state.RevisedV2); err != nil {
		return fmt.Errorf("failed to apply v2 contract revisions: %w", err)
	} else if err := applySuccessfulV2Contracts(ux.tx, index, contracts.V2ContractStatusSuccessful, state.SuccessfulV2, log.Named("successful")); err != nil {
		return fmt.Errorf("failed to apply successful v2 resolution: %w", err)
	} else if err := applySuccessfulV2Contracts(ux.tx, index, contracts.V2ContractStatusRenewed, state.RenewedV2, log.Named("renewed")); err != nil {
		return fmt.Errorf("failed to apply v2 renewed v2 resolution: %w", err)
	} else if err := applyFailedV2Contracts(ux.tx, index, state.FailedV2, log.Named("failed")); err != nil {
		return fmt.Errorf("failed to apply v2 failure resolution: %w", err)
	}
	return nil
}

// RevertContracts reverts relevant contract changes from the contract
// store
func (ux *updateTx) RevertContracts(index types.ChainIndex, state contracts.StateChanges) error {
	if err := revertContractFormation(ux.tx, state.Confirmed); err != nil {
		return fmt.Errorf("failed to revert contract formation: %w", err)
	} else if err := applyContractRevision(ux.tx, state.Revised); err != nil { // note: this is correct. The previous revision is being applied
		return fmt.Errorf("failed to revert contract revisions: %w", err)
	} else if err := revertSuccessfulContracts(ux.tx, state.Successful); err != nil {
		return fmt.Errorf("failed to revert contract resolution: %w", err)
	} else if err := revertFailedContracts(ux.tx, state.Failed); err != nil {
		return fmt.Errorf("failed to revert contract failures: %w", err)
	}

	// v2
	if err := revertV2ContractFormation(ux.tx, state.ConfirmedV2); err != nil {
		return fmt.Errorf("failed to revert v2 contract formation: %w", err)
	} else if err := applyV2ContractRevision(ux.tx, state.RevisedV2); err != nil { // note: this is correct. The previous revision is being applied
		return fmt.Errorf("failed to revert v2 contract revisions: %w", err)
	} else if err := revertSuccessfulV2Contracts(ux.tx, contracts.V2ContractStatusSuccessful, state.SuccessfulV2); err != nil {
		return fmt.Errorf("failed to revert v2 successful resolution: %w", err)
	} else if err := revertSuccessfulV2Contracts(ux.tx, contracts.V2ContractStatusRenewed, state.RenewedV2); err != nil {
		return fmt.Errorf("failed to revert v2 renewed resolution: %w", err)
	} else if err := revertFailedV2Contracts(ux.tx, state.FailedV2); err != nil {
		return fmt.Errorf("failed to revert v2 failure resolution: %w", err)
	}
	return nil
}

// RejectContracts returns any contracts with a negotiation height
// before the provided height that have not been confirmed.
func (ux *updateTx) RejectContracts(height uint64) ([]types.FileContractID, []types.FileContractID, error) {
	rejected, err := rejectContracts(ux.tx, height)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get rejected contracts: %w", err)
	}

	rejectedV2, err := rejectV2Contracts(ux.tx, height)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get rejected v2 contracts: %w", err)
	}

	if len(rejected) == 0 && len(rejectedV2) == 0 {
		return nil, nil, nil
	}

	contractState, stateDone, err := getContractStateStmt(ux.tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer stateDone()

	contractStateV2, stateDoneV2, err := getV2ContractStateStmt(ux.tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer stateDoneV2()

	incrementNumericStat, numericStatDone, err := incrementNumericStatStmt(ux.tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer numericStatDone()

	updateV1Status, err := ux.tx.Prepare(`UPDATE contracts SET contract_status=? WHERE id=?`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateV1Status.Close()

	updateV2Status, err := ux.tx.Prepare(`UPDATE contracts_v2 SET contract_status=? WHERE id=?`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare v2 update statement: %w", err)
	}
	defer updateV2Status.Close()

	for _, id := range rejected {
		state, err := contractState(id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get contract state %q: %w", id, err)
		}

		if state.Status != contracts.ContractStatusPending {
			// orderly applies and reverts should prevent this from happening
			panic(fmt.Sprintf("unexpected contract status %v", state.Status))
		}

		// update metrics
		if _, err := updateV1Status.Exec(contracts.ContractStatusRejected, state.ID); err != nil {
			return nil, nil, fmt.Errorf("failed to update contract status: %w", err)
		} else if err := updateStatusMetrics(state.Status, contracts.ContractStatusRejected, incrementNumericStat); err != nil {
			return nil, nil, fmt.Errorf("failed to update contract metrics: %w", err)
		}
	}
	for _, id := range rejectedV2 {
		state, err := contractStateV2(id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get contract state %q: %w", id, err)
		}

		if state.Status != contracts.V2ContractStatusPending {
			// orderly applies and reverts should prevent this from happening
			panic(fmt.Sprintf("unexpected contract status %v", state.Status))
		}

		// update metrics
		if _, err := updateV2Status.Exec(contracts.V2ContractStatusRejected, state.ID); err != nil {
			return nil, nil, fmt.Errorf("failed to update contract status: %w", err)
		} else if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusRejected, incrementNumericStat); err != nil {
			return nil, nil, fmt.Errorf("failed to update contract metrics: %w", err)
		}
	}
	return rejected, rejectedV2, nil
}

// ContractRelevant returns true if a contract is relevant to the host. Otherwise,
// it returns false.
func (ux *updateTx) ContractRelevant(id types.FileContractID) (relevant bool, err error) {
	if ux.relevant == nil {
		ux.relevant = make(map[types.Hash256]bool)
	}

	if relevant, ok := ux.relevant[types.Hash256(id)]; ok {
		return relevant, nil
	}

	err = ux.tx.QueryRow(`SELECT 1 FROM contracts WHERE contract_id=?`, encode(id)).Scan(&relevant)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	ux.relevant[types.Hash256(id)] = relevant
	return
}

// V2ContractRelevant returns true if the v2 contract is relevant to the host.
// Otherwise, it returns false.
func (ux *updateTx) V2ContractRelevant(id types.FileContractID) (relevant bool, err error) {
	if ux.relevant == nil {
		ux.relevant = make(map[types.Hash256]bool)
	}

	if relevant, ok := ux.relevant[types.Hash256(id)]; ok {
		return relevant, nil
	}

	err = ux.tx.QueryRow(`SELECT 1 FROM contracts_v2 WHERE contract_id=?`, encode(id)).Scan(&relevant)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	ux.relevant[types.Hash256(id)] = relevant
	return
}

func (ux *updateTx) LastAnnouncement() (announcement settings.Announcement, err error) {
	var addr sql.NullString
	err = ux.tx.QueryRow(`SELECT last_announce_index, last_announce_address FROM global_settings`).Scan(decodeNullable(&announcement.Index), &addr)
	if addr.Valid {
		announcement.Address = addr.String
	}
	return
}

func (ux *updateTx) LastV2AnnouncementHash() (h types.Hash256, index types.ChainIndex, err error) {
	err = ux.tx.QueryRow(`SELECT last_v2_announce_hash, last_announce_index FROM global_settings`).
		Scan(decodeNullable(&h), decodeNullable(&index))
	if errors.Is(err, sql.ErrNoRows) {
		return types.Hash256{}, types.ChainIndex{}, nil
	}
	return
}

func (ux *updateTx) RevertLastV2Announcement() error {
	_, err := ux.tx.Exec(`UPDATE global_settings SET last_v2_announce_hash=NULL, last_announce_index=NULL`)
	return err
}

func (ux *updateTx) SetLastV2AnnouncementHash(h types.Hash256, index types.ChainIndex) error {
	_, err := ux.tx.Exec(`UPDATE global_settings SET last_announce_index=?, last_v2_announce_hash=?`, encode(index), encode(h))
	return err
}

func (ux *updateTx) RevertLastAnnouncement() error {
	_, err := ux.tx.Exec(`UPDATE global_settings SET last_announce_address=NULL, last_announce_index=NULL`)
	return err
}

func (ux *updateTx) SetLastAnnouncement(announcement settings.Announcement) error {
	_, err := ux.tx.Exec(`UPDATE global_settings SET last_announce_index=?, last_announce_address=?`, encode(announcement.Index), announcement.Address)
	return err
}

func (ux *updateTx) SetLastIndex(index types.ChainIndex) error {
	_, err := ux.tx.Exec(`UPDATE global_settings SET last_scanned_index=?`, encode(index))
	return err
}

// Tip returns the last scanned chain index.
func (s *Store) Tip() (index types.ChainIndex, err error) {
	err = s.transaction(func(tx *txn) error {
		err = tx.QueryRow(`SELECT last_scanned_index FROM global_settings`).Scan(decodeNullable(&index))
		return err
	})
	return
}

// UpdateChainState updates the chain state with the given updates.
func (s *Store) UpdateChainState(fn func(index.UpdateTx) error) error {
	return s.transaction(func(tx *txn) error {
		return fn(&updateTx{tx: tx})
	})
}

// maturedSiacoinBalance helper to query the sum of matured siacoin elements
// for a given height.
func maturedSiacoinBalance(tx *txn, index types.ChainIndex) (inflow types.Currency, err error) {
	rows, err := tx.Query(`SELECT siacoin_value FROM wallet_siacoin_elements WHERE maturity_height=?`, index.Height)
	if err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to query matured siacoin elements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var value types.Currency
		if err := rows.Scan(decode(&value)); err != nil {
			return types.ZeroCurrency, fmt.Errorf("failed to scan siacoin value: %w", err)
		}
		inflow = inflow.Add(value)
	}
	if err := rows.Err(); err != nil {
		return types.ZeroCurrency, fmt.Errorf("failed to iterate siacoin elements: %w", err)
	}
	return
}

// createSiacoinElements helper to insert siacoin elements into the database.
func createSiacoinElements(tx *txn, index types.ChainIndex, created []types.SiacoinElement) (matureInflow, immatureInflow types.Currency, _ error) {
	if len(created) == 0 {
		return types.ZeroCurrency, types.ZeroCurrency, nil
	}

	stmt, err := tx.Prepare(`INSERT INTO wallet_siacoin_elements (id, siacoin_value, sia_address, merkle_proof, leaf_index, maturity_height) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING;`)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, elem := range created {
		if _, err := stmt.Exec(encode(elem.ID), encode(elem.SiacoinOutput.Value), encode(elem.SiacoinOutput.Address), encode(elem.StateElement.MerkleProof), encode(elem.StateElement.LeafIndex), elem.MaturityHeight); err != nil {
			return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to insert siacoin element %q: %w", elem.ID, err)
		}

		if elem.MaturityHeight <= index.Height {
			matureInflow = matureInflow.Add(elem.SiacoinOutput.Value)
		} else {
			immatureInflow = immatureInflow.Add(elem.SiacoinOutput.Value)
		}
	}
	return
}

// deleteSiacoinElements helper to delete siacoin elements from the database.
func deleteSiacoinElements(tx *txn, index types.ChainIndex, removed []types.SiacoinElement) (matureOutflow types.Currency, immatureOutflow types.Currency, _ error) {
	if len(removed) == 0 {
		return types.ZeroCurrency, types.ZeroCurrency, nil
	}

	stmt, err := tx.Prepare(`DELETE FROM wallet_siacoin_elements WHERE id=?`)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer stmt.Close()

	for _, elem := range removed {
		if res, err := stmt.Exec(encode(elem.ID)); err != nil {
			return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to delete siacoin element: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to delete siacoin element %q: not found", elem.ID)
		}

		if elem.MaturityHeight <= index.Height {
			matureOutflow = matureOutflow.Add(elem.SiacoinOutput.Value)
		} else {
			immatureOutflow = immatureOutflow.Add(elem.SiacoinOutput.Value)
		}
	}
	return
}

// createWalletEvents helper to insert wallet events into the database.
func createWalletEvents(tx *txn, events []wallet.Event) error {
	if len(events) == 0 {
		return nil
	}

	stmt, err := tx.Prepare(`INSERT INTO wallet_events (id, chain_index, maturity_height, event_type, raw_data) VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	buf := bytes.NewBuffer(nil)
	enc := types.NewEncoder(buf)
	for _, event := range events {
		buf.Reset()
		event.EncodeTo(enc)
		_ = enc.Flush() // writes cannot fail

		if _, err := stmt.Exec(encode(event.ID), encode(event.Index), event.MaturityHeight, event.Type, buf.Bytes()); err != nil {
			return fmt.Errorf("failed to insert wallet event: %w", err)
		}
	}
	return nil
}

// updateBalanceMetric updates the wallet balance metric.
func updateBalanceMetric(tx *txn, matureInflow, matureOutflow, immatureInflow, immatureOutflow types.Currency, timestamp time.Time) error {
	// calculate the delta for the balance and immature balance
	var matureDelta types.Currency
	var matureNegative bool
	if n := matureInflow.Cmp(matureOutflow); n > 0 {
		matureDelta = matureInflow.Sub(matureOutflow)
	} else if n < 0 {
		matureDelta = matureOutflow.Sub(matureInflow)
		matureNegative = true
	}

	var immatureDelta types.Currency
	var immatureNegative bool
	if n := immatureInflow.Cmp(immatureOutflow); n > 0 {
		immatureDelta = immatureInflow.Sub(immatureOutflow)
	} else if n < 0 {
		immatureDelta = immatureOutflow.Sub(immatureInflow)
		immatureNegative = true
	}

	// if no change, return
	if matureDelta.IsZero() && immatureDelta.IsZero() {
		return nil
	}

	// prepare the increment statement
	increment, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	if !matureDelta.IsZero() {
		// increment the balance
		if err := increment(metricWalletBalance, matureDelta, matureNegative, timestamp); err != nil {
			return fmt.Errorf("failed to increment balance: %w", err)
		}
	}

	if !immatureDelta.IsZero() {
		// increment the immature balance
		if err := increment(metricWalletImmatureBalance, immatureDelta, immatureNegative, timestamp); err != nil {
			return fmt.Errorf("failed to increment immature balance: %w", err)
		}
	}
	return nil
}

// getContractStateStmt helper to get the current state of a contract.
func getContractStateStmt(tx *txn) (func(contractID types.FileContractID) (contractState, error), func() error, error) {
	stmt, err := tx.Prepare(`SELECT id, locked_collateral, risked_collateral, rpc_revenue, storage_revenue,
ingress_revenue, egress_revenue, registry_read, registry_write, contract_status
FROM contracts
WHERE contract_id=?`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}

	return func(contractID types.FileContractID) (state contractState, err error) {
		err = stmt.QueryRow(encode(contractID)).Scan(&state.ID,
			decode(&state.LockedCollateral), decode(&state.Usage.RiskedCollateral), decode(&state.Usage.RPCRevenue),
			decode(&state.Usage.StorageRevenue), decode(&state.Usage.IngressRevenue), decode(&state.Usage.EgressRevenue),
			decode(&state.Usage.RegistryRead), decode(&state.Usage.RegistryWrite), &state.Status)
		return
	}, stmt.Close, nil
}

// getV2ContractStateStmt helper to get the current state of a v2 contract.
func getV2ContractStateStmt(tx *txn) (func(contractID types.FileContractID) (v2ContractState, error), func() error, error) {
	stmt, err := tx.Prepare(`SELECT id, locked_collateral, risked_collateral, rpc_revenue, storage_revenue,
ingress_revenue, egress_revenue, contract_status
FROM contracts_v2
WHERE contract_id=?`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}

	return func(contractID types.FileContractID) (state v2ContractState, err error) {
		err = stmt.QueryRow(encode(contractID)).Scan(&state.ID,
			decode(&state.LockedCollateral), decode(&state.Usage.RiskedCollateral), decode(&state.Usage.RPC),
			decode(&state.Usage.Storage), decode(&state.Usage.Ingress), decode(&state.Usage.Egress),
			&state.Status)
		return
	}, stmt.Close, nil
}

// updateEarnedRevenueMetrics helper to update the earned revenue metrics.
func updateEarnedRevenueMetrics(usage contracts.Usage, negative bool, fn func(stat string, delta types.Currency, negative bool, timestamp time.Time) error) error {
	if err := fn(metricEarnedRPCRevenue, usage.RPCRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedRPCRevenue, err)
	} else if err := fn(metricEarnedStorageRevenue, usage.StorageRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedStorageRevenue, err)
	} else if err := fn(metricEarnedIngressRevenue, usage.IngressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedIngressRevenue, err)
	} else if err := fn(metricEarnedEgressRevenue, usage.EgressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedEgressRevenue, err)
	} else if err := fn(metricEarnedRegistryReadRevenue, usage.RegistryRead, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedRegistryReadRevenue, err)
	} else if err := fn(metricEarnedRegistryWriteRevenue, usage.RegistryWrite, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedRegistryWriteRevenue, err)
	}
	return nil
}

// updatePotentialRevenueMetrics helper to update the potential revenue metrics.
func updatePotentialRevenueMetrics(usage contracts.Usage, negative bool, fn func(stat string, delta types.Currency, negative bool, timestamp time.Time) error) error {
	if err := fn(metricPotentialRPCRevenue, usage.RPCRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialRPCRevenue, err)
	} else if err := fn(metricPotentialStorageRevenue, usage.StorageRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialStorageRevenue, err)
	} else if err := fn(metricPotentialIngressRevenue, usage.IngressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialIngressRevenue, err)
	} else if err := fn(metricPotentialEgressRevenue, usage.EgressRevenue, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialEgressRevenue, err)
	} else if err := fn(metricPotentialRegistryReadRevenue, usage.RegistryRead, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialRegistryReadRevenue, err)
	} else if err := fn(metricPotentialRegistryWriteRevenue, usage.RegistryWrite, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialRegistryWriteRevenue, err)
	}
	return nil
}

// updateV2EarnedRevenueMetrics helper to update the earned revenue metrics.
func updateV2EarnedRevenueMetrics(usage proto4.Usage, negative bool, fn func(stat string, delta types.Currency, negative bool, timestamp time.Time) error) error {
	if err := fn(metricEarnedRPCRevenue, usage.RPC, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedRPCRevenue, err)
	} else if err := fn(metricEarnedStorageRevenue, usage.Storage, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedStorageRevenue, err)
	} else if err := fn(metricEarnedIngressRevenue, usage.Ingress, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedIngressRevenue, err)
	} else if err := fn(metricEarnedEgressRevenue, usage.Egress, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricEarnedEgressRevenue, err)
	}
	return nil
}

// updateV2PotentialRevenueMetrics helper to update the potential revenue metrics.
func updateV2PotentialRevenueMetrics(usage proto4.Usage, negative bool, fn func(stat string, delta types.Currency, negative bool, timestamp time.Time) error) error {
	if err := fn(metricPotentialRPCRevenue, usage.RPC, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialRPCRevenue, err)
	} else if err := fn(metricPotentialStorageRevenue, usage.Storage, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialStorageRevenue, err)
	} else if err := fn(metricPotentialIngressRevenue, usage.Ingress, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialIngressRevenue, err)
	} else if err := fn(metricPotentialEgressRevenue, usage.Egress, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to update metric %q: %w", metricPotentialEgressRevenue, err)
	}
	return nil
}

// updateCollateralMetrics helper to update the collateral metrics.
func updateCollateralMetrics(locked, risked types.Currency, negative bool, fn func(stat string, delta types.Currency, negative bool, timestamp time.Time) error) error {
	if err := fn(metricLockedCollateral, locked, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to set metric %q: %w", metricLockedCollateral, err)
	} else if err := fn(metricRiskedCollateral, risked, negative, time.Now()); err != nil {
		return fmt.Errorf("failed to set metric %q: %w", metricRiskedCollateral, err)
	}
	return nil
}

// contractStatusMetric returns the metric name for a contract status.
func contractStatusMetric(status contracts.ContractStatus) string {
	switch status {
	case contracts.ContractStatusActive:
		return metricActiveContracts
	case contracts.ContractStatusRejected:
		return metricRejectedContracts
	case contracts.ContractStatusSuccessful:
		return metricSuccessfulContracts
	case contracts.ContractStatusFailed:
		return metricFailedContracts
	default:
		panic(fmt.Sprintf("unexpected contract status: %v", status))
	}
}

// updateStatusMetrics helper to update the contract status metrics.
func updateStatusMetrics(oldStatus, newStatus contracts.ContractStatus, fn func(stat string, delta int64, timestamp time.Time) error) error {
	if oldStatus == newStatus {
		return nil
	}

	// pending contracts do not have a metric.
	if oldStatus != contracts.ContractStatusPending {
		if err := fn(contractStatusMetric(oldStatus), -1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric %q: %w", contractStatusMetric(oldStatus), err)
		}
	}

	if newStatus != contracts.ContractStatusPending {
		if err := fn(contractStatusMetric(newStatus), 1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric %q: %w", contractStatusMetric(newStatus), err)
		}
	}
	return nil
}

// v2ContractStatusMetric returns the metric name for a v2 contract status.
func v2ContractStatusMetric(status contracts.V2ContractStatus) string {
	switch status {
	case contracts.V2ContractStatusActive:
		return metricActiveContracts
	case contracts.V2ContractStatusRejected:
		return metricRejectedContracts
	case contracts.V2ContractStatusSuccessful:
		return metricSuccessfulContracts
	case contracts.V2ContractStatusFailed:
		return metricFailedContracts
	case contracts.V2ContractStatusRenewed:
		return metricRenewedContracts
	default:
		panic(fmt.Sprintf("unexpected contract status: %v", status))
	}
}

// updateV2StatusMetrics helper to update the v2 contract status metrics.
func updateV2StatusMetrics(oldStatus, newStatus contracts.V2ContractStatus, fn func(stat string, delta int64, timestamp time.Time) error) error {
	if oldStatus == newStatus {
		return nil
	}

	if oldStatus != contracts.V2ContractStatusPending {
		if err := fn(v2ContractStatusMetric(oldStatus), -1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric %q: %w", v2ContractStatusMetric(oldStatus), err)
		}
	}

	if newStatus != contracts.V2ContractStatusPending {
		if err := fn(v2ContractStatusMetric(newStatus), 1, time.Now()); err != nil {
			return fmt.Errorf("failed to update metric %q: %w", v2ContractStatusMetric(newStatus), err)
		}
	}
	return nil
}

// applyContractRevision updates the confirmed revision number of a contract.
func applyContractRevision(tx *txn, revisions []types.FileContractElement) error {
	if len(revisions) == 0 {
		return nil
	}

	stmt, err := tx.Prepare(`UPDATE contracts SET confirmed_revision_number=? WHERE contract_id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()

	for _, fce := range revisions {
		if res, err := stmt.Exec(encode(fce.FileContract.RevisionNumber), encode(fce.ID)); err != nil {
			return fmt.Errorf("failed to update contract revision %q: %w", fce.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", fce.ID)
		}
	}
	return nil
}

// applyContractFormation updates the contract table with the confirmation index and new status.
func applyContractFormation(tx *txn, confirmed []types.FileContractElement, log *zap.Logger) error {
	if len(confirmed) == 0 {
		return nil
	}

	getContractState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET formation_confirmed=true, contract_status=$1 WHERE id=$2`)
	if err != nil {
		return fmt.Errorf("failed to prepare confirmation statement: %w", err)
	}
	defer updateStmt.Close()

	for _, fce := range confirmed {
		state, err := getContractState(types.FileContractID(fce.ID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", fce.ID, err)
		}

		if state.Status != contracts.ContractStatusPending && state.Status != contracts.ContractStatusRejected {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", fce.ID), zap.Stringer("status", state.Status))
			continue
		}

		// update the contract table with the confirmation index and new status.
		res, err := updateStmt.Exec(contracts.ContractStatusActive, state.ID)
		if err != nil {
			return fmt.Errorf("failed to update state %q: %w", fce.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update contract %q: %w", fce.ID, err)
		} else if err := updateStatusMetrics(state.Status, contracts.ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to update contract metrics: %w", err)
		}

		if err := updatePotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to add potential revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to add collateral metrics: %w", err)
		}
	}
	return nil
}

// applySuccessfulContracts updates the contract table with the resolution index
// sets the contract status to successful, and updates the revenue metrics.
func applySuccessfulContracts(tx *txn, index types.ChainIndex, successful []types.FileContractID, log *zap.Logger) error {
	if len(successful) == 0 {
		return nil
	}

	getState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET resolution_height=?, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range successful {
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status == contracts.ContractStatusSuccessful {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", contractID))
			continue
		} else if state.Status != contracts.ContractStatusActive && state.Status != contracts.ContractStatusFailed {
			// panic if the contract is not active or failed. Proper reverts
			// should have ensured that this never happens.
			//
			// note: failed -> successful is allowed in case
			// of future logic changes.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", contractID, state.Status, contracts.ContractStatusSuccessful))
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(index.Height, contracts.ContractStatusSuccessful, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		}

		// update the contract status metrics
		if err := updateStatusMetrics(state.Status, contracts.ContractStatusSuccessful, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		// add the usage to the earned revenue metrics
		if err := updateEarnedRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update earned revenue metrics: %w", err)
		}

		// if the state is failed, the potential revenue metrics have already
		// been reduced. If the state is active, subtract the usage from the
		// potential revenue metrics.
		if state.Status == contracts.ContractStatusActive {
			if err := updatePotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
				return fmt.Errorf("failed to update potential revenue metrics: %w", err)
			} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
				return fmt.Errorf("failed to update collateral metrics: %w", err)
			}
		}
	}
	return nil
}

// applyFailedContracts sets the contract status to failed and subtracts the
// potential revenue metrics.
func applyFailedContracts(tx *txn, failed []types.FileContractID, log *zap.Logger) error {
	if len(failed) == 0 {
		return nil
	}

	getState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET resolution_height=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range failed {
		state, err := getState(types.FileContractID(contractID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status == contracts.ContractStatusFailed {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", contractID))
			continue
		} else if state.Status != contracts.ContractStatusActive && state.Status != contracts.ContractStatusSuccessful {
			// panic if the contract is not active or successful. Proper reverts
			// should have ensured that this never happens.
			//
			// note: going from successful -> failed is allowed in the case of
			// future logic changes.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", contractID, state.Status, contracts.ContractStatusFailed))
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(contracts.ContractStatusFailed, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		}

		// update the contract status metrics
		if err := updateStatusMetrics(state.Status, contracts.ContractStatusFailed, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		if state.Status == contracts.ContractStatusActive {
			// if the contract is active, subtract the usage from the potential
			// revenue metrics and the collateral metrics.
			if err := updatePotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
				return fmt.Errorf("failed to update potential revenue metrics: %w", err)
			} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
				return fmt.Errorf("failed to update collateral metrics: %w", err)
			}
		} else if state.Status == contracts.ContractStatusSuccessful {
			// if the contract is successful, subtract the usage from the earned
			// revenue metrics.
			if err := updateEarnedRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
				return fmt.Errorf("failed to update earned revenue metrics: %w", err)
			}
		}
	}
	return nil
}

// revertContractFormation reverts the contract formation by setting the
// confirmation index to null and the status to pending.
func revertContractFormation(tx *txn, reverted []types.FileContractElement) error {
	if len(reverted) == 0 {
		return nil
	}

	getState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET formation_confirmed=false, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, fce := range reverted {
		// get the current state of the contract
		state, err := getState(types.FileContractID(fce.ID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", fce.ID, err)
		}

		if state.Status != contracts.ContractStatusActive {
			// if the contract is not active, panic. Applies should have ensured
			// that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", fce.ID, state.Status, contracts.ContractStatusPending))
		}

		// set the contract status to pending
		if res, err := updateStmt.Exec(contracts.ContractStatusPending, state.ID); err != nil {
			return fmt.Errorf("failed to revert contract formation %q: %w", fce.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated %q", fce.ID)
		}

		// subtract the metrics
		if err := updateStatusMetrics(state.Status, contracts.ContractStatusPending, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to update contract metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		} else if err := updatePotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		}
	}
	return err
}

// revertSuccessfulContracts reverts the contract resolution by setting the
// resolution index to null, the status to active, and updating the revenue
// metrics.
func revertSuccessfulContracts(tx *txn, successful []types.FileContractID) error {
	if len(successful) == 0 {
		return nil
	}

	getState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET resolution_height=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range successful {
		// get the current state of the contract
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status != contracts.ContractStatusSuccessful {
			// if the contract is not successful, panic. Applies should have
			// ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", contractID, state.Status, contracts.ContractStatusActive))
		}

		if res, err := updateStmt.Exec(encode(contractID)); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", contractID)
		}

		// update the contract status metrics
		if err := updateStatusMetrics(state.Status, contracts.ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		// subtract the usage from the earned revenue metrics and add it to the
		// potential revenue metrics
		if err := updatePotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateEarnedRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update earned revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return err
}

// revertFailedContracts sets the contract status to active and adds the
// potential revenue and collateral metrics.
func revertFailedContracts(tx *txn, failed []types.FileContractID) error {
	if len(failed) == 0 {
		return nil
	}

	getState, done, err := getContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts SET resolution_height=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range failed {
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status != contracts.ContractStatusFailed {
			// panic if the contract is not failed. Proper reverts should have
			// ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", contractID, state.Status, contracts.ContractStatusFailed))
		} else if state.Status == contracts.ContractStatusFailed {
			// skip update, most likely rescanning
			continue
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(contracts.ContractStatusActive, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		}

		// update the contract status metrics
		if err := updateStatusMetrics(state.Status, contracts.ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		// add the usage back to the potential revenue metrics
		if err := updatePotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return nil
}

// applyV2ContractFormation updates the contract table with the confirmation index
// and new status.
func applyV2ContractFormation(tx *txn, index types.ChainIndex, confirmed []types.V2FileContractElement, log *zap.Logger) error {
	if len(confirmed) == 0 {
		return nil
	}

	getV2ContractState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare get state statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment numeric statement: %w", err)
	}
	defer done()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment currency statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET confirmation_index=$1, contract_status=$2 WHERE id=$3`)
	if err != nil {
		return fmt.Errorf("failed to prepare update status statement: %w", err)
	}
	defer updateStmt.Close()

	insertElementStmt, err := tx.Prepare(`INSERT INTO contract_v2_state_elements (contract_id, leaf_index, merkle_proof, raw_contract, revision_number) VALUES (?, ?, ?, ?, ?) ON CONFLICT (contract_id) DO UPDATE SET leaf_index=EXCLUDED.leaf_index, merkle_proof=EXCLUDED.merkle_proof, raw_contract=EXCLUDED.raw_contract, revision_number=EXCLUDED.revision_number`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert state element statement: %w", err)
	}
	defer insertElementStmt.Close()

	for _, fce := range confirmed {
		state, err := getV2ContractState(types.FileContractID(fce.ID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", fce.ID, err)
		}

		if _, err := insertElementStmt.Exec(state.ID, fce.StateElement.LeafIndex, encode(fce.StateElement.MerkleProof), encode(fce.V2FileContract), encode(fce.V2FileContract.RevisionNumber)); err != nil {
			return fmt.Errorf("failed to insert contract state element %q: %w", fce.ID, err)
		}

		if state.Status != contracts.V2ContractStatusPending && state.Status != contracts.V2ContractStatusRejected {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", fce.ID), zap.String("current", string(state.Status)))
			continue
		}

		// update the contract table with the confirmation index and new status.
		res, err := updateStmt.Exec(encode(index), contracts.V2ContractStatusActive, state.ID)
		if err != nil {
			return fmt.Errorf("failed to update state %q: %w", fce.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update contract %q: %w", fce.ID, err)
		}

		if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		} else if err := updateV2PotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to update contract metrics: %w", err)
		}
	}
	return nil
}

// revertV2ContractFormation reverts the contract formation by setting the
// confirmation index to null and the status to pending.
func revertV2ContractFormation(tx *txn, reverted []types.V2FileContractElement) error {
	if len(reverted) == 0 {
		return nil
	}

	getV2ContractState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET confirmation_index=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	deleteStmt, err := tx.Prepare(`DELETE FROM contract_v2_state_elements WHERE contract_id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer deleteStmt.Close()

	for _, fce := range reverted {
		// get the current contract state
		state, err := getV2ContractState(types.FileContractID(fce.ID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", fce.ID, err)
		}

		// delete the state element
		if _, err := deleteStmt.Exec(state.ID); err != nil {
			return fmt.Errorf("failed to delete contract state element %q: %w", fce.ID, err)
		}

		if state.Status != contracts.V2ContractStatusActive {
			// if the contract is not active, panic. Applies should have ensured
			// that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q -> %q", state.Status, contracts.ContractStatusPending))
		}

		// set the contract status to pending
		if res, err := updateStmt.Exec(contracts.V2ContractStatusPending, state.ID); err != nil {
			return fmt.Errorf("failed to revert contract formation %q: %w", fce.ID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated %q", fce.ID)
		}

		// subtract the metrics
		if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusPending, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to update contract metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		} else if err := updateV2PotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		}
	}
	return nil
}

// applyV2ContractRevision updates the confirmed revision number of a contract.
func applyV2ContractRevision(tx *txn, revised []types.V2FileContractElement) error {
	selectIDStmt, err := tx.Prepare(`SELECT id FROM contracts_v2 WHERE contract_id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer selectIDStmt.Close()

	updateElementStmt, err := tx.Prepare(`UPDATE contract_v2_state_elements SET leaf_index=?, merkle_proof=?, raw_contract=?, revision_number=? WHERE contract_id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update element statement: %w", err)
	}
	defer updateElementStmt.Close()

	for _, fce := range revised {
		var contractID int64
		if err := selectIDStmt.QueryRow(encode(fce.ID)).Scan(&contractID); err != nil {
			return fmt.Errorf("failed to update contract revision %q: %w", fce.ID, err)
		} else if _, err := updateElementStmt.Exec(fce.StateElement.LeafIndex, encode(fce.StateElement.MerkleProof), encode(fce.V2FileContract), encode(fce.V2FileContract.RevisionNumber), contractID); err != nil {
			return fmt.Errorf("failed to update contract state element %q: %w", fce.ID, err)
		}
	}
	return nil
}

// applySuccessfulV2Contracts updates the contract table with the resolution index
// sets the contract status to successful, and updates the revenue metrics.
func applySuccessfulV2Contracts(tx *txn, index types.ChainIndex, status contracts.V2ContractStatus, successful []types.FileContractID, log *zap.Logger) error {
	if len(successful) == 0 {
		return nil
	}

	getState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET resolution_index=?, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range successful {
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status == status {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", contractID), zap.String("current", string(state.Status)))
			continue
		} else if state.Status != contracts.V2ContractStatusActive {
			// panic if the contract is not active. Proper reverts should have
			// ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q %q -> %q", contractID, state.Status, contracts.V2ContractStatusSuccessful))
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(encode(index), status, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", contractID)
		}

		if err := updateV2StatusMetrics(state.Status, status, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		} else if err := updateV2EarnedRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update earned revenue metrics: %w", err)
		} else if err := updateV2PotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return nil
}

// applyFailedV2Contracts sets the contract status to active and adds the
// potential revenue metrics.
func applyFailedV2Contracts(tx *txn, index types.ChainIndex, failed []types.FileContractID, log *zap.Logger) error {
	if len(failed) == 0 {
		return nil
	}

	getState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET resolution_index=?, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range failed {
		state, err := getState(types.FileContractID(contractID))
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		// skip update if the contract is already failed.
		// This should only happen during a rescan
		if state.Status == contracts.V2ContractStatusFailed {
			log.Debug("skipping rescan state transition", zap.Stringer("contractID", contractID))
			continue
		} else if state.Status != contracts.V2ContractStatusActive {
			// panic if the contract is not active. Proper reverts should have
			// ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q -> %q", state.Status, contracts.V2ContractStatusFailed))
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(encode(index), contracts.V2ContractStatusFailed, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", contractID)
		}

		if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusFailed, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		} else if err := updateV2PotentialRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return nil
}

// revertSuccessfulV2Contracts clears the resolution index, sets the status to
// active and updates the revenue metrics.
func revertSuccessfulV2Contracts(tx *txn, status contracts.V2ContractStatus, successful []types.FileContractID) error {
	if len(successful) == 0 {
		return nil
	}

	getState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET resolution_index=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range successful {
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status != status {
			// panic if the contract is not active. Proper reverts should have
			//  ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q -> %q", state.Status, contracts.V2ContractStatusActive))
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(status, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", contractID)
		}

		// update the contract status metrics
		if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		// add the usage to the potential revenue metrics and subtract it from the
		// earned revenue metrics
		if err := updateV2PotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateV2EarnedRevenueMetrics(state.Usage, true, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update earned revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return nil
}

// revertFailedV2Contracts sets the contract status to active and adds the
// potential revenue and collateral metrics.
func revertFailedV2Contracts(tx *txn, failed []types.FileContractID) error {
	if len(failed) == 0 {
		return nil
	}

	getState, done, err := getV2ContractStateStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare select statement: %w", err)
	}
	defer done()

	updateStmt, err := tx.Prepare(`UPDATE contracts_v2 SET resolution_index=NULL, contract_status=? WHERE id=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer updateStmt.Close()

	incrementCurrencyStat, done, err := incrementCurrencyStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	incrementNumericStat, done, err := incrementNumericStatStmt(tx)
	if err != nil {
		return fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer done()

	for _, contractID := range failed {
		state, err := getState(contractID)
		if err != nil {
			return fmt.Errorf("failed to get contract state %q: %w", contractID, err)
		}

		if state.Status != contracts.V2ContractStatusFailed {
			// panic if the contract is not failed. Proper reverts should have
			//  ensured that this never happens.
			panic(fmt.Errorf("unexpected contract state transition %q -> %q", state.Status, contracts.V2ContractStatusFailed))
		} else if state.Status == contracts.V2ContractStatusFailed {
			// skip update, most likely rescanning
			continue
		}

		// update the contract's resolution index and status
		if res, err := updateStmt.Exec(contracts.V2ContractStatusActive, state.ID); err != nil {
			return fmt.Errorf("failed to update contract %q: %w", contractID, err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("no rows updated: %q", contractID)
		}

		// update the contract status metrics
		if err := updateV2StatusMetrics(state.Status, contracts.V2ContractStatusActive, incrementNumericStat); err != nil {
			return fmt.Errorf("failed to set contract %q status: %w", contractID, err)
		}

		// add the usage back to the potential revenue metrics
		if err := updateV2PotentialRevenueMetrics(state.Usage, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update potential revenue metrics: %w", err)
		} else if err := updateCollateralMetrics(state.LockedCollateral, state.Usage.RiskedCollateral, false, incrementCurrencyStat); err != nil {
			return fmt.Errorf("failed to update collateral metrics: %w", err)
		}
	}
	return nil
}

// rejectContracts returns the ID of any contracts that are not confirmed and have
// a negotiation height less than the given height.
func rejectContracts(tx *txn, height uint64) (rejected []types.FileContractID, err error) {
	rows, err := tx.Query(`SELECT contract_id FROM contracts WHERE contract_status <> $1 AND formation_confirmed=false AND negotiation_height < $2`, contracts.ContractStatusRejected, height)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id types.FileContractID
		if err := rows.Scan(decode(&id)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		rejected = append(rejected, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan contracts: %w", err)
	}
	return
}

// rejectV2Contracts returns the ID of any v2 contracts that are not confirmed and have
// a negotiation height less than the given height.
func rejectV2Contracts(tx *txn, height uint64) (rejected []types.FileContractID, err error) {
	rows, err := tx.Query(`SELECT contract_id FROM contracts_v2 WHERE contract_status <> $1 AND confirmation_index IS NULL AND negotiation_height < $2`, contracts.V2ContractStatusRejected, height)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id types.FileContractID
		if err := rows.Scan(decode(&id)); err != nil {
			return nil, fmt.Errorf("failed to scan contract: %w", err)
		}
		rejected = append(rejected, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan contracts: %w", err)
	}
	return
}
