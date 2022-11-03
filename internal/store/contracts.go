package store

import (
	"errors"
	"sync"

	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// An EphemeralContractStore implements an in-memory contract store.
type EphemeralContractStore struct {
	mu        sync.Mutex
	contracts map[types.FileContractID]*contracts.Contract
	roots     map[types.FileContractID][]crypto.Hash
}

// Close implements ContractStore.Close
func (cs *EphemeralContractStore) Close() error {
	return nil
}

// ActiveContracts returns a list of contracts that are unresolved.
func (cs *EphemeralContractStore) ActiveContracts(limit, skip uint64) (c []contracts.Contract, _ error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, contract := range cs.contracts {
		if contract.Confirmed && contract.State == contracts.ContractStateUnresolved {
			c = append(c, *contract)
		}
	}
	return
}

// ResolvedContracts returns all contracts that have been resolved.
func (cs *EphemeralContractStore) ResolvedContracts(limit, skip uint64) (c []contracts.Contract, _ error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, contract := range cs.contracts {
		if contract.State != contracts.ContractStateUnresolved {
			c = append(c, *contract)
		}
	}
	return
}

// Exists checks if the contract exists in the store.
func (cs *EphemeralContractStore) Exists(id types.FileContractID) (exists bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, exists = cs.contracts[id]
	return
}

// Get returns the contract with the given ID.
func (cs *EphemeralContractStore) Get(id types.FileContractID) (contracts.Contract, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	c, ok := cs.contracts[id]
	if !ok {
		return contracts.Contract{}, errors.New("contract not found")
	}
	return *c, nil
}

// Add stores the provided contract, returning an error if the contract already
// exists.
func (cs *EphemeralContractStore) Add(revision contracts.SignedRevision, formationTxnSet []types.Transaction) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, exists := cs.contracts[revision.Revision.ParentID]; exists {
		return errors.New("contract already exists")
	}
	cs.contracts[revision.Revision.ParentID] = &contracts.Contract{
		SignedRevision:       revision,
		FormationTransaction: formationTxnSet,
	}
	return nil
}

// Delete removes the contract with the given ID.
func (cs *EphemeralContractStore) Delete(id types.FileContractID) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.contracts, id)
	return nil
}

// Revise updates the current revision associated with a contract.
func (cs *EphemeralContractStore) Revise(revision types.FileContractRevision, renterSig, hostSig []byte) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, exists := cs.contracts[revision.ParentID]; !exists {
		return errors.New("contract not found")
	}
	cs.contracts[revision.ParentID].Revision = revision
	return nil
}

// Roots returns the roots of all sectors stored by the contract.
func (cs *EphemeralContractStore) Roots(id types.FileContractID) ([]crypto.Hash, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.roots[id], nil
}

// SetRoots updates the roots of the contract.
func (cs *EphemeralContractStore) SetRoots(id types.FileContractID, roots []crypto.Hash) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.roots[id] = append([]crypto.Hash(nil), roots...)
	return nil
}

// ContractAction determines which contracts in the store need to have a lifecycle action taken. Implements ContractStore.ContractAction
func (cs *EphemeralContractStore) ContractAction(cc *modules.ConsensusChange, contractFn func(types.FileContractID)) error {
	cs.mu.Lock()
	var actionableContracts []types.FileContractID
	for _, c := range cs.contracts {
		if !c.ShouldBroadcastTransaction(uint64(cc.BlockHeight)) || c.ShouldBroadcastRevision(uint64(cc.BlockHeight)) || c.ShouldBroadcastStorageProof(uint64(cc.BlockHeight)) {
			actionableContracts = append(actionableContracts, c.Revision.ParentID)
		}
	}
	// global mutex must be unlocked before calling contractFn
	cs.mu.Unlock()

	for _, c := range actionableContracts {
		contractFn(c)
	}
	return nil
}

// NewEphemeralContractStore initializes a new EphemeralContractStore.
func NewEphemeralContractStore() *EphemeralContractStore {
	return &EphemeralContractStore{
		contracts: make(map[types.FileContractID]*contracts.Contract),
		roots:     make(map[types.FileContractID][]crypto.Hash),
	}
}
