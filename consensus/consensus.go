package consensus

import (
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// A Store stores the current state of the chain manager
	Store interface {
		Close() error
		GetLastChange() (modules.ConsensusChangeID, error)
		SetLastChange(modules.ConsensusChangeID) error
	}

	// A ChainIndex groups a block's ID and height.
	ChainIndex struct {
		ID     types.BlockID
		Height uint64
	}

	// State represents the full state of the chain as of a particular block.
	State struct {
		Index ChainIndex
	}

	// A ChainManager manages the current state of the blockchain.
	ChainManager struct {
		cs    modules.ConsensusSet
		store Store

		close chan struct{}
		mu    sync.Mutex
		tip   State
	}
)

var (
	// ErrBlockNotFound is returned when a block is not found.
	ErrBlockNotFound = errors.New("block not found")
)

// ProcessConsensusChange implements the modules.ConsensusSetSubscriber interface.
func (cm *ChainManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.tip = State{
		Index: ChainIndex{
			ID:     types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()),
			Height: uint64(cc.BlockHeight),
		},
	}
	if err := cm.store.SetLastChange(cc.ID); err != nil {
		panic(err)
	}
}

// Close closes the chain manager.
func (cm *ChainManager) Close() error {
	select {
	case <-cm.close:
		return nil
	default:
	}
	close(cm.close)
	return cm.store.Close()
}

// Synced returns true if the chain manager is synced with the consensus set.
func (cm *ChainManager) Synced() bool {
	return cm.cs.Synced()
}

// IndexAtHeight return the chain index at the given height.
func (cm *ChainManager) IndexAtHeight(height uint64) (ChainIndex, error) {
	block, ok := cm.cs.BlockAtHeight(types.BlockHeight(height))
	if !ok {
		return ChainIndex{}, ErrBlockNotFound
	}
	return ChainIndex{
		ID:     types.BlockID(block.ID()),
		Height: height,
	}, nil
}

// Tip returns the current tip of the blockchain.
func (cm *ChainManager) Tip() State {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.tip
}

// NewChainManager creates a new chain manager.
func NewChainManager(cs modules.ConsensusSet, store Store) (*ChainManager, error) {
	cm := &ChainManager{
		cs:    cs,
		store: store,

		close: make(chan struct{}),
	}
	cc, err := store.GetLastChange()
	if err != nil {
		return nil, fmt.Errorf("failed to get last change: %w", err)
	} else if err := cs.ConsensusSetSubscribe(cm, cc, cm.close); err != nil {
		return nil, fmt.Errorf("failed to subscribe to consensus set: %w", err)
	}
	return cm, nil
}
