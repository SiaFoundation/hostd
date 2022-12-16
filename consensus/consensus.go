package consensus

import (
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	// A Store stores
	Store interface {
		Close() error
		GetLastChange() (modules.ConsensusChangeID, error)
		SetLastChange(modules.ConsensusChangeID) error
	}

	ChainIndex struct {
		ID     types.BlockID
		Height uint64
	}

	State struct {
		Index ChainIndex
	}

	ChainManager struct {
		cs    modules.ConsensusSet
		store Store

		close chan struct{}
		mu    sync.Mutex
		tip   State
	}
)

var (
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

func (cm *ChainManager) Close() error {
	select {
	case <-cm.close:
		return nil
	default:
	}
	close(cm.close)
	return cm.store.Close()
}

func (cm *ChainManager) Synced() bool {
	return cm.cs.Synced()
}

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
