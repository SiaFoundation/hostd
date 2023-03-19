package chain

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
)

var (
	// ErrBlockNotFound is returned when a block is not found.
	ErrBlockNotFound = errors.New("block not found")
)

func convertToSiad(core types.EncoderTo, siad encoding.SiaUnmarshaler) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	core.EncodeTo(e)
	e.Flush()
	if err := siad.UnmarshalSia(&buf); err != nil {
		panic(err)
	}
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// A Manager manages the current state of the blockchain.
type Manager struct {
	cs modules.ConsensusSet

	close chan struct{}
	mu    sync.Mutex
	tip   consensus.State
}

// ProcessConsensusChange implements the modules.ConsensusSetSubscriber interface.
func (m *Manager) ProcessConsensusChange(cc modules.ConsensusChange) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tip = consensus.State{
		Index: types.ChainIndex{
			ID:     types.BlockID(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].ID()),
			Height: uint64(cc.BlockHeight),
		},
	}
}

// Close closes the chain manager.
func (m *Manager) Close() error {
	select {
	case <-m.close:
		return nil
	default:
	}
	close(m.close)
	return m.cs.Close()
}

// Synced returns true if the chain manager is synced with the consensus set.
func (m *Manager) Synced() bool {
	return m.cs.Synced()
}

// BlockAtHeight returns the block at the given height.
func (m *Manager) BlockAtHeight(height uint64) (types.Block, bool) {
	sb, ok := m.cs.BlockAtHeight(stypes.BlockHeight(height))
	var c types.Block
	convertToCore(sb, &c)
	return c, ok
}

// IndexAtHeight return the chain index at the given height.
func (m *Manager) IndexAtHeight(height uint64) (types.ChainIndex, error) {
	block, ok := m.cs.BlockAtHeight(stypes.BlockHeight(height))
	if !ok {
		return types.ChainIndex{}, ErrBlockNotFound
	}
	return types.ChainIndex{
		ID:     types.BlockID(block.ID()),
		Height: height,
	}, nil
}

// TipState returns the current chain state.
func (m *Manager) TipState() consensus.State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tip
}

// AcceptBlock adds b to the consensus set.
func (m *Manager) AcceptBlock(b types.Block) error {
	var sb stypes.Block
	convertToSiad(b, &sb)
	return m.cs.AcceptBlock(sb)
}

// Subscribe subscribes to the consensus set.
func (m *Manager) Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error {
	return m.cs.ConsensusSetSubscribe(s, ccID, cancel)
}

// NewManager creates a new chain manager.
func NewManager(cs modules.ConsensusSet) (*Manager, error) {
	height := cs.Height()
	block, ok := cs.BlockAtHeight(height)
	if !ok {
		return nil, fmt.Errorf("failed to get block at height %d", height)
	}

	m := &Manager{
		cs: cs,
		tip: consensus.State{
			Index: types.ChainIndex{
				ID:     types.BlockID(block.ID()),
				Height: uint64(height),
			},
		},

		close: make(chan struct{}),
	}

	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeRecent, m.close); err != nil {
		return nil, fmt.Errorf("failed to subscribe to consensus set: %w", err)
	}
	return m, nil
}
