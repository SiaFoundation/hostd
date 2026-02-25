package index

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
)

type (
	// A Store is a persistent store for the index manager.
	Store interface {
		// ResetChainState resets the consensus state of the store. This
		// should only occur if the user has reset their consensus database to
		// sync from scratch.
		ResetChainState() error
		UpdateChainState(func(UpdateTx) error) error
		Tip() (types.ChainIndex, error)
	}

	// A ContractManager manages the lifecycle and state of storage contracts
	ContractManager interface {
		// UpdateChainState atomically updates the blockchain state of the
		// contract manager.
		UpdateChainState(tx contracts.UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
		// ProcessActions is called after the chain state is updated to
		// trigger additional actions related to contract lifecycle management.
		// This operation should not assume that it will be called for every
		// block. During syncing, it will be called at most once per batch.
		ProcessActions(index types.ChainIndex) error
	}

	// A SettingsManager manages the host's settings and announcements
	SettingsManager interface {
		// UpdateChainState atomically updates the blockchain state of the
		// settings manager.
		UpdateChainState(tx settings.UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
		// ProcessActions is called after the chain state is updated to
		// trigger additional actions related to settings management.
		// This operation should not assume that it will be called for every
		// block. During syncing, it will be called at most once per batch.
		ProcessActions(index types.ChainIndex) error
	}

	// A WalletManager manages the host's UTXOs and balance
	WalletManager interface {
		// UpdateChainState atomically updates the blockchain state of the
		// wallet manager.
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
	}

	// A VolumeManager manages the host's storage volumes
	VolumeManager interface {
		// ProcessActions is called to trigger additional actions related to
		// volume management. This operation should not assume that it will be
		// called for every block. During syncing, it will be called at most
		// once per batch
		ProcessActions(index types.ChainIndex) error
	}

	// A Manager manages the state of the blockchain and indexes consensus
	// changes.
	Manager struct {
		pruneTarget     uint64
		updateBatchSize int

		chain *chain.Manager
		store Store

		contracts ContractManager
		settings  SettingsManager
		wallet    WalletManager
		volumes   VolumeManager

		tg  *threadgroup.ThreadGroup
		log *zap.Logger

		mu    sync.Mutex // protects the fields below
		index types.ChainIndex
	}
)

// Close stops the manager.
func (m *Manager) Close() {
	m.tg.Stop()
}

// Tip returns the current chain index.
func (m *Manager) Tip() types.ChainIndex {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.index
}

// NewManager creates a new Manager.
func NewManager(store Store, chain *chain.Manager, contracts ContractManager, wallet WalletManager, settings SettingsManager, volumes VolumeManager, opts ...Option) (*Manager, error) {
	index, err := store.Tip()
	if err != nil {
		return nil, fmt.Errorf("failed to get last indexed tip: %w", err)
	}

	m := &Manager{
		pruneTarget:     0, // disabled
		updateBatchSize: 100,

		chain: chain,
		store: store,

		contracts: contracts,
		wallet:    wallet,
		settings:  settings,
		volumes:   volumes,

		index: index,

		tg:  threadgroup.New(),
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(m)
	}

	reorgCh := make(chan struct{}, 1)
	reorgCh <- struct{}{} // trigger initial sync
	stop := m.chain.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})

	go func() {
		defer stop()

		ctx, cancel, err := m.tg.AddContext(context.Background())
		if err != nil {
			m.log.Error("failed to add context", zap.Error(err))
			return
		}
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-reorgCh:
				if err := m.syncDB(ctx); err != nil && !errors.Is(err, context.Canceled) {
					m.log.Error("failed to sync database", zap.Error(err))
				}
			}
		}
	}()
	return m, nil
}
