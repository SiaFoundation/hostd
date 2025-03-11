package index

import (
	"context"
	"fmt"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.uber.org/zap"
)

// An UpdateTx is a transaction that atomically updates the state of the
// index manager.
type UpdateTx interface {
	wallet.UpdateTx
	contracts.UpdateStateTx
	settings.UpdateStateTx

	SetLastIndex(types.ChainIndex) error
}

func (m *Manager) syncDB(ctx context.Context) error {
	log := m.log.Named("sync")
	index := m.Tip()
	for index != m.chain.Tip() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		reverted, applied, err := m.chain.UpdatesSince(index, m.updateBatchSize)
		if err != nil && strings.Contains(err.Error(), "missing block at index") {
			log.Warn("missing block at index, resetting chain state")
			// reset the consensus state. Should delete all chain related
			// state from the store
			if err := m.store.ResetChainState(); err != nil {
				return fmt.Errorf("failed to reset consensus state: %w", err)
			}
			// zero out the index to force a full resync
			m.mu.Lock()
			m.index = types.ChainIndex{}
			m.mu.Unlock()
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to get updates since %v: %w", index, err)
		} else if len(reverted) == 0 && len(applied) == 0 {
			return nil
		}

		err = m.store.UpdateChainState(func(tx UpdateTx) error {
			if err := m.wallet.UpdateChainState(tx, reverted, applied); err != nil {
				return fmt.Errorf("failed to update wallet state: %w", err)
			} else if err := m.contracts.UpdateChainState(tx, reverted, applied); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			} else if err := m.settings.UpdateChainState(tx, reverted, applied); err != nil {
				return fmt.Errorf("failed to update settings state: %w", err)
			}

			if len(applied) > 0 {
				index = applied[len(applied)-1].State.Index
			} else {
				index = reverted[len(reverted)-1].State.Index
			}

			if err := tx.SetLastIndex(index); err != nil {
				return fmt.Errorf("failed to set last index: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to update chain state: %w", err)
		}

		if err := m.contracts.ProcessActions(index); err != nil {
			return fmt.Errorf("failed to process contract actions: %w", err)
		} else if err := m.volumes.ProcessActions(index); err != nil {
			return fmt.Errorf("failed to process storage actions: %w", err)
		} else if err := m.settings.ProcessActions(index); err != nil {
			return fmt.Errorf("failed to process settings actions: %w", err)
		}

		m.mu.Lock()
		m.index = index
		m.mu.Unlock()
		log.Debug("synced to new chain index", zap.Stringer("index", index))
	}
	return nil
}
