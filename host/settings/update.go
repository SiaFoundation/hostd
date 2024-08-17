package settings

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
)

// An UpdateStateTx is a transaction that can update the host's announcement
// state.
type UpdateStateTx interface {
	LastAnnouncement() (Announcement, error)
	RevertLastAnnouncement() error
	SetLastAnnouncement(Announcement) error
}

// UpdateChainState updates the host's announcement state based on the given
// chain updates.
func (cm *ConfigManager) UpdateChainState(tx UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	pk := cm.hostKey.PublicKey()
	lastAnnouncement, err := tx.LastAnnouncement()
	if err != nil {
		return fmt.Errorf("failed to get last announcement: %w", err)
	}

	for _, cru := range reverted {
		if cru.State.Index == lastAnnouncement.Index {
			if err := tx.RevertLastAnnouncement(); err != nil {
				return fmt.Errorf("failed to revert last announcement: %w", err)
			}
		}
	}

	var nextAnnouncement *Announcement
	for _, cau := range applied {
		index := cau.State.Index

		chain.ForEachHostAnnouncement(cau.Block, func(hostKey types.PublicKey, announcement chain.HostAnnouncement) {
			if hostKey != pk {
				return
			}

			nextAnnouncement = &Announcement{
				Address: announcement.NetAddress,
				Index:   index,
			}
		})
	}

	if nextAnnouncement == nil {
		return nil
	}

	if err := tx.SetLastAnnouncement(*nextAnnouncement); err != nil {
		return fmt.Errorf("failed to set last announcement: %w", err)
	}
	cm.log.Debug("announcement confirmed", zap.String("netaddress", nextAnnouncement.Address), zap.Stringer("index", nextAnnouncement.Index))
	return nil
}

// ProcessActions processes announcement actions based on the given chain index.
func (m *ConfigManager) ProcessActions(index types.ChainIndex) error {
	announcement, err := m.store.LastAnnouncement()
	if err != nil {
		return fmt.Errorf("failed to get last announcement: %w", err)
	}

	nextHeight := announcement.Index.Height + m.announceInterval
	netaddress := m.Settings().NetAddress
	if err := validateNetAddress(netaddress); err != nil {
		if m.validateNetAddress {
			return nil
		}
	}

	// check if a new announcement is needed
	n := m.chain.TipState().Network
	// re-announce if the v2 hardfork has activated and the last announcement was before activation
	reannounceV2 := index.Height >= n.HardforkV2.AllowHeight && announcement.Index.Height < n.HardforkV2.AllowHeight
	if !reannounceV2 && index.Height < nextHeight && announcement.Address == netaddress {
		return nil
	}

	// re-announce
	if err := m.Announce(); err != nil {
		m.log.Warn("failed to announce", zap.Error(err))
	}
	return nil
}
