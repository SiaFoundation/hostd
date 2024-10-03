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
	// RevertLastAnnouncement reverts the last announcement.
	RevertLastAnnouncement() error
	// SetLastAnnouncement sets the last announcement.
	SetLastAnnouncement(Announcement) error

	// LastV2AnnouncementHash returns the last v2 announcement.
	LastV2AnnouncementHash() (types.Hash256, types.ChainIndex, error)
	// RevertLastV2Announcement reverts the last v2 announcement.
	RevertLastV2Announcement() error
	// SetLastV2Announcement sets the last v2 announcement.
	SetLastV2AnnouncementHash(types.Hash256, types.ChainIndex) error
}

// UpdateChainState updates the host's announcement state based on the given
// chain updates.
func (cm *ConfigManager) UpdateChainState(tx UpdateStateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	pk := cm.hostKey.PublicKey()
	lastAnnouncement, err := tx.LastAnnouncement()
	if err != nil {
		return fmt.Errorf("failed to get last announcement: %w", err)
	}

	_, v2AnnouncementIndex, err := tx.LastV2AnnouncementHash()
	if err != nil {
		return fmt.Errorf("failed to get last v2 announcement: %w", err)
	}

	for _, cru := range reverted {
		if cru.State.Index == lastAnnouncement.Index {
			if err := tx.RevertLastAnnouncement(); err != nil {
				return fmt.Errorf("failed to revert last announcement: %w", err)
			}
		}

		if cru.State.Index == v2AnnouncementIndex {
			if err := tx.RevertLastV2Announcement(); err != nil {
				return fmt.Errorf("failed to revert last v2 announcement: %w", err)
			}
		}
	}

	var nextAnnouncement *Announcement
	var v2AnnouncementHash types.Hash256
	var v2AnnounceIndex types.ChainIndex
	for _, cau := range applied {
		index := cau.State.Index

		chain.ForEachHostAnnouncement(cau.Block, func(a chain.HostAnnouncement) {
			if a.PublicKey != pk {
				return
			}

			nextAnnouncement = &Announcement{
				Address: a.NetAddress,
				Index:   index,
			}
		})

		h := types.NewHasher()
		chain.ForEachV2HostAnnouncement(cau.Block, func(hostKey types.PublicKey, addresses []chain.NetAddress) {
			if hostKey != pk {
				return
			}

			h.Reset()
			types.EncodeSlice(h.E, addresses)
			if err := h.E.Flush(); err != nil {
				cm.log.Error("failed to hash v2 announcement", zap.Error(err))
				return
			}
			v2AnnouncementHash = h.Sum()
			v2AnnounceIndex = index
		})
	}

	if nextAnnouncement != nil {
		if err := tx.SetLastAnnouncement(*nextAnnouncement); err != nil {
			return fmt.Errorf("failed to set last announcement: %w", err)
		}
		cm.log.Debug("announcement confirmed", zap.String("netaddress", nextAnnouncement.Address), zap.Stringer("index", nextAnnouncement.Index))
	}

	if v2AnnouncementHash != (types.Hash256{}) {
		if err := tx.SetLastV2AnnouncementHash(v2AnnouncementHash, v2AnnounceIndex); err != nil {
			return fmt.Errorf("failed to set last v2 announcement: %w", err)
		}
		cm.log.Debug("v2 announcement confirmed", zap.Stringer("hash", v2AnnouncementHash), zap.Stringer("index", v2AnnounceIndex))
	}

	return nil
}

// ProcessActions processes announcement actions based on the given chain index.
func (cm *ConfigManager) ProcessActions(index types.ChainIndex) error {
	n := cm.chain.TipState().Network

	var shouldAnnounce bool
	if index.Height < n.HardforkV2.AllowHeight {
		announcement, err := cm.store.LastAnnouncement()
		if err != nil {
			return fmt.Errorf("failed to get last announcement: %w", err)
		}

		nextHeight := announcement.Index.Height + cm.announceInterval
		netaddress := cm.Settings().NetAddress
		if err := validateNetAddress(netaddress); err != nil && cm.validateNetAddress {
			cm.log.Warn("invalid net address", zap.String("address", netaddress), zap.Error(err))
			return nil
		}
		shouldAnnounce = index.Height >= nextHeight || announcement.Address != netaddress
	} else {
		ah, announceIndex, err := cm.store.LastV2AnnouncementHash()
		if err != nil {
			return fmt.Errorf("failed to get last v2 announcement: %w", err)
		}

		nextHeight := announceIndex.Height + cm.announceInterval
		h := types.NewHasher()
		types.EncodeSlice(h.E, cm.Settings().V2AnnounceAddresses)
		if err := h.E.Flush(); err != nil {
			return fmt.Errorf("failed to hash v2 announcement: %w", err)
		}
		shouldAnnounce = index.Height >= nextHeight || ah != h.Sum()
	}

	if !shouldAnnounce {
		return nil
	} else if err := cm.Announce(); err != nil {
		cm.log.Warn("failed to announce", zap.Error(err))
	}
	return nil
}
