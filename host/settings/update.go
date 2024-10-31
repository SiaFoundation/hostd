package settings

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
)

// An UpdateStateTx is a transaction that can update the host's announcement
// state.
type UpdateStateTx interface {
	LastAnnouncement() (Announcement, error)
	RevertLastAnnouncement() error
	SetLastAnnouncement(Announcement) error

	// LastV2AnnouncementHash returns the hash of the last v2 announcement.
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

	var announcement Announcement
	var v2AnnounceAddresses []chain.NetAddress
	var v2AnnounceIndex types.ChainIndex
	for _, cau := range applied {
		index := cau.State.Index

		chain.ForEachHostAnnouncement(cau.Block, func(a chain.HostAnnouncement) {
			if a.PublicKey != pk {
				return
			}

			announcement = Announcement{
				Address: a.NetAddress,
				Index:   index,
			}
		})

		chain.ForEachV2HostAnnouncement(cau.Block, func(hostKey types.PublicKey, addresses []chain.NetAddress) {
			if hostKey != pk {
				return
			}

			v2AnnounceAddresses = addresses
			v2AnnounceIndex = index
		})
	}

	if announcement.Index != (types.ChainIndex{}) {
		if err := tx.SetLastAnnouncement(announcement); err != nil {
			return fmt.Errorf("failed to set last announcement: %w", err)
		}
		cm.log.Debug("announcement confirmed", zap.String("netaddress", announcement.Address), zap.Stringer("index", announcement.Index))
	}

	if len(v2AnnounceAddresses) > 0 {
		h := types.NewHasher()
		types.EncodeSlice(h.E, v2AnnounceAddresses)
		if err := h.E.Flush(); err != nil {
			return fmt.Errorf("failed to hash v2 announcement addresses: %w", err)
		} else if err := tx.SetLastV2AnnouncementHash(h.Sum(), v2AnnounceIndex); err != nil {
			return fmt.Errorf("failed to set last v2 announcement: %w", err)
		}

		addresses := make([]string, 0, len(v2AnnounceAddresses))
		for _, addr := range v2AnnounceAddresses {
			addresses = append(addresses, fmt.Sprintf("%s/%s", addr.Protocol, addr.Address)) // TODO: Stringer?
		}
		cm.log.Debug("v2 announcement confirmed", zap.Strings("addresses", addresses), zap.Stringer("index", v2AnnounceIndex))
	}
	return nil
}

// ProcessActions processes announcement actions based on the given chain index.
func (m *ConfigManager) ProcessActions(index types.ChainIndex) error {
	n := m.chain.TipState().Network
	hostPub := m.hostKey.PublicKey()

	// check if there is an unconfirmed announcement
	var unconfirmedAnnouncement bool
	for _, txn := range m.chain.PoolTransactions() {
		var ha chain.HostAnnouncement
		for _, arb := range txn.ArbitraryData {
			if !ha.FromArbitraryData(arb) {
				continue
			} else if ha.PublicKey == hostPub {
				unconfirmedAnnouncement = true
				break
			}
		}
	}
	for _, txn := range m.chain.V2PoolTransactions() {
		for _, att := range txn.Attestations {
			if att.PublicKey == hostPub {
				unconfirmedAnnouncement = true
				break
			}
		}
	}

	if unconfirmedAnnouncement {
		return nil
	}

	var shouldAnnounce bool
	if index.Height < n.HardforkV2.AllowHeight {
		announcement, err := m.store.LastAnnouncement()
		if err != nil {
			return fmt.Errorf("failed to get last announcement: %w", err)
		}

		nextHeight := announcement.Index.Height + m.announceInterval
		netaddress := m.Settings().NetAddress
		if err := validateNetAddress(netaddress); err != nil && m.validateNetAddress {
			m.log.Debug("failed to validate net address", zap.Error(err))
			return nil
		}
		shouldAnnounce = index.Height >= nextHeight || announcement.Address != netaddress
	} else {
		announceHash, announceIndex, err := m.store.LastV2AnnouncementHash()
		if err != nil {
			return fmt.Errorf("failed to get last v2 announcement: %w", err)
		}

		nextHeight := announceIndex.Height + m.announceInterval
		h := types.NewHasher()
		types.EncodeSlice(h.E, chain.V2HostAnnouncement{{Protocol: rhp4.ProtocolTCPSiaMux, Address: m.Settings().NetAddress}})
		if err := h.E.Flush(); err != nil {
			return fmt.Errorf("failed to hash v2 announcement: %w", err)
		}
		shouldAnnounce = index.Height >= nextHeight || announceHash != h.Sum()
	}

	if shouldAnnounce {
		if err := m.Announce(); err != nil {
			m.log.Debug("failed to announce", zap.Error(err))
		}
	}
	return nil
}
