package settings

import (
	"crypto/ed25519"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type (
	Announcement struct {
		Index     types.ChainIndex `json:"index"`
		PublicKey types.PublicKey  `json:"publicKey"`
		Address   string           `json:"address"`
	}
)

// constant to overwrite announcement alerts instead of registering new ones
var alertAnnouncementID = frand.Entropy256()

func (cm *ConfigManager) ProcessConsensusChange(cc modules.ConsensusChange) {
	done, err := cm.tg.Add()
	if err != nil {
		return
	}
	defer done()

	log := cm.log.Named("consensusChange")

	lastAnnouncement, err := cm.store.LastAnnouncement()
	if err != nil {
		log.Fatal("failed to get last announcement", zap.Error(err))
	}

	hostPub := cm.hostKey.PublicKey()

	// check if the host key changed (should not happen)
	if lastAnnouncement.PublicKey != (types.PublicKey{}) && lastAnnouncement.PublicKey != hostPub {
		log.Error("resetting announcement due to host key change", zap.Stringer("oldKey", lastAnnouncement.PublicKey), zap.Stringer("newKey", hostPub))
		if err = cm.store.RevertLastAnnouncement(); err != nil {
			log.Fatal("failed to reset announcements", zap.Error(err))
		}
	}

	// check if a block containing the announcement was reverted
	blockHeight := uint64(cc.BlockHeight) - uint64(len(cc.AppliedBlocks)) + uint64(len(cc.RevertedBlocks)) + 1
	for _, block := range cc.RevertedBlocks {
		if types.BlockID(block.ID()) == lastAnnouncement.Index.ID {
			if err = cm.store.RevertLastAnnouncement(); err != nil {
				log.Fatal("failed to revert last announcement", zap.Error(err))
			}
		}
		lastAnnouncement = Announcement{}
		blockHeight--
	}

	// check for new announcements
	for _, block := range cc.AppliedBlocks {
		blockID := types.BlockID(block.ID())
		for _, txn := range block.Transactions {
			for _, arb := range txn.ArbitraryData {
				address, pubkey, err := modules.DecodeAnnouncement(arb)
				if err != nil || pubkey.Algorithm != stypes.SignatureEd25519 || len(pubkey.Key) != ed25519.PublicKeySize || len(address) == 0 {
					continue
				}
				announcement := Announcement{
					PublicKey: types.PublicKey(pubkey.Key),
					Address:   string(address),
					Index: types.ChainIndex{
						ID:     blockID,
						Height: blockHeight,
					},
				}

				if announcement.PublicKey != hostPub {
					continue
				}

				// update the announcement
				if err := cm.store.UpdateLastAnnouncement(announcement); err != nil {
					log.Fatal("failed to update last announcement", zap.Error(err))
				}
				lastAnnouncement = announcement
				cm.a.Register(alerts.Alert{
					ID:       alertAnnouncementID,
					Severity: alerts.SeverityInfo,
					Message:  "Announcement confirmed",
					Data: map[string]any{
						"address": announcement.Address,
						"height":  blockHeight,
					},
					Timestamp: time.Now(),
				})
			}
		}
		blockHeight++
	}

	var minAnnounceHeight uint64
	if cc.BlockHeight > 144*180 {
		minAnnounceHeight = uint64(cc.BlockHeight) - (144 * 180) // reannounce every 180 days
	}

	// get the current net address
	cm.mu.Lock()
	defer cm.mu.Unlock()
	currentNetAddress := cm.settings.NetAddress
	cm.scanHeight = uint64(cc.BlockHeight)

	// if the current net address is empty, has not changed, or the last announcement is recent, don't announce
	if len(currentNetAddress) == 0 || strings.TrimSpace(currentNetAddress) == "" || currentNetAddress == lastAnnouncement.Address && lastAnnouncement.Index.Height > minAnnounceHeight {
		return
	}

	// in go-routine to prevent deadlock with TPool
	go func() {
		log = log.With(zap.String("address", currentNetAddress), zap.String("oldAddress", lastAnnouncement.Address), zap.Uint64("lastHeight", lastAnnouncement.Index.Height))
		if err := cm.Announce(); err != nil {
			log.Error("failed to announce host", zap.Error(err))
			cm.a.Register(alerts.Alert{
				ID:       alertAnnouncementID,
				Severity: alerts.SeverityWarning,
				Message:  "Announcement failed",
				Data: map[string]any{
					"error": err.Error(),
				},
				Timestamp: time.Now(),
			})
		}
		log.Info("announced host")
		cm.a.Register(alerts.Alert{
			ID:       alertAnnouncementID,
			Severity: alerts.SeverityInfo,
			Message:  "Announcement broadcast",
			Data: map[string]any{
				"address": currentNetAddress,
				"height":  lastAnnouncement.Index.Height,
			},
			Timestamp: time.Now(),
		})
	}()
}
