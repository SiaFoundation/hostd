package settings

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	announcementDebounce = 18 // blocks
)

type (
	// An Announcement contains the host's announced netaddress and public key
	Announcement struct {
		Index     types.ChainIndex `json:"index"`
		PublicKey types.PublicKey  `json:"publicKey"`
		Address   string           `json:"address"`
	}
)

// constant to overwrite announcement alerts instead of registering new ones
var alertAnnouncementID = frand.Entropy256()

// Announce announces the host to the network
func (m *ConfigManager) Announce() error {
	// get the current settings
	settings := m.Settings()
	// if no netaddress is set, override the field with the auto-discovered one
	if settings.NetAddress == "" {
		settings.NetAddress = m.discoveredRHPAddr
	}

	if err := validateNetAddress(settings.NetAddress); err != nil {
		return err
	}

	// create a transaction with an announcement
	minerFee := m.tp.RecommendedFee().Mul64(announcementTxnSize)
	txn := types.Transaction{
		ArbitraryData: [][]byte{
			createAnnouncement(m.hostKey, settings.NetAddress),
		},
		MinerFees: []types.Currency{minerFee},
	}

	// fund the transaction
	toSign, release, err := m.wallet.FundTransaction(&txn, minerFee)
	if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	// sign the transaction
	err = m.wallet.SignTransaction(m.cm.TipState(), &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if err != nil {
		release()
		return fmt.Errorf("failed to sign transaction: %w", err)
	}
	// broadcast the transaction
	err = m.tp.AcceptTransactionSet([]types.Transaction{txn})
	if err != nil {
		release()
		return fmt.Errorf("failed to broadcast transaction: %w", err)
	}
	m.log.Debug("broadcast announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	return nil
}

// ProcessConsensusChange implements modules.ConsensusSetSubscriber.
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
	blockHeight := uint64(cc.BlockHeight)
	for _, block := range cc.RevertedBlocks {
		if types.BlockID(block.ID()) == lastAnnouncement.Index.ID {
			log.Info("resetting announcement due to block revert", zap.Uint64("height", blockHeight), zap.String("address", lastAnnouncement.Address), zap.Stringer("publicKey", hostPub))
			if err = cm.store.RevertLastAnnouncement(); err != nil {
				log.Fatal("failed to revert last announcement", zap.Error(err))
			}
		}
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
				log.Info("announcement confirmed", zap.String("address", announcement.Address), zap.Uint64("height", blockHeight))
			}
		}
		blockHeight++
	}

	// get the last announcement again, in case it was updated
	lastAnnouncement, err = cm.store.LastAnnouncement()
	if err != nil {
		log.Fatal("failed to get last announcement", zap.Error(err))
	}

	// get the current net address
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if err := validateNetAddress(cm.settings.NetAddress); err != nil {
		log.Debug("skipping auto announcement for invalid net address", zap.Error(err))
		return
	}

	currentNetAddress := cm.settings.NetAddress
	cm.scanHeight = uint64(cc.BlockHeight)
	timestamp := time.Unix(int64(cc.AppliedBlocks[len(cc.AppliedBlocks)-1].Timestamp), 0)
	nextAnnounceHeight := lastAnnouncement.Index.Height + autoAnnounceInterval

	log = log.With(zap.Uint64("currentHeight", cm.scanHeight), zap.Uint64("lastHeight", lastAnnouncement.Index.Height), zap.Uint64("nextHeight", nextAnnounceHeight), zap.String("currentAddress", currentNetAddress), zap.String("oldAddress", lastAnnouncement.Address))

	// if the address hasn't changed, don't reannounce
	if cm.scanHeight < nextAnnounceHeight && currentNetAddress == lastAnnouncement.Address {
		log.Debug("skipping announcement for unchanged address")
		return
	}

	// debounce announcements
	if cm.scanHeight < cm.lastAnnounceAttempt+announcementDebounce || time.Since(timestamp) > 3*time.Hour {
		return
	}

	log.Debug("announcing host")
	cm.lastAnnounceAttempt = cm.scanHeight

	// in go-routine to prevent deadlock with TPool
	go func() {
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
			return
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
