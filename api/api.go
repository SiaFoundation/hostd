package api

import (
	"net/http"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

type (
	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		ScanHeight() uint64
		Balance() (spendable, confirmed, unconfirmed types.Currency, err error)
		UnconfirmedTransactions() ([]wallet.Transaction, error)
		FundTransaction(txn *types.Transaction, amount types.Currency) (toSign []types.Hash256, release func(), err error)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
		Transactions(limit, offset int) ([]wallet.Transaction, error)
	}

	// Settings updates and retrieves the host's settings
	Settings interface {
		Announce() error
		UpdateSettings(s settings.Settings) error
		Settings() settings.Settings
	}

	// A VolumeManager manages the host's storage volumes
	VolumeManager interface {
		Usage() (usedBytes uint64, totalBytes uint64, err error)
		Volumes() ([]storage.VolumeMeta, error)
		Volume(id int) (storage.VolumeMeta, error)
		AddVolume(localPath string, maxSectors uint64) (storage.Volume, error)
		SetReadOnly(id int, readOnly bool) error
		RemoveVolume(id int, force bool) error
		ResizeVolume(id int, maxSectors uint64) error
		RemoveSector(root types.Hash256) error
	}

	// A ContractManager manages the host's contracts
	ContractManager interface {
		Contracts(filter contracts.ContractFilter) ([]contracts.Contract, error)
		Contract(id types.FileContractID) (contracts.Contract, error)
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		Address() modules.NetAddress
		Peers() []modules.Peer
		Connect(addr modules.NetAddress) error
		Disconnect(addr modules.NetAddress) error
	}

	// An API provides an HTTP API for the host
	API struct {
		log *zap.Logger

		syncer    Syncer
		contracts ContractManager
		volumes   VolumeManager
		wallet    Wallet
		settings  Settings
	}
)

// NewServer initializes the API
func NewServer(g Syncer, cm ContractManager, vm VolumeManager, s Settings, w Wallet, log *zap.Logger) http.Handler {
	a := &API{
		syncer:    g,
		contracts: cm,
		volumes:   vm,
		settings:  s,
		wallet:    w,
		log:       log,
	}
	r := jape.Mux(map[string]jape.Handler{
		"GET /":                         a.handleGETState,
		"GET /syncer/address":           a.handleGETSyncerAddr,
		"GET /syncer/peers":             a.handleGETSyncerPeers,
		"PUT /syncer/peers/:address":    a.handlePUTSyncerPeer,
		"DELETE /syncer/peers/:address": a.handleDeleteSyncerPeer,
		"GET /settings":                 a.handleGETSettings,
		"POST /settings":                a.handlePOSTSettings,
		"POST /settings/announce":       a.handlePOSTAnnounce,
		"GET /financials/:period":       a.handleGETFinancials,
		"POST /contracts":               a.handlePostContracts,
		"GET /contracts/:id":            a.handleGETContract,
		"DELETE /sectors/:root":         a.handleDeleteSector,
		"GET /volumes":                  a.handleGETVolumes,
		"POST /volumes":                 a.handlePOSTVolume,
		"GET /volumes/:id":              a.handleGETVolume,
		"PUT /volumes/:id":              a.handlePUTVolume,
		"DELETE /volumes/:id":           a.handleDeleteVolume,
		"PUT /volumes/:id/resize":       a.handlePUTVolumeResize,
		"POST /volumes/:id/check":       a.handlePOSTVolumeCheck,
		"GET /wallet":                   a.handleGETWallet,
		"GET /wallet/transactions":      a.handleGETWalletTransactions,
		"GET /wallet/pending":           a.handleGETWalletPending,
		"POST /wallet/send":             a.handlePOSTWalletSend,
	})
	return r
}
