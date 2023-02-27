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
		Balance() (spendable, confirmed types.Currency, err error)
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
		Contracts(limit, offset int) ([]contracts.Contract, error)
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
		"GET /":                          a.handleGetState,
		"GET /syncer/address":            a.handleGetSyncerAddr,
		"GET /syncer/peers":              a.handleGetSyncerPeers,
		"PUT /syncer/peers/:address":     a.handlePutSyncerPeer,
		"DELETE /syncer/peers/:address":  a.handleDeleteSyncerPeer,
		"POST /announce":                 a.handlePostAnnounce,
		"GET /settings":                  a.handleGetSettings,
		"PUT /settings":                  a.handlePutSettings,
		"GET /financials/:period":        a.handleGetFinancials,
		"GET /contracts":                 a.handleGetContracts,
		"GET /contracts/:id":             a.handleGetContract,
		"DELETE /sectors/:root":          a.handleDeleteSector,
		"GET /volumes":                   a.handleGetVolumes,
		"POST /volumes":                  a.handlePostVolume,
		"GET /volumes/:id":               a.handleGetVolume,
		"PUT /volumes/:id":               a.handlePutVolume,
		"DELETE /volumes/:id":            a.handleDeleteVolume,
		"PUT /volumes/:id/resize":        a.handlePutVolumeResize,
		"PUT /volumes/:id/check":         a.handlePutVolumeCheck,
		"GET /wallet":                    a.handleGetWallet,
		"GET /wallet/address":            a.handleGetWalletAddress,
		"GET /wallet/transactions":       a.handleGetWalletTransactions,
		"POST /wallet/transactions/send": a.handlePostWalletSend,
	})
	return r
}
