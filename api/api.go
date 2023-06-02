package api

import (
	"context"
	"net/http"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/logging"
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

		UpdateDDNS(force bool) error
	}

	// Metrics retrieves metrics related to the host
	Metrics interface {
		// PeriodMetrics returns metrics for n periods starting at start.
		PeriodMetrics(start time.Time, periods int, interval metrics.Interval) (period []metrics.Metrics, err error)
		// Metrics returns aggregated metrics for the host as of the timestamp.
		Metrics(time.Time) (m metrics.Metrics, err error)
	}

	// A VolumeManager manages the host's storage volumes
	VolumeManager interface {
		Usage() (usedSectors uint64, totalSectors uint64, err error)
		Volumes() ([]storage.VolumeMeta, error)
		Volume(id int) (storage.VolumeMeta, error)
		AddVolume(localPath string, maxSectors uint64, result chan<- error) (storage.Volume, error)
		RemoveVolume(id int, force bool, result chan<- error) error
		ResizeVolume(id int, maxSectors uint64, result chan<- error) error
		SetReadOnly(id int, readOnly bool) error
		RemoveSector(root types.Hash256) error
	}

	// A ContractManager manages the host's contracts
	ContractManager interface {
		Contracts(filter contracts.ContractFilter) ([]contracts.Contract, int, error)
		Contract(id types.FileContractID) (contracts.Contract, error)

		// CheckIntegrity checks the integrity of a contract's sector roots on
		// disk. The result of each sector checked is sent on the returned
		// channel. Read errors are logged.
		CheckIntegrity(ctx context.Context, contractID types.FileContractID) (<-chan contracts.IntegrityResult, uint64, error)
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		Address() modules.NetAddress
		Peers() []modules.Peer
		Connect(addr modules.NetAddress) error
		Disconnect(addr modules.NetAddress) error
	}

	// A ChainManager retrieves the current blockchain state
	ChainManager interface {
		Synced() bool
		TipState() consensus.State
	}

	// A LogStore retrieves host logs
	LogStore interface {
		LogEntries(logging.Filter) ([]logging.Entry, int, error)
		Prune(time.Time) error
	}

	// A TPool manages the transaction pool
	TPool interface {
		RecommendedFee() (fee types.Currency)
		AcceptTransactionSet(txns []types.Transaction) error
	}

	// An api provides an HTTP API for the host
	api struct {
		hostKey types.PublicKey

		log *zap.Logger

		syncer    Syncer
		chain     ChainManager
		tpool     TPool
		contracts ContractManager
		volumes   VolumeManager
		wallet    Wallet
		logs      LogStore
		metrics   Metrics
		settings  Settings

		checks integrityCheckJobs
	}
)

// NewServer initializes the API
func NewServer(hostKey types.PublicKey, g Syncer, chain ChainManager, tp TPool, cm ContractManager, vm VolumeManager, m Metrics, ls LogStore, s Settings, w Wallet, log *zap.Logger) http.Handler {
	a := &api{
		hostKey: hostKey,

		checks: integrityCheckJobs{
			contracts: cm,
			checks:    make(map[types.FileContractID]IntegrityCheckResult),
		},

		syncer:    g,
		chain:     chain,
		tpool:     tp,
		contracts: cm,
		volumes:   vm,
		logs:      ls,
		metrics:   m,
		settings:  s,
		wallet:    w,
		log:       log,
	}
	return jape.Mux(map[string]jape.Handler{
		// state endpoints
		"GET /state/host":      a.handleGETHostState,
		"GET /state/consensus": a.handleGETConsensusState,
		// gateway endpoints
		"GET /syncer/address":           a.handleGETSyncerAddr,
		"GET /syncer/peers":             a.handleGETSyncerPeers,
		"PUT /syncer/peers":             a.handlePUTSyncerPeer,
		"DELETE /syncer/peers/:address": a.handleDeleteSyncerPeer,
		// settings endpoints
		"GET /settings":             a.handleGETSettings,
		"PATCH /settings":           a.handlePATCHSettings,
		"POST /settings/announce":   a.handlePOSTAnnounce,
		"PUT /settings/ddns/update": a.handlePUTDDNSUpdate,
		// metrics endpoints
		"GET /metrics":         a.handleGETMetrics,
		"GET /metrics/:period": a.handleGETPeriodMetrics,
		// contract endpoints
		"POST /contracts":                 a.handlePostContracts,
		"GET /contracts/:id":              a.handleGETContract,
		"GET /contracts/:id/integrity":    a.handleGETContractCheck,
		"PUT /contracts/:id/integrity":    a.handlePUTContractCheck,
		"DELETE /contracts/:id/integrity": a.handleDeleteContractCheck,
		// sector endpoints
		"DELETE /sectors/:root": a.handleDeleteSector,
		// volume endpoints
		"GET /volumes":            a.handleGETVolumes,
		"POST /volumes":           a.handlePOSTVolume,
		"GET /volumes/:id":        a.handleGETVolume,
		"PUT /volumes/:id":        a.handlePUTVolume,
		"DELETE /volumes/:id":     a.handleDeleteVolume,
		"PUT /volumes/:id/resize": a.handlePUTVolumeResize,
		// tpool endpoints
		"GET /tpool/fee": a.handleGETTPoolFee,
		// wallet endpoints
		"GET /wallet":              a.handleGETWallet,
		"GET /wallet/transactions": a.handleGETWalletTransactions,
		"GET /wallet/pending":      a.handleGETWalletPending,
		"POST /wallet/send":        a.handlePOSTWalletSend,
		// system endpoints
		"GET /system/dir": a.handleGETSystemDir,
		"PUT /system/dir": a.handlePUTSystemDir,
		// log endpoints
		"POST /log/entries":   a.handlePOSTLogEntries,
		"DELETE /log/entries": a.handleDELETELogEntries,
	})
}
