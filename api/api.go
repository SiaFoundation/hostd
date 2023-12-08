package api

import (
	"context"
	"net/http"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/rhp"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/hostd/webhooks"
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
		LastAnnouncement() (settings.Announcement, error)

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
		Volume(id int64) (storage.VolumeMeta, error)
		AddVolume(ctx context.Context, localPath string, maxSectors uint64, result chan<- error) (storage.Volume, error)
		RemoveVolume(ctx context.Context, id int64, force bool, result chan<- error) error
		ResizeVolume(ctx context.Context, id int64, maxSectors uint64, result chan<- error) error
		SetReadOnly(id int64, readOnly bool) error
		RemoveSector(root types.Hash256) error
		ResizeCache(size uint32)
		Read(types.Hash256) (*[rhp2.SectorSize]byte, error)

		// SectorReferences returns the references to a sector
		SectorReferences(root types.Hash256) (storage.SectorReference, error)
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

	// An AccountManager manages ephemeral accounts
	AccountManager interface {
		Accounts(limit, offset int) ([]accounts.Account, error)
		AccountFunding(accountID rhp3.Account) ([]accounts.FundingSource, error)
	}

	// Alerts retrieves and dismisses notifications
	Alerts interface {
		Active() []alerts.Alert
		Dismiss(...types.Hash256)
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

	// A TPool manages the transaction pool
	TPool interface {
		RecommendedFee() (fee types.Currency)
		AcceptTransactionSet(txns []types.Transaction) error
	}

	// WebHooks manages webhooks
	WebHooks interface {
		WebHooks() ([]webhooks.WebHook, error)
		RegisterWebHook(callbackURL string, scopes []string) (webhooks.WebHook, error)
		UpdateWebHook(id int64, callbackURL string, scopes []string) (webhooks.WebHook, error)
		RemoveWebHook(id int64) error
	}

	// A RHPSessionReporter reports on RHP session lifecycle events
	RHPSessionReporter interface {
		Subscribe(rhp.SessionSubscriber)
		Unsubscribe(rhp.SessionSubscriber)

		Active() []rhp.Session
	}

	// An api provides an HTTP API for the host
	api struct {
		hostKey types.PublicKey
		name    string

		log *zap.Logger

		alerts    Alerts
		webhooks  WebHooks
		syncer    Syncer
		chain     ChainManager
		tpool     TPool
		accounts  AccountManager
		contracts ContractManager
		volumes   VolumeManager
		wallet    Wallet
		metrics   Metrics
		settings  Settings
		sessions  RHPSessionReporter

		volumeJobs volumeJobs
		checks     integrityCheckJobs
	}
)

// NewServer initializes the API
func NewServer(name string, hostKey types.PublicKey, a Alerts, wh WebHooks, g Syncer, chain ChainManager, tp TPool, cm ContractManager, am AccountManager, vm VolumeManager, rsr RHPSessionReporter, m Metrics, s Settings, w Wallet, log *zap.Logger) http.Handler {
	api := &api{
		hostKey: hostKey,
		name:    name,

		alerts:    a,
		webhooks:  wh,
		syncer:    g,
		chain:     chain,
		tpool:     tp,
		contracts: cm,
		accounts:  am,
		volumes:   vm,
		metrics:   m,
		settings:  s,
		wallet:    w,
		sessions:  rsr,
		log:       log,

		checks: integrityCheckJobs{
			contracts: cm,
			checks:    make(map[types.FileContractID]IntegrityCheckResult),
		},
		volumeJobs: volumeJobs{
			volumes: vm,
			jobs:    make(map[int64]context.CancelFunc),
		},
	}
	return jape.Mux(map[string]jape.Handler{
		// state endpoints
		"GET /state/host":      api.handleGETHostState,
		"GET /state/consensus": api.handleGETConsensusState,
		// gateway endpoints
		"GET /syncer/address":           api.handleGETSyncerAddr,
		"GET /syncer/peers":             api.handleGETSyncerPeers,
		"PUT /syncer/peers":             api.handlePUTSyncerPeer,
		"DELETE /syncer/peers/:address": api.handleDeleteSyncerPeer,
		// alerts endpoints
		"GET /alerts":          api.handleGETAlerts,
		"POST /alerts/dismiss": api.handlePOSTAlertsDismiss,
		// settings endpoints
		"GET /settings":             api.handleGETSettings,
		"PATCH /settings":           api.handlePATCHSettings,
		"POST /settings/announce":   api.handlePOSTAnnounce,
		"PUT /settings/ddns/update": api.handlePUTDDNSUpdate,
		// metrics endpoints
		"GET /metrics":         api.handleGETMetrics,
		"GET /metrics/:period": api.handleGETPeriodMetrics,
		// contract endpoints
		"POST /contracts":                 api.handlePostContracts,
		"GET /contracts/:id":              api.handleGETContract,
		"GET /contracts/:id/integrity":    api.handleGETContractCheck,
		"PUT /contracts/:id/integrity":    api.handlePUTContractCheck,
		"DELETE /contracts/:id/integrity": api.handleDeleteContractCheck,
		// account endpoints
		"GET /accounts":                  api.handleGETAccounts,
		"GET /accounts/:account/funding": api.handleGETAccountFunding,
		// sector endpoints
		"DELETE /sectors/:root":     api.handleDeleteSector,
		"GET /sectors/:root/verify": api.handleGETVerifySector,
		// volume endpoints
		"GET /volumes":               api.handleGETVolumes,
		"POST /volumes":              api.handlePOSTVolume,
		"GET /volumes/:id":           api.handleGETVolume,
		"PUT /volumes/:id":           api.handlePUTVolume,
		"DELETE /volumes/:id":        api.handleDeleteVolume,
		"DELETE /volumes/:id/cancel": api.handleDELETEVolumeCancelOp,
		"PUT /volumes/:id/resize":    api.handlePUTVolumeResize,
		// session endpoints
		"GET /sessions":           api.handleGETSessions,
		"GET /sessions/subscribe": api.handleGETSessionsSubscribe,
		// tpool endpoints
		"GET /tpool/fee": api.handleGETTPoolFee,
		// wallet endpoints
		"GET /wallet":              api.handleGETWallet,
		"GET /wallet/transactions": api.handleGETWalletTransactions,
		"GET /wallet/pending":      api.handleGETWalletPending,
		"POST /wallet/send":        api.handlePOSTWalletSend,
		// system endpoints
		"GET /system/dir": api.handleGETSystemDir,
		"PUT /system/dir": api.handlePUTSystemDir,
		// webhook endpoints
		"GET /webhooks":        api.handleGETWebhooks,
		"POST /webhooks":       api.handlePOSTWebhooks,
		"PUT /webhooks/:id":    api.handlePUTWebhooks,
		"DELETE /webhooks/:id": api.handleDELETEWebhooks,
	})
}
