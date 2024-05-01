package api

import (
	"context"
	"errors"
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
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/explorer"
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

	// PinnedSettings updates and retrieves the host's pinned settings
	PinnedSettings interface {
		Update(context.Context, pin.PinnedSettings) error
		Pinned(context.Context) pin.PinnedSettings
	}

	// A MetricManager retrieves metrics related to the host
	MetricManager interface {
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
		BroadcastToWebhook(id int64, event, scope string, data interface{}) error
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
		metrics   MetricManager
		settings  Settings
		sessions  RHPSessionReporter

		explorerDisabled bool
		explorer         *explorer.Explorer
		pinned           PinnedSettings

		volumeJobs volumeJobs
		checks     integrityCheckJobs
	}
)

func (a *api) requiresExplorer(h jape.Handler) jape.Handler {
	return func(ctx jape.Context) {
		if a.explorerDisabled {
			ctx.Error(errors.New("explorer data is disabled"), http.StatusNotFound)
			return
		}
		h(ctx)
	}
}

// NewServer initializes the API
func NewServer(name string, hostKey types.PublicKey, opts ...ServerOption) http.Handler {
	a := &api{
		hostKey: hostKey,
		name:    name,
		log:     zap.NewNop(),

		explorerDisabled: true,
	}
	for _, opt := range opts {
		opt(a)
	}
	a.checks = integrityCheckJobs{
		contracts: a.contracts,
		checks:    make(map[types.FileContractID]IntegrityCheckResult),
	}
	a.volumeJobs = volumeJobs{
		volumes: a.volumes,
		jobs:    make(map[int64]context.CancelFunc),
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
		// alerts endpoints
		"GET /alerts":          a.handleGETAlerts,
		"POST /alerts/dismiss": a.handlePOSTAlertsDismiss,
		// settings endpoints
		"GET /settings":             a.handleGETSettings,
		"PATCH /settings":           a.handlePATCHSettings,
		"POST /settings/announce":   a.handlePOSTAnnounce,
		"PUT /settings/ddns/update": a.handlePUTDDNSUpdate,
		"GET /settings/pinned":      a.requiresExplorer(a.handleGETPinnedSettings),
		"PUT /settings/pinned":      a.requiresExplorer(a.handlePUTPinnedSettings),
		// metrics endpoints
		"GET /metrics":         a.handleGETMetrics,
		"GET /metrics/:period": a.handleGETPeriodMetrics,
		// contract endpoints
		"POST /contracts":                 a.handlePostContracts,
		"GET /contracts/:id":              a.handleGETContract,
		"GET /contracts/:id/integrity":    a.handleGETContractCheck,
		"PUT /contracts/:id/integrity":    a.handlePUTContractCheck,
		"DELETE /contracts/:id/integrity": a.handleDeleteContractCheck,
		// account endpoints
		"GET /accounts":                  a.handleGETAccounts,
		"GET /accounts/:account/funding": a.handleGETAccountFunding,
		// sector endpoints
		"DELETE /sectors/:root":     a.handleDeleteSector,
		"GET /sectors/:root/verify": a.handleGETVerifySector,
		// volume endpoints
		"GET /volumes":               a.handleGETVolumes,
		"POST /volumes":              a.handlePOSTVolume,
		"GET /volumes/:id":           a.handleGETVolume,
		"PUT /volumes/:id":           a.handlePUTVolume,
		"DELETE /volumes/:id":        a.handleDeleteVolume,
		"DELETE /volumes/:id/cancel": a.handleDELETEVolumeCancelOp,
		"PUT /volumes/:id/resize":    a.handlePUTVolumeResize,
		// session endpoints
		"GET /sessions":           a.handleGETSessions,
		"GET /sessions/subscribe": a.handleGETSessionsSubscribe,
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
		// webhook endpoints
		"GET /webhooks":           a.handleGETWebhooks,
		"POST /webhooks":          a.handlePOSTWebhooks,
		"PUT /webhooks/:id":       a.handlePUTWebhooks,
		"POST /webhooks/:id/test": a.handlePOSTWebhooksTest,
		"DELETE /webhooks/:id":    a.handleDELETEWebhooks,
	})
}
