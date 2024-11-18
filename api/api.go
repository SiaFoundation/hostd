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
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/explorer"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/webhooks"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		Balance() (balance wallet.Balance, err error)
		UnconfirmedEvents() ([]wallet.Event, error)
		Events(offset, limit int) ([]wallet.Event, error)

		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)

		// v1
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)

		// v2
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}

	// Settings updates and retrieves the host's settings
	Settings interface {
		Announce() error

		UpdateSettings(s settings.Settings) error
		Settings() settings.Settings
		LastAnnouncement() (settings.Announcement, error)

		UpdateDDNS(force bool) error
	}

	// An Index persists updates from the blockchain to a store
	Index interface {
		Tip() types.ChainIndex
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
		Addr() string
		Peers() []*syncer.Peer
		PeerInfo(string) (syncer.PeerInfo, error)
		Connect(ctx context.Context, addr string) (*syncer.Peer, error)

		BroadcastTransactionSet(txns []types.Transaction)
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction)
	}

	// The SQLite3Store provides an interface for backing up a SQLite3 database
	SQLite3Store interface {
		Backup(ctx context.Context, destPath string) error
	}

	// A ChainManager retrieves the current blockchain state
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State

		RecommendedFee() types.Currency
		AddPoolTransactions(txns []types.Transaction) (known bool, err error)
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// Webhooks manages webhooks
	Webhooks interface {
		Webhooks() ([]webhooks.Webhook, error)
		RegisterWebhook(callbackURL string, scopes []string) (webhooks.Webhook, error)
		UpdateWebhook(id int64, callbackURL string, scopes []string) (webhooks.Webhook, error)
		RemoveWebhook(id int64) error
		BroadcastToWebhook(id int64, event, scope string, data interface{}) error
	}

	// An api provides an HTTP API for the host
	api struct {
		hostKey types.PublicKey
		name    string

		log      *zap.Logger
		alerts   Alerts
		webhooks Webhooks

		sqlite3Store SQLite3Store

		syncer    Syncer
		chain     ChainManager
		accounts  AccountManager
		contracts ContractManager
		volumes   VolumeManager
		wallet    Wallet
		metrics   MetricManager
		settings  Settings
		index     Index

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
// syncer
// chain
// accounts
// contracts
// volumes
// wallet
// metrics
// settings
// index
func NewServer(name string, hostKey types.PublicKey, cm ChainManager, s Syncer, am AccountManager, c ContractManager, vm VolumeManager, wm Wallet, mm MetricManager, sm Settings, im Index, opts ...ServerOption) http.Handler {
	a := &api{
		hostKey: hostKey,
		name:    name,

		alerts:   noopAlerts{},
		webhooks: noopWebhooks{},
		log:      zap.NewNop(),

		syncer:    s,
		chain:     cm,
		accounts:  am,
		contracts: c,
		volumes:   vm,
		wallet:    wm,
		metrics:   mm,
		settings:  sm,
		index:     im,

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
		"GET /state": a.handleGETState,
		// consensus endpoints
		"GET /consensus/tip":      a.handleGETConsensusTip,
		"GET /consensus/tipstate": a.handleGETConsensusTipState,
		"GET /consensus/network":  a.handleGETConsensusNetwork,
		// syncer endpoints
		"GET /syncer/address": a.handleGETSyncerAddr,
		"GET /syncer/peers":   a.handleGETSyncerPeers,
		"PUT /syncer/peers":   a.handlePUTSyncerPeer,
		// index endpoints
		"GET /index/tip": a.handleGETIndexTip,
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
		// tpool endpoints
		"GET /tpool/fee": a.handleGETTPoolFee,
		// wallet endpoints
		"GET /wallet":         a.handleGETWallet,
		"GET /wallet/events":  a.handleGETWalletEvents,
		"GET /wallet/pending": a.handleGETWalletPending,
		"POST /wallet/send":   a.handlePOSTWalletSend,
		// system endpoints
		"GET /system/dir":             a.handleGETSystemDir,
		"PUT /system/dir":             a.handlePUTSystemDir,
		"POST /system/sqlite3/backup": a.handlePOSTSystemSQLite3Backup,
		// webhook endpoints
		"GET /webhooks":           a.handleGETWebhooks,
		"POST /webhooks":          a.handlePOSTWebhooks,
		"PUT /webhooks/:id":       a.handlePUTWebhooks,
		"POST /webhooks/:id/test": a.handlePOSTWebhooksTest,
		"DELETE /webhooks/:id":    a.handleDELETEWebhooks,
	})
}
