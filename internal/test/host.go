package test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	crhp2 "go.sia.tech/core/rhp/v2"
	crhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhp2 "go.sia.tech/hostd/rhp/v2"
	rhp3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/hostd/webhooks"
	"go.uber.org/zap"
)

const blocksPerMonth = 144 * 30

type stubDataMonitor struct{}

func (stubDataMonitor) ReadBytes(n int)  {}
func (stubDataMonitor) WriteBytes(n int) {}

// A Host is an ephemeral host that can be used for testing.
type Host struct {
	*Node

	privKey   types.PrivateKey
	store     *sqlite.Store
	log       *zap.Logger
	wallet    *wallet.SingleAddressWallet
	settings  *settings.ConfigManager
	storage   *storage.VolumeManager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager

	rhp2   *rhp2.SessionHandler
	rhp3   *rhp3.SessionHandler
	rhp3WS net.Listener
}

// DefaultSettings returns the default settings for the test host
var DefaultSettings = settings.Settings{
	AcceptingContracts:  true,
	MaxContractDuration: blocksPerMonth * 3,
	WindowSize:          144,
	MaxCollateral:       types.Siacoins(5000),

	ContractPrice: types.Siacoins(1).Div64(4),

	BaseRPCPrice:      types.NewCurrency64(100),
	SectorAccessPrice: types.NewCurrency64(100),

	CollateralMultiplier: 2.0,
	StoragePrice:         types.Siacoins(100).Div64(1e12).Div64(blocksPerMonth),
	EgressPrice:          types.Siacoins(100).Div64(1e12),
	IngressPrice:         types.Siacoins(100).Div64(1e12),

	PriceTableValidity: 2 * time.Minute,

	AccountExpiry:     30 * 24 * time.Hour, // 1 month
	MaxAccountBalance: types.Siacoins(10),
	SectorCacheSize:   64,
}

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhp3WS.Close()
	h.rhp2.Close()
	h.rhp3.Close()
	h.settings.Close()
	h.wallet.Close()
	h.contracts.Close()
	h.storage.Close()
	h.store.Close()
	h.Node.Close()
	h.log.Sync()
	return nil
}

// RHP2Addr returns the address of the rhp2 listener
func (h *Host) RHP2Addr() string {
	return h.rhp2.LocalAddr()
}

// RHP3Addr returns the address of the rhp3 listener
func (h *Host) RHP3Addr() string {
	return h.rhp3.LocalAddr()
}

// RHP3WSAddr returns the address of the rhp3 WebSocket listener
func (h *Host) RHP3WSAddr() string {
	return h.rhp3WS.Addr().String()
}

// AddVolume adds a new volume to the host
func (h *Host) AddVolume(path string, size uint64) error {
	result := make(chan error, 1)
	if _, err := h.storage.AddVolume(context.Background(), path, size, result); err != nil {
		return err
	}
	return <-result
}

// UpdateSettings updates the host's configuration
func (h *Host) UpdateSettings(settings settings.Settings) error {
	return h.settings.UpdateSettings(settings)
}

// RHP2Settings returns the host's current rhp2 settings
func (h *Host) RHP2Settings() (crhp2.HostSettings, error) {
	return h.rhp2.Settings()
}

// RHP3PriceTable returns the host's current rhp3 price table
func (h *Host) RHP3PriceTable() (crhp3.HostPriceTable, error) {
	return h.rhp3.PriceTable()
}

// WalletAddress returns the host's wallet address
func (h *Host) WalletAddress() types.Address {
	return h.wallet.Address()
}

// Contracts returns the host's contract manager
func (h *Host) Contracts() *contracts.ContractManager {
	return h.contracts
}

// Storage returns the host's storage manager
func (h *Host) Storage() *storage.VolumeManager {
	return h.storage
}

// Settings returns the host's settings manager
func (h *Host) Settings() *settings.ConfigManager {
	return h.settings
}

// PublicKey returns the host's public key
func (h *Host) PublicKey() types.PublicKey {
	return h.privKey.PublicKey()
}

// Accounts returns the host's account manager
func (h *Host) Accounts() *accounts.AccountManager {
	return h.accounts
}

// Store returns the host's database
func (h *Host) Store() *sqlite.Store {
	return h.store
}

// NewHost initializes a new test host
func NewHost(privKey types.PrivateKey, dir string, node *Node, log *zap.Logger) (*Host, error) {
	host, err := NewEmptyHost(privKey, dir, node, log)
	if err != nil {
		return nil, err
	}

	result := make(chan error, 1)
	if _, err := host.Storage().AddVolume(context.Background(), filepath.Join(dir, "storage.dat"), 64, result); err != nil {
		return nil, fmt.Errorf("failed to add storage volume: %w", err)
	} else if err := <-result; err != nil {
		return nil, fmt.Errorf("failed to add storage volume: %w", err)
	}
	s := DefaultSettings
	s.NetAddress = host.RHP2Addr()
	if err := host.Settings().UpdateSettings(s); err != nil {
		return nil, fmt.Errorf("failed to update host settings: %w", err)
	}
	return host, nil
}

// NewEmptyHost initializes a new test host
func NewEmptyHost(privKey types.PrivateKey, dir string, node *Node, log *zap.Logger) (*Host, error) {
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}

	wallet, err := wallet.NewSingleAddressWallet(privKey, node.cm, node.tp, db, log.Named("wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	wr, err := webhooks.NewManager(db, log.Named("webhooks"))
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook reporter: %w", err)
	}

	am := alerts.NewManager(wr, log.Named("alerts"))
	storage, err := storage.NewVolumeManager(db, am, node.cm, log.Named("storage"), DefaultSettings.SectorCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	contracts, err := contracts.NewManager(db, am, storage, node.cm, node.tp, wallet, log.Named("contracts"))
	if err != nil {
		return nil, fmt.Errorf("failed to create contract manager: %w", err)
	}

	rhp2Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp2 listener: %w", err)
	}

	rhp3Listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp2 listener: %w", err)
	}

	settings, err := settings.NewConfigManager(dir, privKey, rhp2Listener.Addr().String(), db, node.cm, node.tp, wallet, am, log.Named("settings"))
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	accounts := accounts.NewManager(db, settings)

	sessions := rhp.NewSessionReporter()

	rhp2, err := rhp2.NewSessionHandler(rhp2Listener, privKey, rhp3Listener.Addr().String(), node.cm, node.tp, wallet, contracts, settings, storage, stubDataMonitor{}, sessions, log.Named("rhp2"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp2 session handler: %w", err)
	}
	go rhp2.Serve()

	rhp3, err := rhp3.NewSessionHandler(rhp3Listener, privKey, node.cm, node.tp, wallet, accounts, contracts, storage, settings, stubDataMonitor{}, sessions, log.Named("rhp3"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp3 session handler: %w", err)
	}
	go rhp3.Serve()

	rhp3WSListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("failed to create rhp3 websocket listener: %w", err)
	}

	go func() {
		rhp3WS := http.Server{
			Handler:     rhp3.WebSocketHandler(),
			ReadTimeout: 30 * time.Second,
		}

		if err := rhp3WS.Serve(rhp3WSListener); err != nil {
			return
		}
	}()

	return &Host{
		Node:      node,
		privKey:   privKey,
		store:     db,
		log:       log,
		wallet:    wallet,
		settings:  settings,
		storage:   storage,
		accounts:  accounts,
		contracts: contracts,

		rhp2:   rhp2,
		rhp3:   rhp3,
		rhp3WS: rhp3WSListener,
	}, nil
}
