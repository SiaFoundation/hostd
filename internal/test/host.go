package test

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	crhpv2 "go.sia.tech/core/rhp/v2"
	crhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/persist/sqlite"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type stubMetricReporter struct{}

func (stubMetricReporter) Report(any) (_ error) { return }

// A Host is an ephemeral host that can be used for testing.
type Host struct {
	*Node

	store     *sqlite.Store
	wallet    *wallet.SingleAddressWallet
	settings  *settings.ConfigManager
	storage   *storage.VolumeManager
	registry  *registry.Manager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager

	rhpv2 *rhpv2.SessionHandler
	rhpv3 *rhpv3.SessionHandler
}

// DefaultSettings returns the default settings for the test host
var DefaultSettings = settings.Settings{
	AcceptingContracts:  true,
	MaxContractDuration: uint64(stypes.BlocksPerMonth) * 3,
	MaxCollateral:       types.Siacoins(5000),

	ContractPrice: types.Siacoins(1).Div64(4),

	BaseRPCPrice:      types.NewCurrency64(100),
	SectorAccessPrice: types.NewCurrency64(100),

	Collateral:      types.Siacoins(200).Div64(1e12).Div64(uint64(stypes.BlocksPerMonth)),
	MinStoragePrice: types.Siacoins(100).Div64(1e12).Div64(uint64(stypes.BlocksPerMonth)),
	MinEgressPrice:  types.Siacoins(100).Div64(1e12),
	MinIngressPrice: types.Siacoins(100).Div64(1e12),

	AccountExpiry:     30 * 24 * time.Hour, // 1 month
	MaxAccountBalance: types.Siacoins(10),
}

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhpv2.Close()
	h.rhpv3.Close()
	h.settings.Close()
	h.wallet.Close()
	h.contracts.Close()
	h.storage.Close()
	h.store.Close()
	h.Node.Close()
	return nil
}

// RHPv2Addr returns the address of the RHPv2 listener
func (h *Host) RHPv2Addr() string {
	return h.rhpv2.LocalAddr()
}

// RHPv3Addr returns the address of the RHPv3 listener
func (h *Host) RHPv3Addr() string {
	return h.rhpv3.LocalAddr()
}

// AddVolume adds a new volume to the host
func (h *Host) AddVolume(path string, size uint64) error {
	_, err := h.storage.AddVolume(path, size)
	return err
}

// UpdateSettings updates the host's configuration
func (h *Host) UpdateSettings(settings settings.Settings) error {
	return h.settings.UpdateSettings(settings)
}

// RHPv2Settings returns the host's current RHPv2 settings
func (h *Host) RHPv2Settings() (crhpv2.HostSettings, error) {
	return h.rhpv2.Settings()
}

// RHPv3PriceTable returns the host's current RHPv3 price table
func (h *Host) RHPv3PriceTable() (crhpv3.HostPriceTable, error) {
	return h.rhpv3.PriceTable()
}

// WalletAddress returns the host's wallet address
func (h *Host) WalletAddress() types.Address {
	return h.wallet.Address()
}

// Contracts returns the host's contract manager
func (h *Host) Contracts() *contracts.ContractManager {
	return h.contracts
}

// NewHost initializes a new test host
func NewHost(privKey types.PrivateKey, dir string) (*Host, error) {
	node, err := NewNode(privKey, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	opt := zap.NewDevelopmentConfig()
	opt.OutputPaths = []string{filepath.Join(dir, "hostd.log")}
	log, err := opt.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}

	wallet, err := wallet.NewSingleAddressWallet(privKey, node.cm, node.tp, db, log.Named("wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	storage, err := storage.NewVolumeManager(db, node.cm, log.Named("storage"))
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	storage.AddVolume(filepath.Join(dir, "storage"), 64)
	contracts, err := contracts.NewManager(db, storage, node.cm, node.tp, wallet, log.Named("contracts"))
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

	_, rhp2Port, err := net.SplitHostPort(rhp2Listener.Addr().String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse rhp2 addr: %w", err)
	}
	discoveredAddr := net.JoinHostPort(rhp2Listener.Addr().String(), rhp2Port)

	settings, err := settings.NewConfigManager(privKey, discoveredAddr, db, node.cm, node.tp, wallet, log.Named("settings"))
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	registry := registry.NewManager(privKey, db)
	accounts := accounts.NewManager(db, settings)

	rhpv2, err := rhpv2.NewSessionHandler(rhp2Listener, privKey, rhp3Listener.Addr().String(), node.cm, node.tp, wallet, contracts, settings, storage, stubMetricReporter{}, log.Named("rhpv2"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv2 session handler: %w", err)
	}
	go rhpv2.Serve()

	rhpv3, err := rhpv3.NewSessionHandler(rhp3Listener, privKey, node.cm, node.tp, wallet, accounts, contracts, registry, storage, settings, stubMetricReporter{}, log.Named("rhpv3"))
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv3 session handler: %w", err)
	}
	go rhpv3.Serve()

	return &Host{
		Node:      node,
		store:     db,
		wallet:    wallet,
		settings:  settings,
		storage:   storage,
		registry:  registry,
		accounts:  accounts,
		contracts: contracts,

		rhpv2: rhpv2,
		rhpv3: rhpv3,
	}, nil
}
