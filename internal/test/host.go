package test

import (
	"crypto/ed25519"
	"fmt"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/store"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/siad/types"
)

type stubMetricReporter struct{}

func (stubMetricReporter) Report(any) (_ error) { return }

// A Host is an ephemeral host that can be used for testing.
type Host struct {
	node

	settings  *settings.ConfigManager
	storage   rhpv3.StorageManager
	registry  *registry.Manager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager

	rhpv2 *rhpv2.SessionHandler
	rhpv3 *rhpv3.SessionHandler
}

// DefaultSettings returns the default settings for the test host
var DefaultSettings = settings.Settings{
	AcceptingContracts:  true,
	MaxContractDuration: uint64(types.BlocksPerMonth) * 3,
	MaxCollateral:       types.SiacoinPrecision.Mul64(5000),

	ContractPrice: types.SiacoinPrecision.Div64(4),

	BaseRPCPrice:      types.NewCurrency64(100),
	SectorAccessPrice: types.NewCurrency64(100),

	Collateral:      types.SiacoinPrecision.Mul64(200).Div64(1e12).Div64(uint64(types.BlocksPerMonth)),
	MinStoragePrice: types.SiacoinPrecision.Mul64(100).Div64(1e12).Div64(uint64(types.BlocksPerMonth)),
	MinEgressPrice:  types.SiacoinPrecision.Mul64(100).Div64(1e12),
	MinIngressPrice: types.SiacoinPrecision.Mul64(100).Div64(1e12),

	MaxAccountBalance: types.SiacoinPrecision.Mul64(10),
}

// Close shutsdown the host
func (h *Host) Close() error {
	h.rhpv3.Close()
	h.rhpv2.Close()
	h.node.Close()
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

// UpdateSettings updates the host's configuration
func (h *Host) UpdateSettings(settings settings.Settings) error {
	return h.settings.UpdateSettings(settings)
}

// RHPv2Settings returns the host's current RHPv2 settings
func (h *Host) RHPv2Settings() (rhpv2.HostSettings, error) {
	return h.rhpv2.Settings()
}

// RHPv3PriceTable returns the host's current RHPv3 price table
func (h *Host) RHPv3PriceTable() (rhpv3.PriceTable, error) {
	return h.rhpv3.PriceTable()
}

// NewEphemeralHost initializes a new test host
func NewEphemeralHost(privKey ed25519.PrivateKey, dir string) (*Host, error) {
	node, err := newNode(privKey, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	storage := store.NewEphemeralStorageManager()
	contracts := contracts.NewManager(store.NewEphemeralContractStore(), storage, node.cs, node.tp, node.w)
	settings, err := settings.NewConfigManager(store.NewEphemeralSettingsStore())
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	registry := registry.NewManager(privKey, store.NewEphemeralRegistryStore(1000))
	accounts := accounts.NewManager(store.NewEphemeralAccountStore())

	rhpv2, err := rhpv2.NewSessionHandler(privKey, "localhost:0", node.cs, node.tp, node.w, contracts, settings, storage, stubMetricReporter{})
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv2 session handler: %w", err)
	}
	go rhpv2.Serve()
	rhpv3, err := rhpv3.NewSessionHandler(privKey, "localhost:0", node.cs, node.tp, node.w, accounts, contracts, registry, storage, settings, stubMetricReporter{})
	if err != nil {
		return nil, fmt.Errorf("failed to create rhpv3 session handler: %w", err)
	}
	go rhpv3.Serve()
	return &Host{
		node:      *node,
		settings:  settings,
		storage:   storage,
		registry:  registry,
		accounts:  accounts,
		contracts: contracts,

		rhpv2: rhpv2,
		rhpv3: rhpv3,
	}, nil
}
