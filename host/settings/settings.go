package settings

import (
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	announcementTxnSize = 1000

	blocksPerMonth = 144 * 30 // 144 blocks per day * 30 days

	// defaultBurstSize allow for large reads and writes on the limiter
	defaultBurstSize = 256 * (1 << 20) // 256 MiB

	dnsUpdateFrequency = 30 * time.Second
)

type (
	// A Store persists the host's settings
	Store interface {
		// Settings returns the host's current settings. If the host has no
		// settings yet, ErrNoSettings must be returned.
		Settings() (Settings, error)
		// UpdateSettings updates the host's settings.
		UpdateSettings(s Settings) error

		LastAnnouncement() (Announcement, error)
	}

	// ChainManager defines the interface required by the contract manager to
	// interact with the consensus set.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State
		BestIndex(height uint64) (types.ChainIndex, bool)
		RecommendedFee() types.Currency

		UnconfirmedParents(txn types.Transaction) []types.Transaction
		AddPoolTransactions([]types.Transaction) (known bool, err error)

		V2TransactionSet(types.ChainIndex, types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
	}

	// A Syncer broadcasts transactions to its peers
	Syncer interface {
		BroadcastTransactionSet([]types.Transaction)
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}

	// Alerts registers global alerts.
	Alerts interface {
		Register(alerts.Alert)
	}

	// Settings contains configuration options for the host.
	Settings struct {
		// Host settings
		AcceptingContracts  bool   `json:"acceptingContracts"`
		NetAddress          string `json:"netAddress"`
		MaxContractDuration uint64 `json:"maxContractDuration"`
		WindowSize          uint64 `json:"windowSize"`

		// Pricing
		ContractPrice     types.Currency `json:"contractPrice"`
		BaseRPCPrice      types.Currency `json:"baseRPCPrice"`
		SectorAccessPrice types.Currency `json:"sectorAccessPrice"`

		CollateralMultiplier float64        `json:"collateralMultiplier"`
		MaxCollateral        types.Currency `json:"maxCollateral"`

		StoragePrice types.Currency `json:"storagePrice"`
		EgressPrice  types.Currency `json:"egressPrice"`
		IngressPrice types.Currency `json:"ingressPrice"`

		PriceTableValidity time.Duration `json:"priceTableValidity"`

		// Registry settings
		MaxRegistryEntries uint64 `json:"maxRegistryEntries"`

		// RHP3 settings
		AccountExpiry     time.Duration  `json:"accountExpiry"`
		MaxAccountBalance types.Currency `json:"maxAccountBalance"`

		// Bandwidth limiter settings
		IngressLimit uint64 `json:"ingressLimit"`
		EgressLimit  uint64 `json:"egressLimit"`

		// DNS settings
		DDNS DNSSettings `json:"ddns"`

		SectorCacheSize uint32 `json:"sectorCacheSize"`

		Revision uint64 `json:"revision"`
	}

	// A ConfigManager manages the host's current configuration
	ConfigManager struct {
		hostKey            types.PrivateKey
		announceInterval   uint64
		validateNetAddress bool
		initialSettings    Settings

		store Store
		a     Alerts
		log   *zap.Logger

		chain  ChainManager
		syncer Syncer
		wallet Wallet

		mu         sync.Mutex // guards the following fields
		settings   Settings   // in-memory cache of the host's settings
		scanHeight uint64     // track the last block height that was scanned for announcements

		ingressLimit *rate.Limiter
		egressLimit  *rate.Limiter

		ddnsUpdateTimer *time.Timer
		lastIPv4        net.IP
		lastIPv6        net.IP

		rhp3WSTLS *tls.Config

		tg *threadgroup.ThreadGroup
	}
)

var (
	// DefaultSettings are the default settings for the host
	DefaultSettings = Settings{
		AcceptingContracts:  false,
		NetAddress:          "",
		MaxContractDuration: 6 * blocksPerMonth, // 6 months

		ContractPrice:     types.Siacoins(1).Div64(5),   // 200 ms / contract
		BaseRPCPrice:      types.Siacoins(1).Div64(1e6), // 1 SC / million RPCs
		SectorAccessPrice: types.Siacoins(1).Div64(1e6), // 1 SC / million sectors

		CollateralMultiplier: 2.0, // 2x storage price
		MaxCollateral:        types.Siacoins(1000),

		StoragePrice: types.Siacoins(150).Div64(1 << 40).Div64(blocksPerMonth), // 150 SC / TB / month
		EgressPrice:  types.Siacoins(500).Div64(1 << 40),                       // 500 SC / TB
		IngressPrice: types.Siacoins(10).Div64(1 << 40),                        // 10 SC / TB

		PriceTableValidity: 30 * time.Minute,

		AccountExpiry:     30 * 24 * time.Hour, // 30 days
		MaxAccountBalance: types.Siacoins(10),  // 10SC
		WindowSize:        144,                 // 144 blocks

		MaxRegistryEntries: 100000,
	}
	// ErrNoSettings must be returned by the store if the host has no settings yet
	ErrNoSettings = errors.New("no settings found")
)

// setRateLimit sets the bandwidth rate limit for the host
func (m *ConfigManager) setRateLimit(ingress, egress uint64) {
	var ingressLimit rate.Limit
	if ingress == 0 {
		ingressLimit = rate.Inf
	} else {
		ingressLimit = rate.Limit(ingress)
	}

	var egressLimit rate.Limit
	if egress == 0 {
		egressLimit = rate.Inf
	} else {
		egressLimit = rate.Limit(egress)
	}

	m.ingressLimit.SetLimit(rate.Limit(ingressLimit))
	m.egressLimit.SetLimit(rate.Limit(egressLimit))
}

// Close closes the config manager
func (m *ConfigManager) Close() error {
	m.tg.Stop()
	return nil
}

// ScanHeight returns the last block height that was scanned for announcements
func (m *ConfigManager) ScanHeight() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scanHeight
}

// LastAnnouncement returns the last announcement that was made by the host
func (m *ConfigManager) LastAnnouncement() (Announcement, error) {
	return m.store.LastAnnouncement()
}

// UpdateSettings updates the host's settings.
func (m *ConfigManager) UpdateSettings(s Settings) error {
	// validate DNS settings
	if err := validateDNSSettings(&s.DDNS); err != nil {
		return fmt.Errorf("failed to validate DNS settings: %w", err)
	}

	// if a netaddress is set, validate it
	if strings.TrimSpace(s.NetAddress) != "" && m.validateNetAddress {
		if err := validateNetAddress(s.NetAddress); err != nil {
			return fmt.Errorf("failed to validate net address: %w", err)
		}
	}

	m.mu.Lock()
	m.settings = s
	m.setRateLimit(s.IngressLimit, s.EgressLimit)
	m.resetDDNS()
	m.mu.Unlock()
	return m.store.UpdateSettings(s)
}

// Settings returns the host's current settings.
func (m *ConfigManager) Settings() Settings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settings
}

// BandwidthLimiters returns the rate limiters for all traffic
func (m *ConfigManager) BandwidthLimiters() (ingress, egress *rate.Limiter) {
	return m.ingressLimit, m.egressLimit
}

// NewConfigManager initializes a new config manager
func NewConfigManager(hostKey types.PrivateKey, store Store, cm ChainManager, s Syncer, wm Wallet, opts ...Option) (*ConfigManager, error) {
	m := &ConfigManager{
		announceInterval:   144 * 90, // 90 days
		validateNetAddress: true,
		hostKey:            hostKey,
		initialSettings:    DefaultSettings,

		store:  store,
		chain:  cm,
		syncer: s,
		wallet: wm,

		log: zap.NewNop(),
		a:   alerts.NewNop(),
		tg:  threadgroup.New(),

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),

		// rhp3 WebSocket TLS
		rhp3WSTLS: &tls.Config{},
	}

	for _, opt := range opts {
		opt(m)
	}

	if len(m.hostKey) != ed25519.PrivateKeySize {
		panic("host key invalid")
	}

	settings, err := m.store.Settings()
	if errors.Is(err, ErrNoSettings) {
		settings = m.initialSettings
	} else if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	m.settings = settings
	// update the global rate limiters from settings
	m.setRateLimit(settings.IngressLimit, settings.EgressLimit)
	// initialize the DDNS update timer
	m.resetDDNS()
	return m, nil
}
