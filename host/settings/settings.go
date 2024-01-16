package settings

import (
	"bytes"
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
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/siad/modules"
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
		UpdateLastAnnouncement(Announcement) error
		RevertLastAnnouncement() error

		LastSettingsConsensusChange() (modules.ConsensusChangeID, uint64, error)
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

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		RecommendedFee() types.Currency
	}

	// Alerts registers global alerts.
	Alerts interface {
		Register(alerts.Alert)
	}

	// A ChainManager manages the current consensus state
	ChainManager interface {
		TipState() consensus.State
		Subscribe(s modules.ConsensusSetSubscriber, ccID modules.ConsensusChangeID, cancel <-chan struct{}) error
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		FundTransaction(txn *types.Transaction, amount types.Currency) ([]types.Hash256, func(), error)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
	}

	// A ConfigManager manages the host's current configuration
	ConfigManager struct {
		dir               string
		hostKey           types.PrivateKey
		discoveredRHPAddr string

		store Store
		a     Alerts
		log   *zap.Logger

		cm     ChainManager
		tp     TransactionPool
		wallet Wallet

		mu                  sync.Mutex // guards the following fields
		settings            Settings   // in-memory cache of the host's settings
		scanHeight          uint64     // track the last block height that was scanned for announcements
		lastAnnounceAttempt uint64     // debounce announcement transactions

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

	specifierAnnouncement = types.NewSpecifier("HostAnnouncement")
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
	if strings.TrimSpace(s.NetAddress) != "" {
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

// DiscoveredRHP2Address returns the rhp2 address that was discovered by the gateway
func (m *ConfigManager) DiscoveredRHP2Address() string {
	return m.discoveredRHPAddr
}

func createAnnouncement(priv types.PrivateKey, netaddress string) []byte {
	// encode the announcement
	var buf bytes.Buffer
	pub := priv.PublicKey()
	enc := types.NewEncoder(&buf)
	specifierAnnouncement.EncodeTo(enc)
	enc.WriteString(netaddress)
	pub.UnlockKey().EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		panic(err)
	}
	// hash without the signature
	sigHash := types.HashBytes(buf.Bytes())
	// sign
	sig := priv.SignHash(sigHash)
	sig.EncodeTo(enc)
	if err := enc.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// NewConfigManager initializes a new config manager
func NewConfigManager(dir string, hostKey types.PrivateKey, rhp2Addr string, store Store, cm ChainManager, tp TransactionPool, w Wallet, a Alerts, log *zap.Logger) (*ConfigManager, error) {
	m := &ConfigManager{
		dir:               dir,
		hostKey:           hostKey,
		discoveredRHPAddr: rhp2Addr,

		store:  store,
		a:      a,
		log:    log,
		cm:     cm,
		tp:     tp,
		wallet: w,
		tg:     threadgroup.New(),

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),

		// rhp3 WebSocket TLS
		rhp3WSTLS: &tls.Config{},
	}

	if err := m.reloadCertificates(); err != nil {
		return nil, fmt.Errorf("failed to load rhp3 WebSocket certificates: %w", err)
	}

	settings, err := m.store.Settings()
	if errors.Is(err, ErrNoSettings) {
		if err := store.UpdateSettings(DefaultSettings); err != nil {
			return nil, fmt.Errorf("failed to initialize settings: %w", err)
		}
		settings = DefaultSettings // use the default settings
	} else if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	lastChange, height, err := m.store.LastSettingsConsensusChange()
	if err != nil {
		return nil, fmt.Errorf("failed to load last settings consensus change: %w", err)
	}
	m.scanHeight = height

	go func() {
		// subscribe to consensus changes
		err := cm.Subscribe(m, lastChange, m.tg.Done())
		if errors.Is(err, chain.ErrInvalidChangeID) {
			m.log.Warn("rescanning blockchain due to unknown consensus change ID")
			// reset change ID and subscribe again
			if err := store.RevertLastAnnouncement(); err != nil {
				m.log.Fatal("failed to reset wallet", zap.Error(err))
			} else if err = cm.Subscribe(m, modules.ConsensusChangeBeginning, m.tg.Done()); err != nil {
				m.log.Fatal("failed to reset consensus change subscription", zap.Error(err))
			}
		} else if err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
			m.log.Fatal("failed to subscribe to consensus changes", zap.Error(err))
		}
	}()

	m.settings = settings
	// update the global rate limiters from settings
	m.setRateLimit(settings.IngressLimit, settings.EgressLimit)
	// initialize the DDNS update timer
	m.resetDDNS()
	return m, nil
}
