package settings

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	proto2 "go.sia.tech/core/rhp/v2"
	proto3 "go.sia.tech/core/rhp/v3"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/build"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	rhp2 "go.sia.tech/hostd/v2/rhp/v2"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"lukechampine.com/frand"
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
		// LastV2AnnouncementHash returns the hash of the last v2 announcement.
		LastV2AnnouncementHash() (types.Hash256, types.ChainIndex, error)
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

		PoolTransactions() []types.Transaction
		V2PoolTransactions() []types.V2Transaction

		V2TransactionSet(types.ChainIndex, types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
	}

	// A Syncer broadcasts transactions to its peers
	Syncer interface {
		BroadcastTransactionSet([]types.Transaction) error
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction) error
	}

	// An Explorer provides external information about the
	// Sia network
	Explorer interface {
		TestConnection(context.Context, explorer.Host) (explorer.TestResult, error)
	}

	// Storage provides information about the host's storage capacity
	Storage interface {
		Usage() (used, total uint64, _ error)
	}

	// Certificates provides TLS certificates for the host
	// to use when serving RHP4 over QUIC.
	Certificates interface {
		GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		Address() types.Address
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) error
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)

		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
	}

	// Alerts registers global alerts.
	Alerts interface {
		Register(alerts.Alert)
		DismissCategory(string)
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

		chain    ChainManager
		syncer   Syncer
		storage  Storage
		wallet   Wallet
		explorer Explorer
		certs    Certificates

		mu         sync.Mutex // guards the following fields
		settings   Settings   // in-memory cache of the host's settings
		scanHeight uint64     // track the last block height that was scanned for announcements

		ingressLimit *rate.Limiter
		egressLimit  *rate.Limiter

		ddnsUpdateTimer *time.Timer
		lastIPv4        net.IP
		lastIPv6        net.IP

		rhp2Port uint16
		rhp3Port uint16
		rhp4Port uint16

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

		MaxRegistryEntries: 0,
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
		if err := validateHostname(s.NetAddress); err != nil {
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

// RHPBandwidthLimiters returns the rate limiters for all RHP traffic
func (m *ConfigManager) RHPBandwidthLimiters() (ingress, egress *rate.Limiter) {
	return m.ingressLimit, m.egressLimit
}

// AcceptingContracts returns true if the host is currently accepting contracts
func (m *ConfigManager) AcceptingContracts() bool {
	s := m.Settings()
	return s.AcceptingContracts
}

// RHP2Settings returns the host's current RHP2 settings
func (m *ConfigManager) RHP2Settings() (proto2.HostSettings, error) {
	usedSectors, totalSectors, err := m.storage.Usage()
	if err != nil {
		return proto2.HostSettings{}, fmt.Errorf("failed to get storage usage: %w", err)
	}
	settings := m.Settings()

	return proto2.HostSettings{
		// build info
		Release: "hostd " + build.Version(),
		// protocol version
		Version: rhp2.Version,

		// host info
		Address:          m.wallet.Address(),
		SiaMuxPort:       strconv.FormatUint(uint64(m.rhp3Port), 10),
		NetAddress:       m.rhp2NetAddress(),
		TotalStorage:     totalSectors * proto2.SectorSize,
		RemainingStorage: (totalSectors - usedSectors) * proto2.SectorSize,

		// network defaults
		MaxDownloadBatchSize: rhp2.DefaultBatchSize,
		MaxReviseBatchSize:   rhp2.DefaultBatchSize,
		SectorSize:           proto2.SectorSize,
		WindowSize:           settings.WindowSize,

		// contract formation
		AcceptingContracts: settings.AcceptingContracts,
		MaxDuration:        settings.MaxContractDuration,
		ContractPrice:      settings.ContractPrice,

		// rpc prices
		BaseRPCPrice:           settings.BaseRPCPrice,
		SectorAccessPrice:      settings.SectorAccessPrice,
		Collateral:             settings.StoragePrice.Mul64(uint64(settings.CollateralMultiplier * 1000)).Div64(1000),
		MaxCollateral:          settings.MaxCollateral,
		StoragePrice:           settings.StoragePrice,
		DownloadBandwidthPrice: settings.EgressPrice,
		UploadBandwidthPrice:   settings.IngressPrice,

		// ea settings
		MaxEphemeralAccountBalance: settings.MaxAccountBalance,
		EphemeralAccountExpiry:     settings.AccountExpiry,

		RevisionNumber: settings.Revision,
	}, nil
}

// RHP3PriceTable returns the host's current RHP3 price table
func (m *ConfigManager) RHP3PriceTable() (proto3.HostPriceTable, error) {
	settings := m.Settings()

	fee := m.chain.RecommendedFee()
	currentHeight := m.chain.TipState().Index.Height
	oneHasting := types.NewCurrency64(1)

	return proto3.HostPriceTable{
		UID:             frand.Entropy128(),
		HostBlockHeight: currentHeight,
		Validity:        settings.PriceTableValidity,

		// ephemeral account costs
		AccountBalanceCost:   oneHasting,
		FundAccountCost:      oneHasting,
		UpdatePriceTableCost: oneHasting,

		// MDM costs
		HasSectorBaseCost:   oneHasting,
		MemoryTimeCost:      oneHasting,
		DropSectorsBaseCost: oneHasting,
		DropSectorsUnitCost: oneHasting,
		SwapSectorBaseCost:  oneHasting,

		ReadBaseCost:    settings.SectorAccessPrice,
		ReadLengthCost:  oneHasting,
		WriteBaseCost:   settings.SectorAccessPrice,
		WriteLengthCost: oneHasting,
		WriteStoreCost:  settings.StoragePrice,
		InitBaseCost:    settings.BaseRPCPrice,

		// bandwidth costs
		DownloadBandwidthCost: settings.EgressPrice,
		UploadBandwidthCost:   settings.IngressPrice,

		// LatestRevisionCost is set to a reasonable base + the estimated
		// bandwidth cost of downloading a filecontract. This isn't perfect but
		// at least scales a bit as the host updates their download bandwidth
		// prices.
		LatestRevisionCost: settings.BaseRPCPrice.Add(settings.EgressPrice.Mul64(2048)),

		// Contract Formation/Renewal related fields
		ContractPrice:     settings.ContractPrice,
		CollateralCost:    settings.StoragePrice.Mul64(uint64(settings.CollateralMultiplier * 1000)).Div64(1000),
		MaxCollateral:     settings.MaxCollateral,
		MaxDuration:       settings.MaxContractDuration,
		WindowSize:        settings.WindowSize,
		RenewContractCost: types.Siacoins(100).Div64(1e9),

		// Registry related fields.
		RegistryEntriesLeft:  0,
		RegistryEntriesTotal: 0,

		// Subscription related fields.
		SubscriptionMemoryCost:       oneHasting,
		SubscriptionNotificationCost: oneHasting,

		// TxnFee related fields.
		TxnFeeMinRecommended: fee.Div64(3),
		TxnFeeMaxRecommended: fee,
	}, nil
}

// RHP4Settings returns the host's settings in the RHP4 format. The settings
// are not signed.
func (m *ConfigManager) RHP4Settings() proto4.HostSettings {
	m.mu.Lock()
	settings := m.settings
	m.mu.Unlock()

	used, total, err := m.storage.Usage()
	if err != nil {
		m.log.Error("failed to get storage usage", zap.Error(err))
		return proto4.HostSettings{}
	}

	hs := proto4.HostSettings{
		Release:             "hostd " + build.Version(),
		WalletAddress:       m.wallet.Address(),
		AcceptingContracts:  settings.AcceptingContracts,
		MaxCollateral:       settings.MaxCollateral,
		MaxContractDuration: settings.MaxContractDuration,
		RemainingStorage:    total - used,
		TotalStorage:        total,
		Prices: proto4.HostPrices{
			ContractPrice:   settings.ContractPrice,
			StoragePrice:    settings.StoragePrice,
			Collateral:      settings.StoragePrice.Mul64(uint64(settings.CollateralMultiplier * 1000)).Div64(1000),
			IngressPrice:    settings.IngressPrice,
			EgressPrice:     settings.EgressPrice,
			FreeSectorPrice: types.Siacoins(1).Div64((1 << 40) / proto4.SectorSize), // 1 SC / TB
		},
	}
	return hs
}

// NewConfigManager initializes a new config manager
func NewConfigManager(hostKey types.PrivateKey, store Store, cm ChainManager, s Syncer, sm Storage, wm Wallet, opts ...Option) (*ConfigManager, error) {
	m := &ConfigManager{
		announceInterval:   144 * 90, // 90 days
		validateNetAddress: true,
		hostKey:            hostKey,
		initialSettings:    DefaultSettings,

		store:   store,
		chain:   cm,
		syncer:  s,
		storage: sm,
		wallet:  wm,

		log: zap.NewNop(),
		a:   alerts.NewNop(),
		tg:  threadgroup.New(),

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),

		rhp2Port: 9982,
		rhp3Port: 9983,
		rhp4Port: 9984,
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

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to add context to threadgroup: %w", err)
	}
	go func() {
		defer cancel()

		var consecutiveFailures int
		nextTestTime := 15 * time.Second // short initial delay
		for {
			select {
			case <-time.After(nextTestTime):
				if !m.AcceptingContracts() {
					nextTestTime = time.Minute
					continue
				}

				result, ok, err := m.TestConnection(context.Background())
				if err != nil {
					m.log.Error("failed to test connection", zap.Error(err))
				} else {
					m.log.Debug("connection test result", zap.Bool("ok", ok), zap.Any("result", result))
				}

				if !ok {
					consecutiveFailures++
					nextTestTime = min(2*time.Hour, time.Minute*time.Duration(math.Pow(2, float64(consecutiveFailures))))
				} else {
					consecutiveFailures = 0
					nextTestTime = 2 * time.Hour
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return m, nil
}
