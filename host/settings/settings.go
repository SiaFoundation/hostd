package settings

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	announcementTxnSize = 1000

	blocksPerMonth = 144 * 30 // 144 blocks per day * 30 days

	// defaultBurstSize allow for large reads and writes on the limiter
	defaultBurstSize = 256 * (1 << 20) // 256 MiB
)

type (
	// A Store persists the host's settings
	Store interface {
		// Settings returns the host's current settings. If the host has no
		// settings yet, ErrNoSettings must be returned.
		Settings() (Settings, error)
		// UpdateSettings updates the host's settings.
		UpdateSettings(s Settings) error
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

		Collateral    types.Currency `json:"collateral"`
		MaxCollateral types.Currency `json:"maxCollateral"`

		MinStoragePrice types.Currency `json:"minStoragePrice"`
		MinEgressPrice  types.Currency `json:"minEgressPrice"`
		MinIngressPrice types.Currency `json:"minIngressPrice"`

		// Bandwidth limiter settings
		IngressLimit uint64 `json:"ingressLimit"`
		EgressLimit  uint64 `json:"egressLimit"`

		// Registry settings
		MaxRegistryEntries uint64 `json:"maxRegistryEntries"`

		// RHP3 settings
		AccountExpiry     time.Duration  `json:"accountExpiry"`
		MaxAccountBalance types.Currency `json:"maxAccountBalance"`

		Revision uint64 `json:"revision"`
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		RecommendedFee() types.Currency
	}

	// A ChainManager manages the current consensus state
	ChainManager interface {
		TipState() consensus.State
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		FundTransaction(txn *types.Transaction, amount types.Currency) ([]types.Hash256, func(), error)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
	}

	// A ConfigManager manages the host's current configuration
	ConfigManager struct {
		hostKey           types.PrivateKey
		discoveredRHPAddr string

		store Store
		log   *zap.Logger

		cm     ChainManager
		tp     TransactionPool
		wallet Wallet

		mu           sync.Mutex // guards the following fields
		settings     Settings   // in-memory cache of the host's settings
		ingressLimit *rate.Limiter
		egressLimit  *rate.Limiter
	}
)

var (
	// DefaultSettings are the default settings for the host
	DefaultSettings = Settings{
		AcceptingContracts:  false,
		NetAddress:          "",
		MaxContractDuration: 6 * blocksPerMonth, // 6 months

		ContractPrice:     types.Siacoins(1).Div64(5),
		BaseRPCPrice:      types.NewCurrency64(100),
		SectorAccessPrice: types.NewCurrency64(100),

		Collateral:    types.Siacoins(100).Div64(1 << 40).Div64(blocksPerMonth), // 100 SC / TB / month
		MaxCollateral: types.Siacoins(1000),

		MinStoragePrice: types.Siacoins(50).Div64(1 << 40).Div64(blocksPerMonth), // 50 SC / TB / month
		MinEgressPrice:  types.Siacoins(250).Div64(1 << 40),                      // 250 SC / TB
		MinIngressPrice: types.Siacoins(10).Div64(1 << 40),                       // 10 SC / TB

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

// Close is a no-op
func (m *ConfigManager) Close() error {
	return nil
}

// Announce announces the host to the network
func (m *ConfigManager) Announce() error {
	// get the current settings
	settings := m.Settings()
	// if no netaddress is set, override the field with the auto-discovered one
	if len(settings.NetAddress) == 0 {
		settings.NetAddress = m.discoveredRHPAddr
	}
	// create a transaction with an announcement
	minerFee := m.tp.RecommendedFee().Mul64(announcementTxnSize)
	txn := types.Transaction{
		ArbitraryData: [][]byte{
			createAnnouncement(m.hostKey, settings.NetAddress),
		},
		MinerFees: []types.Currency{minerFee},
	}

	// fund the transaction
	toSign, release, err := m.wallet.FundTransaction(&txn, minerFee)
	if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()
	// sign the transaction
	err = m.wallet.SignTransaction(m.cm.TipState(), &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}
	// broadcast the transaction
	err = m.tp.AcceptTransactionSet([]types.Transaction{txn})
	if err != nil {
		return fmt.Errorf("failed to broadcast transaction: %w", err)
	}
	m.log.Debug("broadcast announcement", zap.String("transactionID", txn.ID().String()), zap.String("netaddress", settings.NetAddress), zap.String("cost", minerFee.ExactString()))
	return nil
}

// UpdateSettings updates the host's settings.
func (m *ConfigManager) UpdateSettings(s Settings) error {
	m.mu.Lock()
	m.settings = s
	m.setRateLimit(s.IngressLimit, s.EgressLimit)
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
func NewConfigManager(hostKey types.PrivateKey, rhp2Addr string, store Store, cm ChainManager, tp TransactionPool, w Wallet, log *zap.Logger) (*ConfigManager, error) {
	m := &ConfigManager{
		hostKey:           hostKey,
		discoveredRHPAddr: rhp2Addr,

		store:  store,
		log:    log,
		cm:     cm,
		tp:     tp,
		wallet: w,

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),
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

	m.settings = settings
	// update the global rate limiters from settings
	m.setRateLimit(settings.IngressLimit, settings.EgressLimit)
	return m, nil
}
