package settings

import (
	"errors"
	"fmt"
	"time"

	"go.sia.tech/siad/types"
	"golang.org/x/time/rate"
)

const (
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

		// RHP3 settings
		AccountExpiry     time.Duration  `json:"accountExpiry"`
		MaxAccountBalance types.Currency `json:"maxAccountBalance"`

		Revision uint64 `json:"revision"`
	}

	// A ConfigManager manages the host's current configuration
	ConfigManager struct {
		settings Store

		ingressLimit *rate.Limiter
		egressLimit  *rate.Limiter
	}
)

var (
	defaultSettings = Settings{
		AcceptingContracts:  false,
		NetAddress:          "",
		MaxContractDuration: 6 * blocksPerMonth, // 6 months

		ContractPrice:     types.SiacoinPrecision.Div64(5),
		BaseRPCPrice:      types.NewCurrency64(1),
		SectorAccessPrice: types.NewCurrency64(1),

		Collateral:    types.SiacoinPrecision.Mul64(100).Div64(1 << 40).Div64(blocksPerMonth), // 100 SC / TB / month
		MaxCollateral: types.SiacoinPrecision.Mul64(1000),

		MinStoragePrice: types.SiacoinPrecision.Mul64(50).Div64(1 << 40).Div64(blocksPerMonth), // 50 SC / TB / month
		MinEgressPrice:  types.SiacoinPrecision.Mul64(250).Div64(1 << 40),                      // 250 SC / TB
		MinIngressPrice: types.SiacoinPrecision.Mul64(10).Div64(1 << 40),                       // 10 SC / TB

		AccountExpiry:     144 * 30,                         // 30 days
		MaxAccountBalance: types.SiacoinPrecision.Mul64(10), // 10SC
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

// Close is a no-op
func (m *ConfigManager) Close() error {
	return nil
}

// Announce announces the host to the network
func (m *ConfigManager) Announce(netaddress string) error {
	panic("not implemented")
}

// UpdateSettings updates the host's settings.
func (m *ConfigManager) UpdateSettings(s Settings) error {
	m.setRateLimit(s.IngressLimit, s.EgressLimit)
	return m.settings.UpdateSettings(s)
}

// Settings returns the host's current settings.
func (m *ConfigManager) Settings() (Settings, error) {
	return m.settings.Settings()
}

// BandwidthLimiters returns the rate limiters for all traffic
func (m *ConfigManager) BandwidthLimiters() (ingress, egress *rate.Limiter) {
	return m.ingressLimit, m.egressLimit
}

// NewConfigManager initializes a new config manager
func NewConfigManager(settingsStore Store) (*ConfigManager, error) {
	m := &ConfigManager{
		settings: settingsStore,

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),
	}

	settings, err := m.settings.Settings()
	if err != nil && errors.Is(err, ErrNoSettings) {
		if err := settingsStore.UpdateSettings(defaultSettings); err != nil {
			return nil, fmt.Errorf("failed to initialize settings: %w", err)
		}
		return m, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// update the global rate limiters from settings
	m.setRateLimit(settings.IngressLimit, settings.EgressLimit)
	return m, nil
}
