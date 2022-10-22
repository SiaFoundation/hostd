package settings

import (
	"fmt"
	"time"

	"go.sia.tech/siad/types"
	"golang.org/x/time/rate"
)

// defaultBurstSize allow for large reads and writes on the limiter
const defaultBurstSize = 256 * (1 << 20) // 256 MiB

type (
	// A SettingsStore persists the host's settings
	SettingsStore interface {
		// Settings returns the host's current settings.
		Settings() (Settings, error)
		// UpdateSettings updates the host's settings.
		UpdateSettings(s Settings) error

		Close() error
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
		SiaMuxPort        string         `json:"siaMuxPort"`
		AccountExpiry     time.Duration  `json:"accountExpiry"`
		MaxAccountBalance types.Currency `json:"maxAccountBalance"`

		Revision uint64 `json:"revision"`
	}

	// A ConfigManager manages the host's current configuration
	ConfigManager struct {
		settings SettingsStore

		ingressLimit *rate.Limiter
		egressLimit  *rate.Limiter
	}
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

func (m *ConfigManager) Close() error {
	return m.settings.Close()
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
func NewConfigManager(settingsStore SettingsStore) (*ConfigManager, error) {
	m := &ConfigManager{
		settings: settingsStore,

		// initialize the rate limiters
		ingressLimit: rate.NewLimiter(rate.Inf, defaultBurstSize),
		egressLimit:  rate.NewLimiter(rate.Inf, defaultBurstSize),
	}

	settings, err := m.settings.Settings()
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// update the global rate limiters from settings
	m.setRateLimit(settings.IngressLimit, settings.EgressLimit)
	return m, nil
}
