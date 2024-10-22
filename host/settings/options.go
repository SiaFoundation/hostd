package settings

import (
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
)

// An Option is a functional option that can be used to configure a config
// manager.
type Option func(*ConfigManager)

// WithLog sets the logger for the settings manager.
func WithLog(log *zap.Logger) Option {
	return func(cm *ConfigManager) {
		cm.log = log
	}
}

// WithAlertManager sets the alerts manager for the settings manager.
func WithAlertManager(am Alerts) Option {
	return func(c *ConfigManager) {
		c.a = am
	}
}

// WithAnnounceInterval sets the interval at which the host should re-announce
// itself.
func WithAnnounceInterval(interval uint64) Option {
	return func(c *ConfigManager) {
		c.announceInterval = interval
	}
}

// WithValidateNetAddress sets whether the settings manager should validate
// the announced net address.
func WithValidateNetAddress(validate bool) Option {
	return func(c *ConfigManager) {
		c.validateNetAddress = validate
	}
}

// WithInitialSettings sets the host's settings when the config manager is
// initialized. If this option is not provided, the default settings are used.
// If the database already contains settings, they will be used.
func WithInitialSettings(settings Settings) Option {
	return func(c *ConfigManager) {
		c.initialSettings = settings
	}
}

// WithRHP4AnnounceAddresses sets the addresses to announce on the blockchain
// for RHP4.
func WithRHP4AnnounceAddresses(addresses []chain.NetAddress) Option {
	return func(c *ConfigManager) {
		c.rhp4AnnounceAddresses = addresses
	}
}
