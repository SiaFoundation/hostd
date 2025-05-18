package settings

import (
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

// WithRHP2Port sets the port that the host is listening for
// RHP2 connections on. This is appended to the host's net address
// and announced on the blockchain.
func WithRHP2Port(port uint16) Option {
	return func(c *ConfigManager) {
		c.rhp2Port = port
	}
}

// WithRHP3Port sets the port that the host is listening for
// RHP3 connections on. This is part of the RHP2 settings.
func WithRHP3Port(port uint16) Option {
	return func(c *ConfigManager) {
		c.rhp3Port = port
	}
}

// WithRHP4Port sets the port that the host is listening for
// RHP4 connections on. This is appended to the host's net address
// and announced on the blockchain.
func WithRHP4Port(port uint16) Option {
	return func(c *ConfigManager) {
		c.rhp4Port = port
	}
}

// WithExplorer sets the explorer for the settings manager.
func WithExplorer(explorer Explorer) Option {
	return func(c *ConfigManager) {
		c.explorer = explorer
	}
}
