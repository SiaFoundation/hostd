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
