package settings

import (
	"go.sia.tech/core/types"
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

// WithStore sets the store for the settings manager.
func WithStore(s Store) Option {
	return func(cm *ConfigManager) {
		cm.store = s
	}
}

// WithChainManager sets the chain manager for the settings manager.
func WithChainManager(cm ChainManager) Option {
	return func(c *ConfigManager) {
		c.cm = cm
	}
}

// WithTransactionPool sets the transaction pool for the settings manager.
func WithTransactionPool(tp TransactionPool) Option {
	return func(c *ConfigManager) {
		c.tp = tp
	}
}

// WithWallet sets the wallet for the settings manager.
func WithWallet(w Wallet) Option {
	return func(c *ConfigManager) {
		c.wallet = w
	}
}

// WithAlertManager sets the alerts manager for the settings manager.
func WithAlertManager(am Alerts) Option {
	return func(c *ConfigManager) {
		c.a = am
	}
}

// WithHostKey sets the host key for the settings manager.
func WithHostKey(pk types.PrivateKey) Option {
	return func(c *ConfigManager) {
		c.hostKey = pk
	}
}

// WithRHP2Addr sets the address of the RHP2 server.
func WithRHP2Addr(addr string) Option {
	return func(c *ConfigManager) {
		c.discoveredRHPAddr = addr
	}
}

// WithCertificateFiles sets the certificate files for the settings manager.
func WithCertificateFiles(certFilePath, keyFilePath string) Option {
	return func(c *ConfigManager) {
		c.certCertFilePath = certFilePath
		c.certKeyFilePath = keyFilePath
	}
}
