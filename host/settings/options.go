package settings

import (
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

type Option func(*ConfigManager)

// WithLog sets the logger for the settings manager.
func WithLog(log *zap.Logger) Option {
	return func(cm *ConfigManager) {
		cm.log = log
	}
}

func WithStore(s Store) Option {
	return func(cm *ConfigManager) {
		cm.store = s
	}
}

func WithChainManager(cm ChainManager) Option {
	return func(c *ConfigManager) {
		c.cm = cm
	}
}

func WithTransactionPool(tp TransactionPool) Option {
	return func(c *ConfigManager) {
		c.tp = tp
	}
}

func WithWallet(w Wallet) Option {
	return func(c *ConfigManager) {
		c.wallet = w
	}
}

func WithAlertManager(am Alerts) Option {
	return func(c *ConfigManager) {
		c.a = am
	}
}

func WithHostKey(pk types.PrivateKey) Option {
	return func(c *ConfigManager) {
		c.hostKey = pk
	}
}

func WithRHP2Addr(addr string) Option {
	return func(c *ConfigManager) {
		c.discoveredRHPAddr = addr
	}
}

func WithCertificateFiles(certFilePath, keyFilePath string) Option {
	return func(c *ConfigManager) {
		c.certCertFilePath = certFilePath
		c.certKeyFilePath = keyFilePath
	}
}
