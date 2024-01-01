//go:build testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_ZEN_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_ZEN_SEED"
	// logPathEnvVariable overrides the path of the log file.
	// Deprecated: use logFileEnvVar instead.
	logPathEnvVariable = "HOSTD_ZEN_LOG_PATH"
	// logFileEnvVariable overrides the location of the log file.
	logFileEnvVariable    = "HOSTD_ZEN_LOG_FILE"
	configPathEnvVariable = "HOSTD_ZEN_CONFIG_FILE"

	defaultAPIAddr        = ":9880"
	defaultGatewayAddr    = ":9881"
	defaultRHP2Addr       = ":9882"
	defaultRHP3TCPAddr    = ":9883"
	defaultRHP3WSAddr     = ":9884"
	defaultPrometheusAddr = ":9885"
)
