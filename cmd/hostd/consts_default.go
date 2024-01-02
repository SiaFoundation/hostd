//go:build !testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_SEED"
	// logPathEnvVariable overrides the path of the log file.
	// Deprecated: use logFileEnvVar instead.
	logPathEnvVariable = "HOSTD_LOG_PATH"
	// logFileEnvVariable overrides the location of the log file.
	logFileEnvVariable    = "HOSTD_LOG_FILE"
	configPathEnvVariable = "HOSTD_CONFIG_FILE"

	defaultAPIAddr        = "localhost:9980"
	defaultGatewayAddr    = ":9981"
	defaultRHP2Addr       = ":9982"
	defaultRHP3TCPAddr    = ":9983"
	defaultRHP3WSAddr     = ":9984"
	defaultPrometheusAddr = ":9985"
)
