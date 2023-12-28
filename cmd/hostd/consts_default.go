//go:build !testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_SEED"
	logPathEnvVariable     = "HOSTD_LOG_PATH"
	configPathEnvVariable  = "HOSTD_CONFIG_FILE"

	defaultAPIAddr        = ":9980"
	defaultGatewayAddr    = ":9981"
	defaultRHP2Addr       = ":9982"
	defaultRHP3TCPAddr    = ":9983"
	defaultRHP3WSAddr     = ":9984"
	defaultPrometheusAddr = ":9985"
)
