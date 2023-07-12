//go:build !testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_SEED"
	logPathEnvVariable     = "HOSTD_LOG_PATH"
	configPathEnvVariable  = "HOSTD_CONFIG_FILE"

	defaultAPIAddr      = "localhost:9980"
	defaultGatewayAddr  = ":9981"
	defaultRHPv2Addr    = ":9982"
	defaultRHPv3TCPAddr = ":9983"
	defaultRHPv3WSAddr  = ":9984"
)
