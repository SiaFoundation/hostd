//go:build !testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_WALLET_SEED"

	defaultAPIAddr     = "localhost:9980"
	defaultGatewayAddr = ":9981"
	defaultRHPv2Addr   = ":9982"
	defaultRHPv3Addr   = ":9983"
)
