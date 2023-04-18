//go:build testnet

package main

const (
	apiPasswordEnvVariable = "HOSTD_ZEN_API_PASSWORD"
	walletSeedEnvVariable  = "HOSTD_ZEN_WALLET_SEED"

	defaultAPIAddr      = "localhost:9880"
	defaultGatewayAddr  = ":9881"
	defaultRHPv2Addr    = ":9882"
	defaultRHPv3TCPAddr = ":9883"
	defaultRHPv3WSAddr  = ":9884"
)
