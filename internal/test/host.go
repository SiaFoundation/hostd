package test

import (
	"go.sia.tech/hostd/internal/cpuminer"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/host"
)

// A TestHost is an ephemeral host that can be used for testing. The node is
// automatically stopped and cleaned up after the test.
type TestHost struct {
	g  modules.Gateway
	cs modules.ConsensusSet
	w  wallet.SingleAddressWallet
	tp modules.TransactionPool
	m  *cpuminer.Miner

	h *host.Host
}
