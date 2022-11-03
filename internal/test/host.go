package test

import (
	"go.sia.tech/hostd/internal/cpuminer"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
)

// A Host is an ephemeral host that can be used for testing. The node is
// automatically stopped and cleaned up after the test.
type Host struct {
	g  modules.Gateway
	cs modules.ConsensusSet
	w  wallet.SingleAddressWallet
	tp modules.TransactionPool
	m  *cpuminer.Miner

	rhpv2 *rhpv2.SessionHandler
	rhpv3 *rhpv3.SessionHandler
}
