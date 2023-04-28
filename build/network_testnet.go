//go:build testnet

package build

import (
	"go.sia.tech/core/chain"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// Network returns the Sia network consts and genesis block for the current build.
func Network() (*consensus.Network, types.Block) {
	return chain.TestnetZen()
}
