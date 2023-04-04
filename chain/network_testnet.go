//go:build testnet

package chain

import (
	"go.sia.tech/core/chain"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// network returns the Sia network consts and genesis block for the current build.
func network() (*consensus.Network, types.Block) {
	return chain.TestnetZen()
}
