//go:build testing

package chain

import (
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// network returns the Sia network consts and genesis block for the current build.
func network() (*consensus.Network, types.Block) {
	n := &consensus.Network{
		InitialCoinbase: types.Siacoins(300000),
		MinimumCoinbase: types.Siacoins(299990),
		InitialTarget:   types.BlockID{4: 32},
	}

	n.HardforkDevAddr.Height = 3
	n.HardforkDevAddr.OldAddress = types.Address{}
	n.HardforkDevAddr.NewAddress = types.Address{}

	n.HardforkTax.Height = 10

	n.HardforkStorageProof.Height = 10

	n.HardforkOak.Height = 20
	n.HardforkOak.FixHeight = 23
	n.HardforkOak.GenesisTimestamp = time.Now().Add(-1e6 * time.Second)

	n.HardforkASIC.Height = 5
	n.HardforkASIC.OakTime = 10000 * time.Second
	n.HardforkASIC.OakTarget = types.BlockID{255, 255}

	n.HardforkFoundation.Height = 50
	n.HardforkFoundation.PrimaryAddress = types.GeneratePrivateKey().PublicKey().StandardAddress()
	n.HardforkFoundation.FailsafeAddress = types.GeneratePrivateKey().PublicKey().StandardAddress()
	return n, types.Block{
		Transactions: []types.Transaction{
			{
				SiafundOutputs: []types.SiafundOutput{
					{Value: 2000, Address: types.Address{214, 166, 197, 164, 29, 201, 53, 236, 106, 239, 10, 158, 127, 131, 20, 138, 63, 221, 230, 16, 98, 247, 32, 77, 210, 68, 116, 12, 241, 89, 27, 223}},
					{Value: 7000, Address: types.Address{209, 246, 228, 60, 248, 78, 242, 110, 9, 8, 227, 248, 225, 216, 163, 52, 142, 93, 47, 176, 103, 41, 137, 80, 212, 8, 132, 58, 241, 189, 2, 17}},
					{Value: 1000, Address: types.VoidAddress},
				},
			},
		},
	}
}
