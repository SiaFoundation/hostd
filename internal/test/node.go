package test

import (
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"time"

	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

type (
	// A node is a base Sia node that can be extended by a Renter or Host
	node struct {
		privKey ed25519.PrivateKey

		g  modules.Gateway
		cs modules.ConsensusSet
		tp modules.TransactionPool
		m  *Miner
	}
)

func (n *node) Close() error {
	n.tp.Close()
	n.cs.Close()
	n.g.Close()
	return nil
}

func (n *node) PublicKey() ed25519.PublicKey {
	return n.privKey.Public().(ed25519.PublicKey)
}

// GatewayAddr returns the address of the gateway
func (n *node) GatewayAddr() string {
	return string(n.g.Address())
}

// ConnectPeer connects the host's gateway to a peer
func (n *node) ConnectPeer(addr string) error {
	return n.g.Connect(modules.NetAddress(addr))
}

// MineBlocks mines n blocks sending the reward to address
func (n *node) MineBlocks(address types.UnlockHash, count int) error {
	return n.m.Mine(address, count)
}

// newNode creates a new Sia node and wallet with the given key
func newNode(privKey ed25519.PrivateKey, dir string) (*node, error) {
	g, err := gateway.New("localhost:0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	cs, errCh := mconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}

	tp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	m := NewMiner(cs)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe miner to consensus set: %w", err)
	}
	tp.TransactionPoolSubscribe(m)
	return &node{
		privKey: privKey,

		g:  g,
		cs: cs,
		tp: tp,
		m:  m,
	}, nil
}

// NewTestingPair creates a new renter and host pair, connects them to each
// other, and funds both wallets.
func NewTestingPair(dir string) (*Renter, *Host, error) {
	hostKey, renterKey := ed25519.NewKeyFromSeed(frand.Bytes(32)), ed25519.NewKeyFromSeed(frand.Bytes(32))

	// initialize the host
	host, err := NewHost(hostKey, filepath.Join(dir, "host"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create host: %w", err)
	} else if err := host.UpdateSettings(DefaultSettings); err != nil {
		return nil, nil, fmt.Errorf("failed to update host settings: %w", err)
	}

	// initialize the renter
	renter, err := NewRenter(renterKey, filepath.Join(dir, "renter"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create renter: %w", err)
	}

	// connect the renter and host
	if err := host.ConnectPeer(renter.GatewayAddr()); err != nil {
		return nil, nil, fmt.Errorf("failed to connect node gateways: %w", err)
	}

	// mine enough blocks to fund the host's wallet
	if err := host.MineBlocks(host.WalletAddress(), int(types.MaturityDelay)*2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)

	// mine enough blocks to fund the renter's wallet
	if err := renter.MineBlocks(renter.WalletAddress(), int(types.MaturityDelay)*2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)
	return renter, host, nil
}
