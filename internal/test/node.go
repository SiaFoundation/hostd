package test

import (
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"time"

	"go.sia.tech/hostd/internal/cpuminer"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// A node is the base Sia node that is used to by a renter or host
type node struct {
	g  modules.Gateway
	cs modules.ConsensusSet
	tp modules.TransactionPool
	w  *wallet.SingleAddressWallet
	m  *cpuminer.Miner
}

func (n *node) Close() error {
	n.w.Close()
	n.tp.Close()
	n.cs.Close()
	n.g.Close()
	return nil
}

// GatewayAddr returns the address of the gateway
func (n *node) GatewayAddr() string {
	return string(n.g.Address())
}

// ConnectPeer connects the host's gateway to a peer
func (n *node) ConnectPeer(addr string) error {
	return n.g.Connect(modules.NetAddress(addr))
}

// MineBlocks mines n blocks sending the reward to the node's wallet
func (n *node) MineBlocks(count int) error {
	return n.m.Mine(n.w.Address(), count)
}

func (n *node) CurrentBlock() types.Block {
	return n.cs.CurrentBlock()
}

func newNode(privKey ed25519.PrivateKey, dir string) (*node, error) {
	g, err := gateway.New("localhost:0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}
	wstore := store.NewEphemeralWalletStore(wallet.StandardAddress(privKey.Public().(ed25519.PublicKey)))
	w := wallet.NewSingleAddressWallet(privKey, wstore)
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe wallet to consensus set: %w", err)
	}
	tp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	m := cpuminer.NewMiner(cs)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe miner to consensus set: %w", err)
	}
	tp.TransactionPoolSubscribe(m)
	return &node{
		g:  g,
		cs: cs,
		tp: tp,
		w:  w,
		m:  m,
	}, nil
}

func NewTestingPair(dir string) (*Renter, *Host, error) {
	hostKey, renterKey := ed25519.NewKeyFromSeed(frand.Bytes(32)), ed25519.NewKeyFromSeed(frand.Bytes(32))

	// initialize the host
	host, err := NewEphemeralHost(hostKey, filepath.Join(dir, "host"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create host: %w", err)
	} else if err := host.UpdateSettings(DefaultSettings); err != nil {
		return nil, nil, fmt.Errorf("failed to update host settings: %w", err)
	}

	// initialize the renter
	renter, err := NewEphemeralRenter(renterKey, filepath.Join(dir, "renter"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create renter: %w", err)
	}

	// connect the renter and host
	if err := host.ConnectPeer(renter.GatewayAddr()); err != nil {
		return nil, nil, fmt.Errorf("failed to connect node gateways: %w", err)
	}

	// mine enough blocks to fund the host's wallet
	if err := host.MineBlocks(10); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}

	// small sleep for synchronization
	time.Sleep(time.Millisecond * 100)

	// mine enough blocks to fund the renter's wallet
	if err := renter.MineBlocks(10); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}

	// small sleep for synchronization
	time.Sleep(time.Millisecond * 100)
	return renter, host, nil
}
