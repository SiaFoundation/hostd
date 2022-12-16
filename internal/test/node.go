package test

import (
	"crypto/ed25519"
	"fmt"
	"path/filepath"
	"time"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

type (
	// A node is the base Sia node that is used to by a renter or host
	node struct {
		privKey ed25519.PrivateKey

		g  modules.Gateway
		cm *consensus.ChainManager
		tp modules.TransactionPool
		w  *wallet.SingleAddressWallet
		m  *Miner
	}
)

func (n *node) Close() error {
	n.w.Close()
	n.tp.Close()
	n.cm.Close()
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

// MineBlocks mines n blocks sending the reward to the node's wallet
func (n *node) MineBlocks(count int) error {
	return n.m.Mine(n.w.Address(), count)
}

// CurrentBlock returns the last block in the consensus
func (n *node) TipState() consensus.State {
	return n.cm.Tip()
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

	chainStore := store.NewEphemeralChainManagerStore()
	cm, err := consensus.NewChainManager(cs, chainStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create chain manager: %w", err)
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
	m := NewMiner(cs)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe miner to consensus set: %w", err)
	}
	tp.TransactionPoolSubscribe(m)
	return &node{
		privKey: privKey,

		g:  g,
		cm: cm,
		tp: tp,
		w:  w,
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
	if err := host.MineBlocks(int(types.MaturityDelay) * 2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)

	// mine enough blocks to fund the renter's wallet
	if err := renter.MineBlocks(int(types.MaturityDelay) * 2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)
	return renter, host, nil
}
