package test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

type (
	// A Node is a base Sia node that can be extended by a Renter or Host
	Node struct {
		g  modules.Gateway
		cs modules.ConsensusSet
		cm *chain.Manager
		tp *chain.TransactionPool
		m  *Miner
	}
)

// Close closes the node
func (n *Node) Close() error {
	n.tp.Close()
	n.cs.Close()
	n.g.Close()
	return nil
}

// GatewayAddr returns the address of the gateway
func (n *Node) GatewayAddr() string {
	return string(n.g.Address())
}

// ConnectPeer connects the host's gateway to a peer
func (n *Node) ConnectPeer(addr string) error {
	return n.g.Connect(modules.NetAddress(addr))
}

// TipState returns the current consensus state.
func (n *Node) TipState() consensus.State {
	return n.cm.TipState()
}

// MineBlocks mines n blocks sending the reward to address
func (n *Node) MineBlocks(address types.Address, count int) error {
	return n.m.Mine(address, count)
}

// ChainManager returns the chain manager
func (n *Node) ChainManager() *chain.Manager {
	return n.cm
}

// TPool returns the transaction pool
func (n *Node) TPool() *chain.TransactionPool {
	return n.tp
}

// NewNode creates a new Sia node and wallet with the given key
func NewNode(dir string) (*Node, error) {
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
	ctp := chain.NewTPool(tp)

	cm, err := chain.NewManager(cs, ctp)
	if err != nil {
		return nil, err
	}

	m := NewMiner(cm)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe miner to consensus set: %w", err)
	}
	tp.TransactionPoolSubscribe(m)
	return &Node{
		g:  g,
		cs: cs,
		cm: cm,
		tp: ctp,
		m:  m,
	}, nil
}

// NewTestingPair creates a new renter and host pair, connects them to each
// other, and funds both wallets.
func NewTestingPair(dir string, log *zap.Logger) (*Renter, *Host, error) {
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	node, err := NewNode(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create node: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "host"), 0700); err != nil {
		return nil, nil, fmt.Errorf("failed to create host dir: %w", err)
	} else if err := os.MkdirAll(filepath.Join(dir, "renter"), 0700); err != nil {
		return nil, nil, fmt.Errorf("failed to create renter dir: %w", err)
	}

	// initialize the host
	host, err := NewHost(hostKey, filepath.Join(dir, "host"), node, log.Named("host"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create host: %w", err)
	}

	// initialize the renter
	renter, err := NewRenter(renterKey, filepath.Join(dir, "renter"), node, log.Named("renter"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create renter: %w", err)
	}

	// mine enough blocks to fund the host's wallet
	if err := host.MineBlocks(host.WalletAddress(), int(stypes.MaturityDelay)*2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)

	// mine enough blocks to fund the renter's wallet
	if err := renter.MineBlocks(renter.WalletAddress(), int(stypes.MaturityDelay)*2); err != nil {
		return nil, nil, fmt.Errorf("failed to mine blocks: %w", err)
	}
	// small sleep for synchronization
	time.Sleep(time.Second)
	return renter, host, nil
}
