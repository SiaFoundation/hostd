package test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/chain"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

func convertToSiad(core types.EncoderTo, siad encoding.SiaUnmarshaler) {
	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	core.EncodeTo(e)
	e.Flush()
	if err := siad.UnmarshalSia(&buf); err != nil {
		panic(err)
	}
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

// TXPool wraps a siad transaction pool with core types.
type TXPool struct {
	tp modules.TransactionPool
}

// RecommendedFee returns the recommended fee for a transaction.
func (tp TXPool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, &fee)
	return
}

// Transactions returns all transactions in the pool.
func (tp TXPool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

// AcceptTransactionSet adds a transaction set to the pool.
func (tp TXPool) AcceptTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	err := tp.tp.AcceptTransactionSet(stxns)
	if err == modules.ErrDuplicateTransactionSet {
		err = nil
	}
	return err
}

// UnconfirmedParents returns the parents of a transaction in the pool.
func (tp TXPool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	pool := tp.Transactions()
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	seen := make(map[types.TransactionID]bool)
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[sci.ParentID]; ok {
			if txid := parent.ID(); !seen[txid] {
				seen[txid] = true
				parents = append(parents, *parent)
			}
		}
	}
	return parents, nil
}

// Subscribe subscribes to the transaction pool.
func (tp TXPool) Subscribe(subscriber modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(subscriber)
}

type (
	// A Node is a base Sia node that can be extended by a Renter or Host
	Node struct {
		privKey types.PrivateKey

		g  modules.Gateway
		cs modules.ConsensusSet
		cm *chain.Manager
		tp *TXPool
		m  *Miner
	}
)

// Close closes the node
func (n *Node) Close() error {
	n.tp.tp.Close()
	n.cs.Close()
	n.g.Close()
	return nil
}

// PublicKey returns the public key of the node
func (n *Node) PublicKey() types.PublicKey {
	return n.privKey.PublicKey()
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
func (n *Node) TPool() *TXPool {
	return n.tp
}

// NewNode creates a new Sia node and wallet with the given key
func NewNode(privKey types.PrivateKey, dir string) (*Node, error) {
	g, err := gateway.New("localhost:0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	cs, errCh := mconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus set: %w", err)
	}
	cm, err := chain.NewManager(cs)
	if err != nil {
		return nil, err
	}

	tp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %w", err)
	}
	m := NewMiner(cm)
	if err := cs.ConsensusSetSubscribe(m, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe miner to consensus set: %w", err)
	}
	tp.TransactionPoolSubscribe(m)
	return &Node{
		privKey: privKey,

		g:  g,
		cs: cs,
		cm: cm,
		tp: &TXPool{tp},
		m:  m,
	}, nil
}

// NewTestingPair creates a new renter and host pair, connects them to each
// other, and funds both wallets.
func NewTestingPair(dir string, log *zap.Logger) (*Renter, *Host, error) {
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	// initialize the host
	host, err := NewHost(hostKey, filepath.Join(dir, "host"), log.Named("host"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create host: %w", err)
	}

	// initialize the renter
	renter, err := NewRenter(renterKey, filepath.Join(dir, "renter"), log.Named("renter"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create renter: %w", err)
	}

	// connect the renter and host
	if err := host.ConnectPeer(renter.GatewayAddr()); err != nil {
		return nil, nil, fmt.Errorf("failed to connect node gateways: %w", err)
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
