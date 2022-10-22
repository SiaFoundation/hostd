package main

import (
	"crypto/ed25519"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/store"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/types"
)

type node struct {
	g  modules.Gateway
	cs modules.ConsensusSet
	tp modules.TransactionPool
	w  *wallet.SingleAddressWallet

	a  *accounts.AccountManager
	cm *contracts.ContractManager
	r  *registry.RegistryManager
	sm *store.EphemeralStorageManager

	rhp2 *rhpv2.SessionHandler
	// rhp3 *rhpv3.SessionHandler
}

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) FeeEstimate() (min, max types.Currency) {
	return tp.tp.FeeEstimation()
}

func (tp txpool) Transactions() []types.Transaction {
	return tp.tp.Transactions()
}

func (tp txpool) AcceptTransactionSet(txns []types.Transaction) error {
	return tp.tp.AcceptTransactionSet(txns)
}

func (n *node) Close() error {
	n.rhp2.Close()
	n.sm.Close()
	n.r.Close()
	n.cm.Close()
	n.a.Close()
	n.w.Close()
	n.tp.Close()
	n.cs.Close()
	n.g.Close()
	return nil
}

func newNode(gatewayAddr, rhp2Addr, rhp3Addr, dir string, bootstrap bool, walletKey ed25519.PrivateKey) (*node, error) {
	gatewayDir := filepath.Join(dir, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create gateway dir: %w", err)
	}
	g, err := gateway.New(gatewayAddr, bootstrap, gatewayDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %w", err)
	}
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, err
	}
	cs, errCh := consensus.New(g, bootstrap, consensusDir)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("failed to create consensus: %w", err)
		}
	default:
		go func() {
			if err := <-errCh; err != nil {
				log.Println("WARNING: consensus initialization returned an error:", err)
			}
		}()
	}
	tpoolDir := filepath.Join(dir, "tpool")
	if err := os.MkdirAll(tpoolDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create tpool dir: %w", err)
	}
	tp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create tpool: %w", err)
	}

	walletAddr := wallet.StandardAddress(walletKey.Public().(ed25519.PublicKey))
	ws := store.NewEphemeralWalletStore(walletAddr)
	w := wallet.NewSingleAddressWallet(walletKey, ws)
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe wallet to consensus: %w", err)
	}

	ss := store.NewEphemeralSettingsStore()
	sr, err := settings.NewConfigManager(ss)
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	as := store.NewEphemeralAccountStore()
	accountManager := accounts.NewManager(as, sr)

	sm := store.NewEphemeralStorageManager()
	contractStore := store.NewEphemeralContractStore()
	contractManager := contracts.NewManager(contractStore, sm, cs, txpool{tp}, w)

	er := store.NewEphemeralRegistryStore(1000)
	registryManager := registry.NewRegistryManager(walletKey, er)

	l, err := net.Listen("tcp", rhp2Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}
	rhp2 := rhpv2.NewSessionHandler(walletKey, cs, txpool{tp}, w, contractManager, sm, sr, stdoutmetricReporter{})
	go rhp2.Serve(l)

	return &node{
		g:  g,
		cs: cs,
		tp: tp,
		w:  w,

		a:  accountManager,
		cm: contractManager,
		sm: sm,
		r:  registryManager,
	}, nil
}
