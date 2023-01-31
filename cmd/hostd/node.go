package main

import (
	"crypto/ed25519"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.sia.tech/hostd/internal/store"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
)

type node struct {
	g     modules.Gateway
	cs    modules.ConsensusSet
	tp    modules.TransactionPool
	w     *wallet.SingleAddressWallet
	store *sqlite.Store

	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager
	registry  *registry.Manager
	storage   *storage.VolumeManager

	rhp2 *rhpv2.SessionHandler
	// rhp3 *rhpv3.SessionHandler
}

func (n *node) Close() error {
	// n.rhp3.Close()
	n.rhp2.Close()
	n.w.Close()
	n.tp.Close()
	n.cs.Close()
	n.g.Close()
	n.store.Close()
	return nil
}

func startRHP2(hostKey ed25519.PrivateKey, addr string, cs rhpv2.ChainManager, tp rhpv2.TransactionPool, w rhpv2.Wallet, cm rhpv2.ContractManager, sr rhpv2.SettingsReporter, sm rhpv2.StorageManager) (*rhpv2.SessionHandler, error) {
	rhp2, err := rhpv2.NewSessionHandler(hostKey, addr, cs, tp, w, cm, sr, sm, stdoutmetricReporter{})
	if err != nil {
		return nil, err
	}
	go rhp2.Serve()
	return rhp2, nil
}

/*func startRHP3(hostKey ed25519.PrivateKey, addr string, cs rhpv3.ChainManager, tp rhpv3.TransactionPool, am rhpv3.AccountManager, cm rhpv3.ContractManager, rm rhpv3.RegistryManager, sr rhpv3.SettingsReporter, sm rhpv3.StorageManager, w rhpv3.Wallet) (*rhpv3.SessionHandler, error) {
	rhp3, err := rhpv3.NewSessionHandler(hostKey, addr, cs, tp, w, am, cm, rm, sm, sr, stdoutmetricReporter{})
	if err != nil {
		return nil, err
	}
	go rhp3.Serve()
	return rhp3, nil
}*/

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
	cs, errCh := mconsensus.New(g, bootstrap, consensusDir)
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

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sqlite store: %w", err)
	}

	chainManager, err := consensus.NewChainManager(cs)
	if err != nil {
		return nil, fmt.Errorf("failed to create chain manager: %w", err)
	}

	w := wallet.NewSingleAddressWallet(walletKey, chainManager, db)
	go func() {
		walletChangeID, err := db.LastWalletChange()
		if err != nil {
			panic(fmt.Errorf("failed to get last wallet change: %w", err))
		}
		// note: start in goroutine for now to avoid blocking the main thread
		if err := cs.ConsensusSetSubscribe(w, walletChangeID, nil); err != nil {
			panic(fmt.Errorf("failed to subscribe wallet to consensus: %w", err))
		}
	}()
	tp.TransactionPoolSubscribe(w)

	sr, err := settings.NewConfigManager(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create settings manager: %w", err)
	}

	as := store.NewEphemeralAccountStore()
	accountManager := accounts.NewManager(as)

	sm := storage.NewVolumeManager(db)
	contractManager := contracts.NewManager(db, sm, chainManager, tp, w)

	er := store.NewEphemeralRegistryStore(1000)
	registryManager := registry.NewManager(walletKey, er)

	rhp2, err := startRHP2(walletKey, rhp2Addr, chainManager, tp, w, contractManager, sr, sm)
	if err != nil {
		return nil, fmt.Errorf("failed to start rhp2: %w", err)
	}

	/*rhp3, err := startRHP3(walletKey, rhp3Addr, chainManager, tp, accountManager, contractManager, registryManager, sr, sm, w)
	if err != nil {
		return nil, fmt.Errorf("failed to start rhp3: %w", err)
	}*/

	return &node{
		g:  g,
		cs: cs,
		tp: tp,
		w:  w,

		accounts:  accountManager,
		contracts: contractManager,
		storage:   sm,
		registry:  registryManager,

		rhp2: rhp2,
		// rhp3: rhp3,
	}, nil
}
