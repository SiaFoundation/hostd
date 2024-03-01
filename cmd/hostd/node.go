package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/hostd/internal/explorer"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhp2 "go.sia.tech/hostd/rhp/v2"
	rhp3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/hostd/webhooks"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.uber.org/zap"
)

type node struct {
	g     modules.Gateway
	a     *alerts.Manager
	wh    *webhooks.Manager
	cm    *chain.Manager
	tp    *chain.TransactionPool
	w     *wallet.SingleAddressWallet
	store *sqlite.Store

	metrics   *metrics.MetricManager
	settings  *settings.ConfigManager
	pinned    *settings.PinManager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager
	registry  *registry.Manager
	storage   *storage.VolumeManager

	sessions *rhp.SessionReporter
	data     *rhp.DataRecorder
	rhp2     *rhp2.SessionHandler
	rhp3     *rhp3.SessionHandler
}

func (n *node) Close() error {
	n.rhp3.Close()
	n.rhp2.Close()
	n.data.Close()
	n.registry.Close()
	n.storage.Close()
	n.contracts.Close()
	n.w.Close()
	n.tp.Close()
	n.cm.Close()
	n.g.Close()
	n.wh.Close()
	n.store.Close()
	return nil
}

func startRHP2(l net.Listener, hostKey types.PrivateKey, rhp3Addr string, cs rhp2.ChainManager, tp rhp2.TransactionPool, w rhp2.Wallet, cm rhp2.ContractManager, sr rhp2.SettingsReporter, sm rhp2.StorageManager, monitor rhp.DataMonitor, sessions *rhp.SessionReporter, log *zap.Logger) (*rhp2.SessionHandler, error) {
	rhp2, err := rhp2.NewSessionHandler(l, hostKey, rhp3Addr, cs, tp, w, cm, sr, sm, monitor, sessions, log)
	if err != nil {
		return nil, err
	}
	go rhp2.Serve()
	return rhp2, nil
}

func startRHP3(l net.Listener, hostKey types.PrivateKey, cs rhp3.ChainManager, tp rhp3.TransactionPool, w rhp3.Wallet, am rhp3.AccountManager, cm rhp3.ContractManager, rm rhp3.RegistryManager, sr rhp3.SettingsReporter, sm rhp3.StorageManager, monitor rhp.DataMonitor, sessions *rhp.SessionReporter, log *zap.Logger) (*rhp3.SessionHandler, error) {
	rhp3, err := rhp3.NewSessionHandler(l, hostKey, cs, tp, w, am, cm, rm, sm, sr, monitor, sessions, log)
	if err != nil {
		return nil, err
	}
	go rhp3.Serve()
	return rhp3, nil
}

func newNode(ctx context.Context, walletKey types.PrivateKey, logger *zap.Logger) (*node, types.PrivateKey, error) {
	gatewayDir := filepath.Join(cfg.Directory, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create gateway dir: %w", err)
	}
	g, err := gateway.NewCustomGateway(cfg.Consensus.GatewayAddress, cfg.Consensus.Bootstrap, false, gatewayDir, modules.ProdDependencies)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create gateway: %w", err)
	}

	// connect to additional peers from the config file
	go func() {
		for _, peer := range cfg.Consensus.Peers {
			g.Connect(modules.NetAddress(peer))
		}
	}()

	consensusDir := filepath.Join(cfg.Directory, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, types.PrivateKey{}, err
	}
	cs, errCh := consensus.New(g, cfg.Consensus.Bootstrap, consensusDir)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, types.PrivateKey{}, fmt.Errorf("failed to create consensus: %w", err)
		}
	default:
		go func() {
			if err := <-errCh; err != nil && !strings.Contains(err.Error(), "ThreadGroup already stopped") {
				logger.Warn("consensus subscribe error", zap.Error(err))
			}
		}()
	}
	tpoolDir := filepath.Join(cfg.Directory, "tpool")
	if err := os.MkdirAll(tpoolDir, 0700); err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create tpool dir: %w", err)
	}
	stp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create tpool: %w", err)
	}
	tp := chain.NewTPool(stp)

	db, err := sqlite.OpenDatabase(filepath.Join(cfg.Directory, "hostd.db"), logger.Named("sqlite"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create sqlite store: %w", err)
	}

	// load the host identity
	hostKey := db.HostKey()

	cm, err := chain.NewManager(cs)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create chain manager: %w", err)
	}

	w, err := wallet.NewSingleAddressWallet(walletKey, cm, tp, db, logger.Named("wallet"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create wallet: %w", err)
	}

	webhookReporter, err := webhooks.NewManager(db, logger.Named("webhooks"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create webhook reporter: %w", err)
	}

	rhp2Listener, err := net.Listen("tcp", cfg.RHP2.Address)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}

	rhp3Listener, err := net.Listen("tcp", cfg.RHP3.TCPAddress)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to listen on rhp3 addr: %w", err)
	}

	_, rhp2Port, err := net.SplitHostPort(cfg.RHP2.Address)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to parse rhp2 addr: %w", err)
	}
	discoveredAddr := net.JoinHostPort(g.Address().Host(), rhp2Port)
	logger.Debug("discovered address", zap.String("addr", discoveredAddr))

	am := alerts.NewManager(webhookReporter, logger.Named("alerts"))
	sr, err := settings.NewConfigManager(settings.WithHostKey(hostKey),
		settings.WithStore(db),
		settings.WithChainManager(cm),
		settings.WithTransactionPool(tp),
		settings.WithWallet(w),
		settings.WithAlertManager(am),
		settings.WithLog(logger.Named("settings")))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create settings manager: %w", err)
	}

	var pm *settings.PinManager
	if !cfg.Explorer.Disable {
		ex := explorer.New(cfg.Explorer.URL)

		pm, err = settings.NewPinManager(5*time.Minute, db, ex, sr, logger.Named("pin"))
		if err != nil {
			return nil, types.PrivateKey{}, fmt.Errorf("failed to create pin manager: %w", err)
		}
		go pm.Run(ctx)
	}

	accountManager := accounts.NewManager(db, sr)

	sm, err := storage.NewVolumeManager(db, am, cm, logger.Named("volumes"), sr.Settings().SectorCacheSize)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create storage manager: %w", err)
	}

	contractManager, err := contracts.NewManager(db, am, sm, cm, tp, w, logger.Named("contracts"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create contract manager: %w", err)
	}
	registryManager := registry.NewManager(hostKey, db, logger.Named("registry"))

	sessions := rhp.NewSessionReporter()

	dm := rhp.NewDataRecorder(db, logger.Named("data"))
	rhp2, err := startRHP2(rhp2Listener, hostKey, rhp3Listener.Addr().String(), cm, tp, w, contractManager, sr, sm, dm, sessions, logger.Named("rhp2"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to start rhp2: %w", err)
	}

	rhp3, err := startRHP3(rhp3Listener, hostKey, cm, tp, w, accountManager, contractManager, registryManager, sr, sm, dm, sessions, logger.Named("rhp3"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to start rhp3: %w", err)
	}

	return &node{
		g:     g,
		a:     am,
		wh:    webhookReporter,
		cm:    cm,
		tp:    tp,
		w:     w,
		store: db,

		metrics:   metrics.NewManager(db),
		settings:  sr,
		pinned:    pm,
		accounts:  accountManager,
		contracts: contractManager,
		storage:   sm,
		registry:  registryManager,

		sessions: sessions,
		data:     dm,
		rhp2:     rhp2,
		rhp3:     rhp3,
	}, hostKey, nil
}
