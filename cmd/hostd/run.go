package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/api"
	"go.sia.tech/hostd/config"
	"go.sia.tech/hostd/explorer"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/index"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhp2 "go.sia.tech/hostd/rhp/v2"
	rhp3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/webhooks"
	"go.sia.tech/jape"
	"go.sia.tech/web/hostd"
	"go.uber.org/zap"
	"lukechampine.com/upnp"
)

func setupUPNP(ctx context.Context, port uint16, log *zap.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	d, err := upnp.Discover(ctx)
	if err != nil {
		return "", fmt.Errorf("couldn't discover UPnP router: %w", err)
	} else if !d.IsForwarded(port, "TCP") {
		if err := d.Forward(uint16(port), "TCP", "walletd"); err != nil {
			log.Debug("couldn't forward port", zap.Error(err))
		} else {
			log.Debug("upnp: forwarded p2p port", zap.Uint16("port", port))
		}
	}
	return d.ExternalIP()
}

// migrateDB renames hostd.db to hostd.sqlite3 to be explicit about the store
func migrateDB(dir string) error {
	oldPath := filepath.Join(dir, "hostd.db")
	newPath := filepath.Join(dir, "hostd.sqlite3")
	if _, err := os.Stat(oldPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return os.Rename(oldPath, newPath)
}

// deleteSiadData deletes the siad specific databases if they exist
func deleteSiadData(dir string) error {
	paths := []string{
		filepath.Join(dir, "consensus"),
		filepath.Join(dir, "gateway"),
		filepath.Join(dir, "tpool"),
	}

	for _, path := range paths {
		if dir, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		} else if !dir.IsDir() {
			return fmt.Errorf("expected %s to be a directory", path)
		}

		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to delete %s: %w", path, err)
		}
	}
	return nil
}

// startLocalhostListener https://github.com/SiaFoundation/hostd/issues/202
func startLocalhostListener(listenAddr string, log *zap.Logger) (l net.Listener, err error) {
	addr, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API address: %w", err)
	}

	// if the address is not localhost, listen on the address as-is
	if addr != "localhost" {
		return net.Listen("tcp", listenAddr)
	}

	// localhost fails on some new installs of Windows 11, so try a few
	// different addresses
	tryAddresses := []string{
		net.JoinHostPort("localhost", port), // original address
		net.JoinHostPort("127.0.0.1", port), // IPv4 loopback
		net.JoinHostPort("::1", port),       // IPv6 loopback
	}

	for _, addr := range tryAddresses {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			return
		}
		log.Debug("failed to listen on fallback address", zap.String("address", addr), zap.Error(err))
	}
	return
}

func runNode(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, log *zap.Logger) error {
	if err := migrateDB(cfg.Directory); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	} else if err := deleteSiadData(cfg.Directory); err != nil {
		return fmt.Errorf("failed to migrate v1 consensus database: %w", err)
	}

	store, err := sqlite.OpenDatabase(filepath.Join(cfg.Directory, "hostd.sqlite3"), log.Named("sqlite3"))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// load the host identity
	hostKey := store.HostKey()

	var network *consensus.Network
	var genesisBlock types.Block
	switch cfg.Consensus.Network {
	case "mainnet":
		network, genesisBlock = chain.Mainnet()
		if cfg.Syncer.Bootstrap {
			cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.MainnetBootstrapPeers...)
		}
	case "zen":
		network, genesisBlock = chain.TestnetZen()
		if cfg.Syncer.Bootstrap {
			cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.ZenBootstrapPeers...)
		}
	case "anagami":
		network, genesisBlock = chain.TestnetAnagami()
		if cfg.Syncer.Bootstrap {
			cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.AnagamiBootstrapPeers...)
		}
	default:
		return errors.New("invalid network: must be one of 'mainnet' or 'zen'")
	}

	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(cfg.Directory, "consensus.db"))
	if err != nil {
		return fmt.Errorf("failed to open consensus database: %w", err)
	}
	defer bdb.Close()

	dbstore, tipState, err := chain.NewDBStore(bdb, network, genesisBlock)
	if err != nil {
		return fmt.Errorf("failed to create chain store: %w", err)
	}
	cm := chain.NewManager(dbstore, tipState)

	httpListener, err := startLocalhostListener(cfg.HTTP.Address, log.Named("listener"))
	if err != nil {
		return fmt.Errorf("failed to listen on http address: %w", err)
	}
	defer httpListener.Close()

	syncerListener, err := net.Listen("tcp", cfg.Syncer.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on syncer address: %w", err)
	}
	defer syncerListener.Close()

	rhp2Listener, err := net.Listen("tcp", cfg.RHP2.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}
	defer rhp2Listener.Close()

	rhp3Listener, err := net.Listen("tcp", cfg.RHP3.TCPAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on rhp3 addr: %w", err)
	}
	defer rhp3Listener.Close()

	syncerAddr := syncerListener.Addr().String()
	if cfg.Syncer.EnableUPnP {
		_, portStr, _ := net.SplitHostPort(cfg.Syncer.Address)
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return fmt.Errorf("failed to parse syncer port: %w", err)
		}

		ip, err := setupUPNP(context.Background(), uint16(port), log)
		if err != nil {
			log.Warn("failed to set up UPnP", zap.Error(err))
		} else {
			syncerAddr = net.JoinHostPort(ip, portStr)
		}
	}
	// peers will reject us if our hostname is empty or unspecified, so use loopback
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	ps, err := sqlite.NewPeerStore(store)
	if err != nil {
		return fmt.Errorf("failed to create peer store: %w", err)
	}
	for _, peer := range cfg.Syncer.Peers {
		if err := ps.AddPeer(peer); err != nil {
			log.Warn("failed to add peer", zap.String("address", peer), zap.Error(err))
		}
	}

	log.Debug("starting syncer", zap.String("syncer address", syncerAddr))
	s := syncer.New(syncerListener, cm, ps, gateway.Header{
		GenesisID:  genesisBlock.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}, syncer.WithLogger(log.Named("syncer")))
	go s.Run(ctx)
	defer s.Close()

	wm, err := wallet.NewSingleAddressWallet(walletKey, cm, store, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}
	defer wm.Close()

	wr, err := webhooks.NewManager(store, log.Named("webhooks"))
	if err != nil {
		return fmt.Errorf("failed to create webhook reporter: %w", err)
	}
	defer wr.Close()
	sr := rhp.NewSessionReporter()

	am := alerts.NewManager(alerts.WithEventReporter(wr), alerts.WithLog(log.Named("alerts")))

	cfm, err := settings.NewConfigManager(hostKey, store, cm, s, wm, settings.WithAlertManager(am), settings.WithLog(log.Named("settings")))
	if err != nil {
		return fmt.Errorf("failed to create settings manager: %w", err)
	}
	defer cfm.Close()

	vm, err := storage.NewVolumeManager(store, storage.WithLogger(log.Named("volumes")), storage.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer vm.Close()

	contractManager, err := contracts.NewManager(store, vm, cm, s, wm, contracts.WithLog(log.Named("contracts")), contracts.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contractManager.Close()

	index, err := index.NewManager(store, cm, contractManager, wm, cfm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(cfg.Consensus.IndexBatchSize))
	if err != nil {
		return fmt.Errorf("failed to create index manager: %w", err)
	}
	defer index.Close()

	dr := rhp.NewDataRecorder(store, log.Named("data"))

	rhp2, err := rhp2.NewSessionHandler(rhp2Listener, hostKey, rhp3Listener.Addr().String(), cm, s, wm, contractManager, cfm, vm, rhp2.WithDataMonitor(dr), rhp2.WithLog(log.Named("rhp2")))
	if err != nil {
		return fmt.Errorf("failed to create rhp2 session handler: %w", err)
	}
	go rhp2.Serve()
	defer rhp2.Close()

	registry := registry.NewManager(hostKey, store, log.Named("registry"))
	accounts := accounts.NewManager(store, cfm)
	rhp3, err := rhp3.NewSessionHandler(rhp3Listener, hostKey, cm, s, wm, accounts, contractManager, registry, vm, cfm, rhp3.WithDataMonitor(dr), rhp3.WithSessionReporter(sr), rhp3.WithLog(log.Named("rhp3")))
	if err != nil {
		return fmt.Errorf("failed to create rhp3 session handler: %w", err)
	}
	go rhp3.Serve()
	defer rhp3.Close()

	apiOpts := []api.ServerOption{
		api.ServerWithAlerts(am),
		api.ServerWithLogger(log.Named("api")),
		api.ServerWithRHPSessionReporter(sr),
		api.ServerWithWebhooks(wr),
	}
	if !cfg.Explorer.Disable {
		ex := explorer.New(cfg.Explorer.URL)
		pm, err := pin.NewManager(store, cfm, ex, pin.WithLogger(log.Named("pin")))
		if err != nil {
			return fmt.Errorf("failed to create pin manager: %w", err)
		}

		apiOpts = append(apiOpts, api.ServerWithPinnedSettings(pm), api.ServerWithExplorer(ex))
	}

	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.HTTP.Password)(api.NewServer(cfg.Name, hostKey.PublicKey(), cm, s, accounts, contractManager, vm, wm, store, cfm, index, apiOpts...)),
			ui:  hostd.Handler(),
		},
		ReadTimeout: 30 * time.Second,
	}
	defer web.Close()

	go func() {
		log.Debug("starting http server", zap.String("address", cfg.HTTP.Address))
		if err := web.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(err))
		}
	}()

	if cfg.AutoOpenWebUI {
		time.Sleep(time.Millisecond) // give the web server a chance to start
		_, port, err := net.SplitHostPort(httpListener.Addr().String())
		if err != nil {
			log.Debug("failed to parse API address", zap.Error(err))
		} else if err := openBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			log.Debug("failed to open browser", zap.Error(err))
		}
	}

	log.Info("node started", zap.String("network", cm.TipState().Network.Name), zap.String("hostKey", hostKey.PublicKey().String()), zap.String("http", httpListener.Addr().String()), zap.String("p2p", string(s.Addr())), zap.String("rhp2", rhp2.LocalAddr()), zap.String("rhp3", rhp3.LocalAddr()))
	<-ctx.Done()
	log.Info("shutting down...")
	time.AfterFunc(5*time.Minute, func() {
		log.Fatal("failed to shut down within 5 minutes")
	})
	return nil
}
