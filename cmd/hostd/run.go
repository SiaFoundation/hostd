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
	rhp4 "go.sia.tech/coreutils/rhp/v4"
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

// openSQLite3Database opens the hostd database. The function first looks for
// the deprecated hostd.db file. If that fails, it tries to open the preferred
// hostd.sqlite3 file.
func openSQLite3Database(dir string, log *zap.Logger) (*sqlite.Store, error) {
	oldPath := filepath.Join(dir, "hostd.db")
	if _, err := os.Stat(oldPath); err == nil {
		log.Warn("using deprecated hostd.db", zap.String("path", oldPath))
		return sqlite.OpenDatabase(oldPath, log)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to stat database: %w", err)
	}

	newPath := filepath.Join(dir, "hostd.sqlite3")
	return sqlite.OpenDatabase(newPath, log)
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

func parseAnnounceAddresses(listen []config.RHP4ListenAddress, announce []config.RHP4AnnounceAddress) ([]chain.NetAddress, error) {
	// build a map of the ports that each supported protocol is listening on
	protocolPorts := make(map[chain.Protocol]map[uint16]bool)
	for _, addr := range listen {
		switch addr.Protocol {
		case "tcp", "tcp4", "tcp6":
			hostname, port, err := net.SplitHostPort(addr.Address)
			if err != nil {
				return nil, fmt.Errorf("failed to parse listen address %q: %w", addr.Address, err)
			} else if ip := net.ParseIP(hostname); hostname != "" && ip == nil {
				return nil, fmt.Errorf("rhp4 listen address %q should be an IP address", addr.Address)
			}
			ports, ok := protocolPorts[rhp4.ProtocolTCPSiaMux]
			if !ok {
				ports = make(map[uint16]bool)
			}
			n, err := strconv.ParseUint(port, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("failed to parse port %q: %w", port, err)
			}
			ports[uint16(n)] = true
			protocolPorts[rhp4.ProtocolTCPSiaMux] = ports
		default:
			return nil, fmt.Errorf("unsupported protocol %q: %s", addr.Address, addr.Protocol)
		}
	}

	var addrs []chain.NetAddress
	for _, addr := range announce {
		switch addr.Protocol {
		case rhp4.ProtocolTCPSiaMux:
		default:
			return nil, fmt.Errorf("unsupported protocol %q", addr.Protocol)
		}

		hostname, port, err := net.SplitHostPort(addr.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to parse announce address %q: %w", addr.Address, err)
		}
		ip := net.ParseIP(hostname)
		if ip != nil && (ip.IsLoopback() || ip.IsUnspecified() || ip.IsGlobalUnicast()) {
			return nil, fmt.Errorf("invalid announce address %q: must be a public IP address", addr.Address)
		}

		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("failed to parse port %q: %w", port, err)
		}

		// ensure the port is being listened on
		ports, ok := protocolPorts[addr.Protocol]
		if !ok {
			return nil, fmt.Errorf("no listen address for protocol %q", addr.Protocol)
		} else if !ports[uint16(n)] {
			return nil, fmt.Errorf("no listen address for protocol %q port %d", addr.Protocol, n)
		}

		addrs = append(addrs, chain.NetAddress{
			Protocol: addr.Protocol,
			Address:  net.JoinHostPort(addr.Address, port),
		})
	}
	return addrs, nil
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

func runRootCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, log *zap.Logger) error {
	if err := deleteSiadData(cfg.Directory); err != nil {
		return fmt.Errorf("failed to migrate v1 consensus database: %w", err)
	}

	store, err := openSQLite3Database(cfg.Directory, log.Named("sqlite3"))
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
	}, syncer.WithLogger(log.Named("syncer")), syncer.WithMaxInboundPeers(64), syncer.WithMaxOutboundPeers(16))
	go s.Run(ctx)
	defer s.Close()

	wr, err := webhooks.NewManager(store, log.Named("webhooks"))
	if err != nil {
		return fmt.Errorf("failed to create webhook reporter: %w", err)
	}
	defer wr.Close()
	am := alerts.NewManager(alerts.WithEventReporter(wr), alerts.WithLog(log.Named("alerts")))

	wm, err := wallet.NewSingleAddressWallet(walletKey, cm, store, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}
	defer wm.Close()

	vm, err := storage.NewVolumeManager(store, storage.WithLogger(log.Named("volumes")), storage.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer vm.Close()

	announceAddresses, err := parseAnnounceAddresses(cfg.RHP4.ListenAddresses, cfg.RHP4.AnnounceAddresses)
	if err != nil {
		return fmt.Errorf("failed to parse announce addresses: %w", err)
	}

	sm, err := settings.NewConfigManager(hostKey, store, cm, s, wm, vm,
		settings.WithRHP4AnnounceAddresses(announceAddresses),
		settings.WithAlertManager(am),
		settings.WithLog(log.Named("settings")))
	if err != nil {
		return fmt.Errorf("failed to create settings manager: %w", err)
	}
	defer sm.Close()

	contracts, err := contracts.NewManager(store, vm, cm, s, wm, contracts.WithLog(log.Named("contracts")), contracts.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contracts.Close()

	index, err := index.NewManager(store, cm, contracts, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(cfg.Consensus.IndexBatchSize))
	if err != nil {
		return fmt.Errorf("failed to create index manager: %w", err)
	}
	defer index.Close()

	dr := rhp.NewDataRecorder(store, log.Named("data"))
	rl, wl := sm.RHPBandwidthLimiters()
	rhp2Listener, err := rhp.Listen("tcp", cfg.RHP2.Address, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
	if err != nil {
		return fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}
	defer rhp2Listener.Close()

	rhp3Listener, err := rhp.Listen("tcp", cfg.RHP3.TCPAddress, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
	if err != nil {
		return fmt.Errorf("failed to listen on rhp3 addr: %w", err)
	}
	defer rhp3Listener.Close()

	rhp2, err := rhp2.NewSessionHandler(rhp2Listener, hostKey, rhp3Listener.Addr().String(), cm, s, wm, contracts, sm, vm, log.Named("rhp2"))
	if err != nil {
		return fmt.Errorf("failed to create rhp2 session handler: %w", err)
	}
	go rhp2.Serve()
	defer rhp2.Close()

	registry := registry.NewManager(hostKey, store, log.Named("registry"))
	accounts := accounts.NewManager(store, sm)
	rhp3, err := rhp3.NewSessionHandler(rhp3Listener, hostKey, cm, s, wm, accounts, contracts, registry, vm, sm, log.Named("rhp3"))
	if err != nil {
		return fmt.Errorf("failed to create rhp3 session handler: %w", err)
	}
	go rhp3.Serve()
	defer rhp3.Close()

	rhp4 := rhp4.NewServer(hostKey, cm, s, contracts, wm, sm, vm, rhp4.WithPriceTableValidity(30*time.Minute), rhp4.WithContractProofWindowBuffer(72))

	var stopListenerFuncs []func() error
	defer func() {
		for _, f := range stopListenerFuncs {
			if err := f(); err != nil {
				log.Error("failed to stop listener", zap.Error(err))
			}
		}
	}()
	for _, addr := range cfg.RHP4.ListenAddresses {
		switch addr.Protocol {
		case "tcp", "tcp4", "tcp6":
			l, err := rhp.Listen(addr.Protocol, addr.Address, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
			if err != nil {
				return fmt.Errorf("failed to listen on rhp4 addr: %w", err)
			}
			stopListenerFuncs = append(stopListenerFuncs, l.Close)
			go rhp.ServeRHP4SiaMux(l, rhp4, log.Named("rhp4"))
		default:
			return fmt.Errorf("unsupported protocol: %s", addr.Protocol)
		}
	}

	apiOpts := []api.ServerOption{
		api.WithAlerts(am),
		api.WithLogger(log.Named("api")),
		api.WithWebhooks(wr),
		api.WithSQLite3Store(store),
	}
	if !cfg.Explorer.Disable {
		ex := explorer.New(cfg.Explorer.URL)
		pm, err := pin.NewManager(store, sm, ex, pin.WithLogger(log.Named("pin")))
		if err != nil {
			return fmt.Errorf("failed to create pin manager: %w", err)
		}

		apiOpts = append(apiOpts, api.WithPinnedSettings(pm), api.WithExplorer(ex))
	}

	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.HTTP.Password)(api.NewServer(cfg.Name, hostKey.PublicKey(), cm, s, accounts, contracts, vm, wm, store, sm, index, apiOpts...)),
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
