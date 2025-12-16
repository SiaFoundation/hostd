package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/api"
	"go.sia.tech/hostd/v2/certificates"
	"go.sia.tech/hostd/v2/certificates/providers/local"
	"go.sia.tech/hostd/v2/certificates/providers/nomad"
	"go.sia.tech/hostd/v2/config"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/explorer/connectivity"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/settings/pin"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/persist/sqlite"
	"go.sia.tech/hostd/v2/rhp"
	"go.sia.tech/hostd/v2/version"
	"go.sia.tech/hostd/v2/webhooks"
	"go.sia.tech/jape"
	"go.sia.tech/web/hostd"
	"go.uber.org/zap"
	"lukechampine.com/upnp"
)

func defaultDataDirectory(fp string) string {
	// use the provided path if it's not empty
	if fp != "" {
		return fp
	}

	// check for databases in the current directory
	if _, err := os.Stat("hostd.db"); err == nil {
		return "."
	} else if _, err := os.Stat("hostd.sqlite3"); err == nil {
		return "."
	}

	// default to the operating system's application directory
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "hostd")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "hostd")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "var", "lib", "hostd")
	default:
		return "."
	}
}

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
		log.Debug("using deprecated hostd.db", zap.String("path", oldPath))
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

func getRandomOpenPort() (uint16, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return 0, fmt.Errorf("failed to split port: %w", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("failed to parse port: %w", err)
	}
	return uint16(port), nil
}

func normalizeAddress(addr string) (string, uint16, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	} else if portStr == "" || portStr == "0" {
		randPort, err := getRandomOpenPort()
		if err != nil {
			return "", 0, fmt.Errorf("failed to get open port: %w", err)
		}
		return net.JoinHostPort(host, strconv.FormatUint(uint64(randPort), 10)), randPort, nil
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	return addr, uint16(port), err
}

func explorerURL() string {
	if cfg.Explorer.URL != "" {
		return cfg.Explorer.URL
	}
	switch cfg.Consensus.Network {
	case "zen":
		return "https://api.siascan.com/zen"
	default:
		return "https://api.siascan.com"
	}
}

func consensusExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "consensus.db"))
	return !errors.Is(err, os.ErrNotExist)
}

func runRootCmd(ctx context.Context, cfg config.Config, walletKey types.PrivateKey, instantSync bool, log *zap.Logger) error {
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
	default:
		return errors.New("invalid network: must be one of 'mainnet' or 'zen'")
	}

	walletHash := types.HashBytes(walletKey[:])
	if err := store.VerifyWalletKey(walletHash); errors.Is(err, wallet.ErrDifferentSeed) {
		if err := store.ResetChainState(); err != nil {
			return fmt.Errorf("failed to reset chain state: %w", err)
		} else if err := store.UpdateWalletHash(walletHash); err != nil {
			return fmt.Errorf("failed to update wallet hash: %w", err)
		}
		log.Info("chain state reset due to wallet seed change")
	} else if err != nil {
		return fmt.Errorf("failed to verify wallet key: %w", err)
	}

	var exp *explorer.Explorer
	if !cfg.Explorer.Disable {
		exp = explorer.New(explorerURL())
	}

	consensusExists := consensusExists(cfg.Directory)

	var dbstore *chain.DBStore
	var tipState consensus.State
	walletAddress := types.StandardUnlockHash(walletKey.PublicKey())
	if instantSync && !consensusExists {
		if exp == nil {
			return errors.New("instant sync requires the explorer to be enabled")
		}

		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		log.Debug("wallet address for checkpoint", zap.Stringer("address", walletAddress))
		checkpoint, err := exp.AddressCheckpoint(ctx, walletAddress)
		if err != nil {
			return fmt.Errorf("failed to get address checkpoint from explorer: %w", err)
		} else if checkpoint.Height < network.HardforkV2.RequireHeight {
			return fmt.Errorf("unable to instant sync: wallet checkpoint height %d is before hardfork v2 require height %d", checkpoint.Height, network.HardforkV2.RequireHeight)
		}

		log := log.With(zap.Stringer("checkpoint", checkpoint))
		log.Info("starting instant sync from checkpoint")

		cs, b, err := syncer.RetrieveCheckpoint(ctx, cfg.Syncer.Peers, checkpoint, network, genesisBlock.ID())
		if err != nil {
			return fmt.Errorf("failed to retrieve checkpoint: %w", err)
		}
		log.Debug("retrieved checkpoint")

		if err := store.SetCheckpoint(checkpoint); err != nil {
			return fmt.Errorf("failed to set checkpoint in store: %w", err)
		}

		bdb, err := coreutils.OpenBoltChainDB(filepath.Join(cfg.Directory, "consensus.db"))
		if err != nil {
			return fmt.Errorf("failed to open consensus database: %w", err)
		}
		defer bdb.Close()

		dbstore, tipState, err = chain.NewDBStoreAtCheckpoint(bdb, cs, b, chain.NewZapMigrationLogger(log.Named("chain")))
		if err != nil {
			return fmt.Errorf("failed to create chain store from checkpoint: %w", err)
		}

		log.Info("synced to checkpoint")
	} else {
		if instantSync {
			log.Warn("instant sync skipped: consensus database already exists")
		}
		bdb, err := coreutils.OpenBoltChainDB(filepath.Join(cfg.Directory, "consensus.db"))
		if err != nil {
			return fmt.Errorf("failed to open consensus database: %w", err)
		}
		defer bdb.Close()

		dbstore, tipState, err = chain.NewDBStore(bdb, network, genesisBlock, chain.NewZapMigrationLogger(log.Named("chain")))
		if err != nil {
			return fmt.Errorf("failed to create chain store: %w", err)
		}
	}

	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))

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
	}, syncer.WithLogger(log.Named("syncer")))
	go s.Run()
	defer s.Close()

	wm, err := wallet.NewSingleAddressWallet(walletKey, cm, store, s, wallet.WithLogger(log.Named("wallet")), wallet.WithReservationDuration(3*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to create wallet: %w", err)
	}
	defer wm.Close()

	wr, err := webhooks.NewManager(store, log.Named("webhooks"))
	if err != nil {
		return fmt.Errorf("failed to create webhook reporter: %w", err)
	}
	defer wr.Close()

	am := alerts.NewManager(alerts.WithEventReporter(wr), alerts.WithLog(log.Named("alerts")))
	vm, err := storage.NewVolumeManager(store, storage.WithLogger(log.Named("volumes")), storage.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer vm.Close()

	var rhp4PortStr string
	var hasQuic bool
	var hasSiamux bool
	protos := make(map[config.RHP4Proto]bool)
	for _, addr := range cfg.RHP4.ListenAddresses {
		if protos[addr.Protocol] {
			return fmt.Errorf("RHP4 listen addresses must not have duplicate protocols: %s", addr.Protocol)
		}
		protos[addr.Protocol] = true
		switch {
		case addr.Protocol == config.RHP4ProtoTCP || addr.Protocol == config.RHP4ProtoTCP4 || addr.Protocol == config.RHP4ProtoTCP6:
			hasSiamux = true
		case addr.Protocol == config.RHP4ProtoQUIC || addr.Protocol == config.RHP4ProtoQUIC4 || addr.Protocol == config.RHP4ProtoQUIC6:
			hasQuic = true
		default:
			return fmt.Errorf("unsupported RHP4 protocol: %s", addr.Protocol)
		}

		_, portStr, err := net.SplitHostPort(addr.Address)
		if err != nil {
			return fmt.Errorf("failed to parse RHP4 address: %w", err)
		} else if rhp4PortStr == "" {
			rhp4PortStr = portStr
		} else if rhp4PortStr != portStr {
			return errors.New("RHP4 listen addresses must all have the same port")
		}
	}
	_, rhp4Port, err := normalizeAddress(net.JoinHostPort("", rhp4PortStr))
	if err != nil {
		return fmt.Errorf("failed to normalize RHP4 address: %w", err)
	}
	// update the listen addresses with the normalized port
	for i := range cfg.RHP4.ListenAddresses {
		host, _, _ := net.SplitHostPort(cfg.RHP4.ListenAddresses[i].Address)
		cfg.RHP4.ListenAddresses[i].Address = net.JoinHostPort(host, strconv.FormatUint(uint64(rhp4Port), 10))
	}

	if !hasSiamux {
		cfg.RHP4.ListenAddresses = append(cfg.RHP4.ListenAddresses, config.RHP4ListenAddress{
			Protocol: config.RHP4ProtoTCP,
			Address:  fmt.Sprintf(":%d", rhp4Port),
		})
		log.Debug("no RHP4 SiaMux address specified, adding default", zap.String("address", cfg.RHP4.ListenAddresses[len(cfg.RHP4.ListenAddresses)-1].Address))
	}

	if !hasQuic {
		cfg.RHP4.ListenAddresses = append(cfg.RHP4.ListenAddresses, config.RHP4ListenAddress{
			Protocol: config.RHP4ProtoQUIC,
			Address:  fmt.Sprintf(":%d", rhp4Port),
		})
		log.Debug("no RHP4 QUIC address specified, adding default", zap.String("address", cfg.RHP4.ListenAddresses[len(cfg.RHP4.ListenAddresses)-1].Address))
	}

	var certProvider certificates.Provider
	if cfg.RHP4.QUIC.CertPath != "" || cfg.RHP4.QUIC.KeyPath != "" {
		certProvider, err = local.NewProvider(cfg.RHP4.QUIC.CertPath, cfg.RHP4.QUIC.KeyPath, am, log)
		if err != nil {
			return fmt.Errorf("failed to create local certificates provider: %w", err)
		}
	} else {
		nm := nomad.NewProvider(filepath.Join(cfg.Directory, "nomad"), hostKey, log.Named("certificates.nomad"))
		defer nm.Close()
		certProvider = nm
	}

	settingsOpts := []settings.Option{
		settings.WithAlertManager(am),
		settings.WithRHP4Port(uint16(rhp4Port)),
		settings.WithLog(log.Named("settings")),
		settings.WithCertificates(certProvider),
	}

	sm, err := settings.NewConfigManager(hostKey, store, cm, vm, wm, settingsOpts...)
	if err != nil {
		return fmt.Errorf("failed to create settings manager: %w", err)
	}
	defer sm.Close()

	contractManager, err := contracts.NewManager(store, vm, cm, wm, contracts.WithLog(log.Named("contracts")), contracts.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contractManager.Close()

	index, err := index.NewManager(store, cm, contractManager, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(cfg.Consensus.IndexBatchSize))
	if err != nil {
		return fmt.Errorf("failed to create index manager: %w", err)
	}
	defer index.Close()

	dr := rhp.NewDataRecorder(store, log.Named("data"))
	rl, wl := sm.RHPBandwidthLimiters()

	rhp4 := rhp4.NewServer(hostKey, cm, contractManager, wm, sm, vm, rhp4.WithPriceTableValidity(30*time.Minute))

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
		case config.RHP4ProtoTCP, config.RHP4ProtoTCP4, config.RHP4ProtoTCP6:
			l, err := rhp.Listen(string(addr.Protocol), addr.Address, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
			if err != nil {
				return fmt.Errorf("failed to listen on rhp4 addr: %w", err)
			}
			log.Info("started RHP4 listener", zap.String("address", l.Addr().String()))
			stopListenerFuncs = append(stopListenerFuncs, l.Close)
			go siamux.Serve(l, rhp4, log.Named("rhp4.siamux"))
		case config.RHP4ProtoQUIC, config.RHP4ProtoQUIC4, config.RHP4ProtoQUIC6:
			var proto string
			switch addr.Protocol {
			case config.RHP4ProtoQUIC:
				proto = "udp"
			case config.RHP4ProtoQUIC4:
				proto = "udp4"
			case config.RHP4ProtoQUIC6:
				proto = "udp6"
			}
			udpAddr, err := net.ResolveUDPAddr(proto, addr.Address)
			if err != nil {
				return fmt.Errorf("failed to resolve UDP address: %w", err)
			}
			l, err := net.ListenUDP(proto, udpAddr)
			if err != nil {
				return fmt.Errorf("failed to listen on RHP4 QUIC address: %w", err)
			}
			stopListenerFuncs = append(stopListenerFuncs, l.Close)
			pc := rhp.NewRHPPacketConn(l, rl, wl, dr)
			ql, err := quic.Listen(pc, certificates.NewQUICCertManager(certProvider))
			if err != nil {
				return fmt.Errorf("failed to listen on RHP4 QUIC address: %w", err)
			}
			log.Info("started RHP4 QUIC listener", zap.String("address", l.LocalAddr().String()))
			stopListenerFuncs = append(stopListenerFuncs, ql.Close)
			go quic.Serve(ql, rhp4, log.Named("rhp4.quic"))
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
		apiOpts = append(apiOpts, api.WithExplorer(exp))

		// enable price pinning
		pm, err := pin.NewManager(store, sm, exp, pin.WithLogger(log.Named("pin")))
		if err != nil {
			return fmt.Errorf("failed to create pin manager: %w", err)
		}
		defer pm.Close()

		// enable connectivity tests
		connectivity, err := connectivity.NewManager(hostKey.PublicKey(), sm, exp, connectivity.WithLog(log.Named("connectivity")), connectivity.WithAlerts(am))
		if err != nil {
			return fmt.Errorf("failed to create connectivity manager: %w", err)
		}
		defer connectivity.Close()

		apiOpts = append(apiOpts, api.WithPinnedSettings(pm), api.WithConnectivity(connectivity))
	}
	go version.RunVersionCheck(ctx, am, log.Named("version"))
	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.HTTP.Password)(api.NewServer(cfg.Name, hostKey.PublicKey(), cm, s, contractManager, vm, wm, store, sm, index, apiOpts...)),
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

	log.Info("node started", zap.String("network", cm.TipState().Network.Name), zap.String("hostKey", hostKey.PublicKey().String()), zap.String("http", httpListener.Addr().String()), zap.String("p2p", string(s.Addr())))
	<-ctx.Done()
	log.Info("shutting down...")
	time.AfterFunc(time.Minute, func() {
		log.Fatal("failed to shut down within 5 minutes")
	})
	return nil
}
