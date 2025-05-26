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
	"go.sia.tech/hostd/v2/config"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/host/accounts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/registry"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/settings/pin"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/persist/sqlite"
	"go.sia.tech/hostd/v2/rhp"
	rhp2 "go.sia.tech/hostd/v2/rhp/v2"
	rhp3 "go.sia.tech/hostd/v2/rhp/v3"
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

// migrateConsensusDB checks if the consensus database needs to be migrated
// to match the new v2 commitment.
func migrateConsensusDB(fp string, n *consensus.Network, genesis types.Block, log *zap.Logger) error {
	bdb, err := coreutils.OpenBoltChainDB(fp)
	if err != nil {
		return fmt.Errorf("failed to open consensus database: %w", err)
	}
	defer bdb.Close()

	dbstore, tipState, err := chain.NewDBStore(bdb, n, genesis, chain.NewZapMigrationLogger(log.Named("chaindb")))
	if err != nil {
		return fmt.Errorf("failed to create chain store: %w", err)
	} else if tipState.Index.Height < n.HardforkV2.AllowHeight {
		log.Debug("chain is still on v1 -- no migration needed")
		return nil // no migration needed, the chain is still on v1
	}

	log.Debug("checking for v2 commitment migration")
	b, _, ok := dbstore.Block(tipState.Index.ID)
	if !ok {
		return fmt.Errorf("failed to get tip block %q", tipState.Index)
	} else if b.V2 == nil {
		log.Debug("tip block is not a v2 block -- skipping commitment migration")
		return nil
	}

	parentState, ok := dbstore.State(b.ParentID)
	if !ok {
		return fmt.Errorf("failed to get parent state for tip block %q", b.ParentID)
	}
	commitment := parentState.Commitment(b.MinerPayouts[0].Address, b.Transactions, b.V2Transactions())
	log = log.With(zap.Stringer("tip", b.ID()), zap.Stringer("commitment", b.V2.Commitment), zap.Stringer("expected", commitment))
	if b.V2.Commitment == commitment {
		log.Debug("tip block commitment matches parent state -- no migration needed")
		return nil
	}
	// reset the database if the commitment is not a merkle root
	log.Debug("resetting consensus database for new v2 commitment")
	if err := bdb.Close(); err != nil {
		return fmt.Errorf("failed to close old consensus database: %w", err)
	} else if err := os.RemoveAll(fp); err != nil {
		return fmt.Errorf("failed to remove old consensus database: %w", err)
	}
	log.Debug("consensus database reset")
	return nil
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
	case "erravimus":
		network, genesisBlock = chain.TestnetErravimus()
		if cfg.Syncer.Bootstrap {
			cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.ErravimusBootstrapPeers...)
		}
	default:
		return errors.New("invalid network: must be one of 'mainnet' or 'zen'")
	}

	var exp *explorer.Explorer
	if !cfg.Explorer.Disable {
		exp = explorer.New(explorerURL())
	}

	consensusPath := filepath.Join(cfg.Directory, "consensus.db")
	if err := migrateConsensusDB(consensusPath, network, genesisBlock, log); err != nil {
		return fmt.Errorf("failed to migrate consensus database: %w", err)
	}

	bdb, err := coreutils.OpenBoltChainDB(consensusPath)
	if err != nil {
		return fmt.Errorf("failed to open consensus database: %w", err)
	}
	defer bdb.Close()

	dbstore, tipState, err := chain.NewDBStore(bdb, network, genesisBlock, chain.NewZapMigrationLogger(log.Named("chain")))
	if err != nil {
		return fmt.Errorf("failed to create chain store: %w", err)
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

	var minRebroadcastHeight uint64
	if height := cm.Tip().Height; height > 4380 {
		minRebroadcastHeight = height - 4380
	}
	rebroadcast, err := store.RebroadcastFormationSets(minRebroadcastHeight)
	if err != nil {
		return fmt.Errorf("failed to load rebroadcast formation sets: %w", err)
	}
	for _, formationSet := range rebroadcast {
		if len(formationSet) == 0 {
			continue
		}

		formationTxn := formationSet[len(formationSet)-1]
		if len(formationTxn.FileContracts) == 0 {
			continue
		}
		log := log.Named("rebroadcast").With(zap.Stringer("transactionID", formationTxn.ID()), zap.Stringer("contractID", formationTxn.FileContractID(0)))

		// add all transactions back to the pool progressively in case some of them were confirmed.
		for _, txn := range formationSet {
			cm.AddPoolTransactions(append(cm.UnconfirmedParents(txn), txn)) // error is ignored because it will be caught below
		}
		// update the formation set and broadcast it as a whole. Technically not necessary, but logs the error if it still fails
		formationSet = append(cm.UnconfirmedParents(formationTxn), formationTxn)
		if _, err := cm.AddPoolTransactions(formationSet); err != nil {
			log.Debug("failed to add formation set to pool", zap.Error(err))
			continue
		} else if err := s.BroadcastTransactionSet(formationSet); err != nil {
			log.Debug("failed to rebroadcast formation set", zap.Error(err))
			continue
		}
		log.Debug("rebroadcasted formation set")
	}

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

	am := alerts.NewManager(alerts.WithEventReporter(wr), alerts.WithLog(log.Named("alerts")))

	rhp2Addr, rhp2Port, err := normalizeAddress(cfg.RHP2.Address)
	if err != nil {
		return fmt.Errorf("failed to normalize RHP2 address: %w", err)
	}

	rhp3Addr, rhp3Port, err := normalizeAddress(cfg.RHP3.TCPAddress)
	if err != nil {
		return fmt.Errorf("failed to normalize RHP3 address: %w", err)
	}

	vm, err := storage.NewVolumeManager(store, storage.WithLogger(log.Named("volumes")), storage.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer vm.Close()

	var rhp4PortStr string
	for _, addr := range cfg.RHP4.ListenAddresses {
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

	sm, err := settings.NewConfigManager(hostKey, store, cm, s, vm, wm,
		settings.WithAlertManager(am),
		settings.WithRHP2Port(uint16(rhp2Port)),
		settings.WithRHP3Port(uint16(rhp3Port)),
		settings.WithRHP4Port(uint16(rhp4Port)),
		settings.WithExplorer(exp),
		settings.WithLog(log.Named("settings")))
	if err != nil {
		return fmt.Errorf("failed to create settings manager: %w", err)
	}
	defer sm.Close()

	contractManager, err := contracts.NewManager(store, vm, cm, s, wm, contracts.WithLog(log.Named("contracts")), contracts.WithAlerter(am))
	if err != nil {
		return fmt.Errorf("failed to create contracts manager: %w", err)
	}
	defer contractManager.Close()

	index, err := index.NewManager(store, cm, contractManager, wm, sm, vm, index.WithLog(log.Named("index")), index.WithBatchSize(cfg.Consensus.IndexBatchSize))
	if err != nil {
		return fmt.Errorf("failed to create index manager: %w", err)
	}
	defer index.Close()

	certs, err := certificates.NewManager(cfg.Directory, hostKey, certificates.WithLocalCert(cfg.RHP4.QUIC.CertPath, cfg.RHP4.QUIC.KeyPath), certificates.WithLog(log.Named("certificates")))
	if err != nil {
		return fmt.Errorf("failed to create certificates manager: %w", err)
	}
	defer certs.Close()

	dr := rhp.NewDataRecorder(store, log.Named("data"))
	rl, wl := sm.RHPBandwidthLimiters()
	rhp2Listener, err := rhp.Listen("tcp", rhp2Addr, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
	if err != nil {
		return fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}
	defer rhp2Listener.Close()

	rhp3Listener, err := rhp.Listen("tcp", rhp3Addr, rhp.WithDataMonitor(dr), rhp.WithReadLimit(rl), rhp.WithWriteLimit(wl))
	if err != nil {
		return fmt.Errorf("failed to listen on rhp3 addr: %w", err)
	}
	defer rhp3Listener.Close()

	rhp2 := rhp2.NewSessionHandler(rhp2Listener, hostKey, cm, s, wm, contractManager, sm, vm, log.Named("rhp2"))
	go rhp2.Serve()
	defer rhp2.Close()

	registry := registry.NewManager(hostKey, store, log.Named("registry"))
	accounts := accounts.NewManager(store, sm)
	rhp3 := rhp3.NewSessionHandler(rhp3Listener, hostKey, cm, s, wm, accounts, contractManager, registry, vm, sm, log.Named("rhp3"))
	go rhp3.Serve()
	defer rhp3.Close()

	rhp4 := rhp4.NewServer(hostKey, cm, s, contractManager, wm, sm, vm, rhp4.WithPriceTableValidity(30*time.Minute))

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
			log.Info("started RHP4 listener", zap.String("address", l.Addr().String()))
			stopListenerFuncs = append(stopListenerFuncs, l.Close)
			go siamux.Serve(l, rhp4, log.Named("rhp4.siamux"))
		case "quic", "quic4", "quic6":
			var proto string
			switch addr.Protocol {
			case "quic":
				proto = "udp"
			case "quic4":
				proto = "udp4"
			case "quic6":
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
			ql, err := quic.Listen(l, certs)
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
		api.WithExplorer(exp),
	}

	if !cfg.Explorer.Disable {
		pm, err := pin.NewManager(store, sm, exp, pin.WithLogger(log.Named("pin")))
		if err != nil {
			return fmt.Errorf("failed to create pin manager: %w", err)
		}
		defer pm.Close()

		apiOpts = append(apiOpts, api.WithPinnedSettings(pm))
	}
	go version.RunVersionCheck(ctx, am, log.Named("version"))
	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.HTTP.Password)(api.NewServer(cfg.Name, hostKey.PublicKey(), cm, s, accounts, contractManager, vm, wm, store, sm, index, apiOpts...)),
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
