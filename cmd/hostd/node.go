package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/chain"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/logging"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/rhp"
	rhpv2 "go.sia.tech/hostd/rhp/v2"
	rhpv3 "go.sia.tech/hostd/rhp/v3"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, &fee)
	return
}

func (tp txpool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

func (tp txpool) AcceptTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	return tp.tp.AcceptTransactionSet(stxns)
}

func (tp txpool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
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

func (tp txpool) Subscribe(s modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(s)
}

func (tp txpool) Close() error {
	return tp.tp.Close()
}

type node struct {
	g     modules.Gateway
	cm    *chain.Manager
	tp    *txpool
	w     *wallet.SingleAddressWallet
	store *sqlite.Store

	metrics   *metrics.MetricManager
	settings  *settings.ConfigManager
	accounts  *accounts.AccountManager
	contracts *contracts.ContractManager
	registry  *registry.Manager
	storage   *storage.VolumeManager

	rhp2Monitor *rhp.DataRecorder
	rhp2        *rhpv2.SessionHandler
	rhp3Monitor *rhp.DataRecorder
	rhp3        *rhpv3.SessionHandler
}

func (n *node) Close() error {
	n.rhp3.Close()
	n.rhp2.Close()
	n.rhp2Monitor.Close()
	n.rhp3Monitor.Close()
	n.storage.Close()
	n.contracts.Close()
	n.w.Close()
	n.tp.Close()
	n.cm.Close()
	n.g.Close()
	n.store.Close()
	return nil
}

func startRHP2(l net.Listener, hostKey types.PrivateKey, rhp3Addr string, cs rhpv2.ChainManager, tp rhpv2.TransactionPool, w rhpv2.Wallet, cm rhpv2.ContractManager, sr rhpv2.SettingsReporter, sm rhpv2.StorageManager, monitor rhp.DataMonitor, log *zap.Logger) (*rhpv2.SessionHandler, error) {
	rhp2, err := rhpv2.NewSessionHandler(l, hostKey, rhp3Addr, cs, tp, w, cm, sr, sm, monitor, discardMetricReporter{}, log)
	if err != nil {
		return nil, err
	}
	go rhp2.Serve()
	return rhp2, nil
}

func startRHP3(l net.Listener, hostKey types.PrivateKey, cs rhpv3.ChainManager, tp rhpv3.TransactionPool, w rhpv3.Wallet, am rhpv3.AccountManager, cm rhpv3.ContractManager, rm rhpv3.RegistryManager, sr rhpv3.SettingsReporter, sm rhpv3.StorageManager, monitor rhp.DataMonitor, log *zap.Logger) (*rhpv3.SessionHandler, error) {
	rhp3, err := rhpv3.NewSessionHandler(l, hostKey, cs, tp, w, am, cm, rm, sm, sr, monitor, discardMetricReporter{}, log)
	if err != nil {
		return nil, err
	}
	go rhp3.Serve()
	return rhp3, nil
}

func newNode(gatewayAddr, rhp2Addr, rhp3Addr, dir string, bootstrap bool, walletKey types.PrivateKey, logger *zap.Logger, logLevel zapcore.Level) (*node, types.PrivateKey, error) {
	gatewayDir := filepath.Join(dir, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create gateway dir: %w", err)
	}
	g, err := gateway.NewCustomGateway(gatewayAddr, bootstrap, false, gatewayDir, modules.ProdDependencies)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create gateway: %w", err)
	}
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, types.PrivateKey{}, err
	}
	cs, errCh := consensus.New(g, bootstrap, consensusDir)
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
	tpoolDir := filepath.Join(dir, "tpool")
	if err := os.MkdirAll(tpoolDir, 0700); err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create tpool dir: %w", err)
	}
	stp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create tpool: %w", err)
	}
	tp := &txpool{stp}

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), logger.Named("sqlite"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create sqlite store: %w", err)
	}

	// create a new zap core by combining the current logger and a custom logging core
	core := zapcore.NewTee(logger.Core(), logging.Core(db, logLevel))
	// reinstantiate the logger with the new core
	logger = zap.New(core)
	// reset the logger so queries are logged to the database
	db.SetLogger(logger.Named("sqlite"))

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

	rhp2Listener, err := net.Listen("tcp", rhp2Addr)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to listen on rhp2 addr: %w", err)
	}

	rhp3Listener, err := net.Listen("tcp", rhp3Addr)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to listen on rhp3 addr: %w", err)
	}

	_, rhp2Port, err := net.SplitHostPort(rhp2Addr)
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to parse rhp2 addr: %w", err)
	}
	discoveredAddr := net.JoinHostPort(g.Address().Host(), rhp2Port)
	logger.Debug("discovered address", zap.String("addr", discoveredAddr))

	sr, err := settings.NewConfigManager(dir, hostKey, discoveredAddr, db, cm, tp, w, logger.Named("settings"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create settings manager: %w", err)
	}

	accountManager := accounts.NewManager(db, sr)

	sm, err := storage.NewVolumeManager(db, cm, logger.Named("volumes"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create storage manager: %w", err)
	}

	contractManager, err := contracts.NewManager(db, sm, cm, tp, w, logger.Named("contracts"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to create contract manager: %w", err)
	}
	registryManager := registry.NewManager(hostKey, db, logger.Named("registry"))

	rhp2Monitor := rhp.NewDataRecorder(&rhp2MonitorStore{db}, logger.Named("rhp2Monitor"))
	rhp2, err := startRHP2(rhp2Listener, hostKey, rhp3Listener.Addr().String(), cm, tp, w, contractManager, sr, sm, rhp2Monitor, logger.Named("rhpv2"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to start rhp2: %w", err)
	}

	rhp3Monitor := rhp.NewDataRecorder(&rhp3MonitorStore{db}, logger.Named("rhp3Monitor"))
	rhp3, err := startRHP3(rhp3Listener, hostKey, cm, tp, w, accountManager, contractManager, registryManager, sr, sm, rhp3Monitor, logger.Named("rhpv3"))
	if err != nil {
		return nil, types.PrivateKey{}, fmt.Errorf("failed to start rhp3: %w", err)
	}

	return &node{
		g:     g,
		cm:    cm,
		tp:    tp,
		w:     w,
		store: db,

		metrics:   metrics.NewManager(db),
		settings:  sr,
		accounts:  accountManager,
		contracts: contractManager,
		storage:   sm,
		registry:  registryManager,

		rhp2Monitor: rhp2Monitor,
		rhp2:        rhp2,
		rhp3Monitor: rhp3Monitor,
		rhp3:        rhp3,
	}, hostKey, nil
}
