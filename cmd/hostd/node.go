package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/chain"
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
	"go.sia.tech/siad/modules/consensus"
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

func startRHP2(hostKey types.PrivateKey, addr string, cs rhpv2.ChainManager, tp rhpv2.TransactionPool, w rhpv2.Wallet, cm rhpv2.ContractManager, sr rhpv2.SettingsReporter, sm rhpv2.StorageManager) (*rhpv2.SessionHandler, error) {
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

func newNode(gatewayAddr, rhp2Addr, rhp3Addr, dir string, bootstrap bool, walletKey types.PrivateKey, logger *zap.Logger) (*node, error) {
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

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), logger.Named("store"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sqlite store: %w", err)
	}

	chainManager, err := chain.NewManager(cs)
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

	sm, err := storage.NewVolumeManager(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer sm.Close()

	contractManager := contracts.NewManager(db, sm, chainManager, txpool{tp}, w)

	er := store.NewEphemeralRegistryStore(1000)
	registryManager := registry.NewManager(walletKey, er)

	rhp2, err := startRHP2(walletKey, rhp2Addr, chainManager, txpool{tp}, w, contractManager, sr, sm)
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
