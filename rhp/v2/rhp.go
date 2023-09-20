package rhp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	defaultBatchSize = 20 * (1 << 20) // 20 MiB

	// Version is the current version of the RHP2 protocol.
	Version = "1.6.0"
)

type (
	// A ContractManager manages the set of contracts that the host is currently
	// storing data for
	ContractManager interface {
		// Lock locks the contract with the given ID. Will wait for the given
		// duration before giving up. Unlock must be called to unlock the
		// contract.
		Lock(ctx context.Context, id types.FileContractID) (contracts.SignedRevision, error)
		// Unlock unlocks the contract with the given ID.
		Unlock(id types.FileContractID)

		// AddContract adds a new contract to the manager.
		AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, initialUsage contracts.Usage) error
		// RenewContract renews an existing contract.
		RenewContract(renewal contracts.SignedRevision, existing contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, clearingUsage, renewalUsage contracts.Usage) error
		// ReviseContract atomically revises a contract and its sector roots
		ReviseContract(contractID types.FileContractID) (*contracts.ContractUpdater, error)

		// SectorRoots returns the sector roots of the contract with the given ID.
		SectorRoots(id types.FileContractID, limit, offset int) ([]types.Hash256, error)
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

		// Write writes a sector to persistent storage. release should only be
		// called after the contract roots have been committed to prevent the
		// sector from being deleted.
		Write(root types.Hash256, data *[rhp2.SectorSize]byte) (release func() error, _ error)
		// Read reads the sector with the given root from the manager.
		Read(root types.Hash256) (*[rhp2.SectorSize]byte, error)
		// Sync syncs the data files of changed volumes.
		Sync() error
	}

	// A ChainManager provides access to the current state of the blockchain.
	ChainManager interface {
		TipState() consensus.State
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		Address() types.Address
		FundTransaction(txn *types.Transaction, amount types.Currency) ([]types.Hash256, func(), error)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		RecommendedFee() types.Currency
	}

	// A SettingsReporter reports the host's current configuration.
	SettingsReporter interface {
		DiscoveredRHP2Address() string
		Settings() settings.Settings
		BandwidthLimiters() (ingress, egress *rate.Limiter)
	}

	// SessionReporter reports session metrics
	SessionReporter interface {
		StartSession(conn *rhp.Conn, proto string, version int) (sessionID rhp.UID, end func())
		StartRPC(sessionID rhp.UID, rpc types.Specifier) (rpcID rhp.UID, end func(contracts.Usage, error))
	}

	// A SessionHandler handles the host side of the renter-host protocol and
	// manages renter sessions
	SessionHandler struct {
		privateKey types.PrivateKey
		rhp3Port   string

		listener net.Listener
		monitor  rhp.DataMonitor
		tg       *threadgroup.ThreadGroup

		cm     ChainManager
		tpool  TransactionPool
		wallet Wallet

		contracts ContractManager
		sessions  SessionReporter
		settings  SettingsReporter
		storage   StorageManager
		log       *zap.Logger
	}
)

func (sh *SessionHandler) rpcLoop(sess *session, log *zap.Logger) error {
	done, err := sh.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	id, err := sess.t.ReadID()
	if err != nil {
		return fmt.Errorf("failed to read RPC ID: %w", err)
	}

	rpcFn, ok := map[types.Specifier]func(*session, *zap.Logger) (contracts.Usage, error){
		rhp2.RPCFormContractID:       sh.rpcFormContract,
		rhp2.RPCRenewClearContractID: sh.rpcRenewAndClearContract,
		rhp2.RPCLockID:               sh.rpcLock,
		rhp2.RPCUnlockID:             sh.rpcUnlock,
		rhp2.RPCSectorRootsID:        sh.rpcSectorRoots,
		rhp2.RPCReadID:               sh.rpcRead,
		rhp2.RPCSettingsID:           sh.rpcSettings,
		rhp2.RPCWriteID:              sh.rpcWrite,
	}[id]
	if !ok {
		err = fmt.Errorf("unknown RPC ID %q", id)
		sess.t.WriteResponseErr(err)
		return err
	}
	start := time.Now()
	rpcID, end := sh.sessions.StartRPC(sess.id, id)
	log = log.Named(id.String()).With(zap.Stringer("rpcID", rpcID))
	log.Debug("RPC start")
	usage, err := rpcFn(sess, log)
	end(usage, err)
	if err != nil {
		log.Warn("RPC error", zap.Error(err), zap.Duration("elapsed", time.Since(start)))
		return fmt.Errorf("RPC %q error: %w", id, err)
	}
	log.Info("RPC success", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// upgrade performs the RHP2 handshake and begins handling RPCs
func (sh *SessionHandler) upgrade(conn net.Conn) error {
	// wrap the conn with the bandwidth limiters
	ingressLimiter, egressLimiter := sh.settings.BandwidthLimiters()
	rhpConn := rhp.NewConn(conn, sh.monitor, ingressLimiter, egressLimiter)

	t, err := rhp2.NewHostTransport(rhpConn, sh.privateKey)
	if err != nil {
		return err
	}

	sessionID, end := sh.sessions.StartSession(rhpConn, rhp.SessionProtocolTCP, 2)
	defer end()

	sess := &session{
		id:   sessionID,
		conn: rhpConn,
		t:    t,
	}
	defer t.Close()

	defer func() {
		if sess.contract.Revision.ParentID != (types.FileContractID{}) {
			sh.contracts.Unlock(sess.contract.Revision.ParentID)
		}
	}()

	log := sh.log.With(zap.Stringer("sessionID", sessionID), zap.Stringer("peerAddr", conn.RemoteAddr()))

	for {
		if err := sh.rpcLoop(sess, log); err != nil {
			return err
		}
	}
}

// Close closes the listener and stops accepting new connections
func (sh *SessionHandler) Close() error {
	sh.tg.Stop()
	return sh.listener.Close()
}

// Settings returns the host's current settings
func (sh *SessionHandler) Settings() (rhp2.HostSettings, error) {
	settings := sh.settings.Settings()
	usedSectors, totalSectors, err := sh.storage.Usage()
	if err != nil {
		return rhp2.HostSettings{}, fmt.Errorf("failed to get storage usage: %w", err)
	}

	netaddr := settings.NetAddress
	if len(netaddr) == 0 {
		netaddr = sh.settings.DiscoveredRHP2Address()
	}
	// if the net address is still empty, return an error
	if len(netaddr) == 0 {
		return rhp2.HostSettings{}, errors.New("no net address found")
	}

	return rhp2.HostSettings{
		// protocol version
		Version: Version,

		// host info
		Address:          sh.wallet.Address(),
		SiaMuxPort:       sh.rhp3Port,
		NetAddress:       netaddr,
		TotalStorage:     totalSectors * rhp2.SectorSize,
		RemainingStorage: (totalSectors - usedSectors) * rhp2.SectorSize,

		// network defaults
		MaxDownloadBatchSize: defaultBatchSize,
		MaxReviseBatchSize:   defaultBatchSize,
		SectorSize:           rhp2.SectorSize,
		WindowSize:           settings.WindowSize,

		// contract formation
		AcceptingContracts: settings.AcceptingContracts,
		MaxDuration:        settings.MaxContractDuration,
		ContractPrice:      settings.ContractPrice,

		// rpc prices
		BaseRPCPrice:           settings.BaseRPCPrice,
		SectorAccessPrice:      settings.SectorAccessPrice,
		Collateral:             settings.StoragePrice.Mul64(uint64(settings.CollateralMultiplier * 1000)).Div64(1000),
		MaxCollateral:          settings.MaxCollateral,
		StoragePrice:           settings.StoragePrice,
		DownloadBandwidthPrice: settings.EgressPrice,
		UploadBandwidthPrice:   settings.IngressPrice,

		// ea settings
		MaxEphemeralAccountBalance: settings.MaxAccountBalance,
		EphemeralAccountExpiry:     settings.AccountExpiry,

		RevisionNumber: settings.Revision,
	}, nil
}

// Serve starts listening for new connections and blocks until closed
func (sh *SessionHandler) Serve() error {
	for {
		conn, err := sh.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}
		go func() {
			defer conn.Close()
			if err := sh.upgrade(conn); err != nil {
				if errors.Is(err, rhp2.ErrRenterClosed) || errors.Is(err, io.EOF) {
					// skip logging graceful close and EOF errors
					return
				}
				sh.log.Debug("failed to upgrade connection", zap.Error(err), zap.String("remoteAddr", conn.RemoteAddr().String()))
			}
		}()
	}
}

// LocalAddr returns the listener's listen address
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new RHP2 SessionHandler
func NewSessionHandler(l net.Listener, hostKey types.PrivateKey, rhp3Addr string, cm ChainManager, tpool TransactionPool, wallet Wallet, contracts ContractManager, settings SettingsReporter, storage StorageManager, monitor rhp.DataMonitor, sessions SessionReporter, log *zap.Logger) (*SessionHandler, error) {
	_, rhp3Port, err := net.SplitHostPort(rhp3Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rhp3 addr: %w", err)
	}

	sh := &SessionHandler{
		privateKey: hostKey,
		tg:         threadgroup.New(),
		rhp3Port:   rhp3Port,

		listener: l,
		monitor:  monitor,
		cm:       cm,
		tpool:    tpool,
		wallet:   wallet,

		contracts: contracts,
		sessions:  sessions,
		settings:  settings,
		storage:   storage,
		log:       log,
	}
	return sh, nil
}
