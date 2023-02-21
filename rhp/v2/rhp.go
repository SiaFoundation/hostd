package rhp

import (
	"context"
	"errors"
	"fmt"
	"net"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	defaultBatchSize = 20 * (1 << 20) // 20 MiB

	// Version is the current version of the RHP2 protocol.
	Version = "2.0.0"
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
		AddContract(revision contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error
		// RenewContract renews an existing contract.
		RenewContract(renewal contracts.SignedRevision, existing contracts.SignedRevision, formationSet []types.Transaction, lockedCollateral types.Currency, negotationHeight uint64) error
		// ReviseContract atomically revises a contract and its sector roots
		ReviseContract(contractID types.FileContractID) (*contracts.ContractUpdater, error)

		// SectorRoots returns the sector roots of the contract with the given ID.
		SectorRoots(id types.FileContractID, limit, offset uint64) ([]types.Hash256, error)
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

		// Write writes a sector to persistent storage. release should only be
		// called after the contract roots have been committed to prevent the
		// sector from being deleted.
		Write(root types.Hash256, data *[rhpv2.SectorSize]byte) (release func() error, _ error)
		// Read reads the sector with the given root from the manager.
		Read(root types.Hash256) (*[rhpv2.SectorSize]byte, error)
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
		Settings() (settings.Settings, error)
		BandwidthLimiters() (ingress, egress *rate.Limiter)
	}

	// MetricReporter records metrics from the host
	MetricReporter interface {
		Report(any) error
	}

	// A FinancialReporter records financial transactions on the host.
	FinancialReporter interface {
		Add(financials.Record) error
	}

	// A SessionHandler handles the host side of the renter-host protocol and
	// manages renter sessions
	SessionHandler struct {
		privateKey types.PrivateKey

		listener net.Listener

		cm     ChainManager
		tpool  TransactionPool
		wallet Wallet

		contracts ContractManager
		metrics   MetricReporter
		settings  SettingsReporter
		storage   StorageManager
		log       *zap.Logger
	}
)

// upgrade performs the RHP2 handshake and begins handling RPCs
func (sh *SessionHandler) upgrade(conn net.Conn) error {
	// wrap the conn with the bandwidth limiters
	ingressLimiter, egressLimiter := sh.settings.BandwidthLimiters()
	conn = rhp.NewConn(conn, ingressLimiter, egressLimiter)

	t, err := rhpv2.NewHostTransport(conn, sh.privateKey)
	if err != nil {
		return err
	}

	sess := &session{
		conn:    conn.(*rhp.Conn),
		t:       t,
		metrics: sh.metrics,
	}
	defer t.Close()

	recordEnd := sh.recordSessionStart(sess)
	defer recordEnd()
	defer func() {
		if sess.contract.Revision.ParentID != (types.FileContractID{}) {
			sh.contracts.Unlock(sess.contract.Revision.ParentID)
		}
	}()

	for {
		id, err := t.ReadID()
		if errors.Is(err, rhpv2.ErrRenterClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to read RPC ID: %w", err)
		}

		var rpcFn func(*session) error
		rpcFn, ok := map[types.Specifier]func(*session) error{
			rhpv2.RPCFormContractID:       sh.rpcFormContract,
			rhpv2.RPCRenewClearContractID: sh.rpcRenewAndClearContract,
			rhpv2.RPCLockID:               sh.rpcLock,
			rhpv2.RPCUnlockID:             sh.rpcUnlock,
			rhpv2.RPCSectorRootsID:        sh.rpcSectorRoots,
			rhpv2.RPCReadID:               sh.rpcRead,
			rhpv2.RPCSettingsID:           sh.rpcSettings,
			rhpv2.RPCWriteID:              sh.rpcWrite,
		}[id]
		if !ok {
			return t.WriteResponseErr(fmt.Errorf("unknown RPC ID %q", id))
		}
		recordEnd := sh.recordRPC(id, sess)
		err = rpcFn(sess)
		recordEnd(err)
		if err != nil {
			sh.log.Warn("RPC error", zap.Stringer("RPC", id), zap.Error(err), zap.String("remote", conn.RemoteAddr().String()))
			return fmt.Errorf("RPC %q error: %w", id, err)
		}
	}
}

// Close closes the listener and stops accepting new connections
func (sh *SessionHandler) Close() error {
	return sh.listener.Close()
}

// Settings returns the host's current settings
func (sh *SessionHandler) Settings() (rhpv2.HostSettings, error) {
	settings, err := sh.settings.Settings()
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to get host settings: %w", err)
	}
	used, total, err := sh.storage.Usage()
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to get storage usage: %w", err)
	}
	return rhpv2.HostSettings{
		// protocol version
		Version: Version,

		// host info
		Address:          sh.wallet.Address(),
		NetAddress:       settings.NetAddress,
		TotalStorage:     total,
		RemainingStorage: total - used,

		// network defaults
		MaxDownloadBatchSize: defaultBatchSize,
		MaxReviseBatchSize:   defaultBatchSize,
		SectorSize:           rhpv2.SectorSize,
		WindowSize:           144,

		// contract formation
		AcceptingContracts: settings.AcceptingContracts,
		MaxDuration:        settings.MaxContractDuration,
		ContractPrice:      settings.ContractPrice,

		// rpc prices
		BaseRPCPrice:           settings.BaseRPCPrice,
		SectorAccessPrice:      settings.SectorAccessPrice,
		Collateral:             settings.Collateral,
		MaxCollateral:          settings.MaxCollateral,
		StoragePrice:           settings.MinStoragePrice,
		DownloadBandwidthPrice: settings.MinIngressPrice,
		UploadBandwidthPrice:   settings.MinEgressPrice,

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

			ingress, egress := sh.settings.BandwidthLimiters()
			if err := sh.upgrade(rhp.NewConn(conn, ingress, egress)); err != nil {
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
func NewSessionHandler(hostKey types.PrivateKey, addr string, cm ChainManager, tpool TransactionPool, wallet Wallet, contracts ContractManager, settings SettingsReporter, storage StorageManager, metrics MetricReporter, log *zap.Logger) (*SessionHandler, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr: %w", err)
	}
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,
		cm:       cm,
		tpool:    tpool,
		wallet:   wallet,

		contracts: contracts,
		metrics:   metrics,
		settings:  settings,
		storage:   storage,
		log:       log,
	}
	return sh, nil
}
