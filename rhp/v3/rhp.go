package rhp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// Version is the current version of the RHP3 protocol.
	Version = "1.6.0"
)

type (
	// An AccountManager manages deposits and withdrawals for accounts.
	AccountManager interface {
		Balance(accountID rhpv3.Account) (types.Currency, error)
		Credit(accountID rhpv3.Account, amount types.Currency, expiration time.Time) (balance types.Currency, err error)
		Budget(ctx context.Context, accountID rhpv3.Account, amount types.Currency) (*accounts.Budget, error)
	}

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
		SectorRoots(id types.FileContractID, limit, offset uint64) ([]types.Hash256, error)
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

		// LockSector locks the sector with the given root. If the sector does not
		// exist, an error is returned. Release must be called when the sector is no
		// longer needed.
		LockSector(root types.Hash256) (func() error, error)
		// Write writes a sector to persistent storage. release should only be
		// called after the contract roots have been committed to prevent the
		// sector from being deleted.
		Write(root types.Hash256, data *[rhpv2.SectorSize]byte) (release func() error, _ error)
		// Read reads the sector with the given root from the manager.
		Read(root types.Hash256) (*[rhpv2.SectorSize]byte, error)
		// Sync syncs the data files of changed volumes.
		Sync() error

		// AddTemporarySectors adds the given sectors to the storage manager.
		// as temporary sectors. Temporary sectors are short-lived sectors not
		// associated with a contract.
		AddTemporarySectors([]storage.TempSector) error
	}

	// A RegistryManager manages registry entries stored in a RegistryStore.
	RegistryManager interface {
		Get(key rhpv3.RegistryKey) (rhpv3.RegistryValue, error)
		Put(value rhpv3.RegistryEntry, expirationHeight uint64) (rhpv3.RegistryValue, error)
		Entries() (count uint64, max uint64, err error)
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
		Settings() settings.Settings
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
		monitor  rhp.DataMonitor
		tg       *threadgroup.ThreadGroup

		accounts  AccountManager
		contracts ContractManager
		metrics   MetricReporter
		registry  RegistryManager
		storage   StorageManager
		log       *zap.Logger

		chain    ChainManager
		settings SettingsReporter
		tpool    TransactionPool
		wallet   Wallet

		priceTables *priceTableManager
	}
)

var (
	// ErrContractRevisionLimit is returned when a contract revision would
	// exceed the maximum revision number.
	ErrContractRevisionLimit = errors.New("max revision number reached")
	// ErrContractProofWindowStarted is returned when a contract revision is
	// attempted after the proof window has started.
	ErrContractProofWindowStarted = errors.New("proof window has started")
	// ErrContractExpired is returned when a contract revision is attempted
	// after the contract has expired.
	ErrContractExpired = errors.New("contract has expired")

	// ErrInvalidSectorLength is returned when a sector is not the correct
	// length.
	ErrInvalidSectorLength = errors.New("length of sector data must be exactly 4MiB")

	// ErrTrimOutOfBounds is returned when a trim operation exceeds the total
	// number of sectors
	ErrTrimOutOfBounds = errors.New("trim size exceeds number of sectors")
	// ErrSwapOutOfBounds is returned when one of the swap indices exceeds the
	// total number of sectors
	ErrSwapOutOfBounds = errors.New("swap index is out of bounds")
	// ErrUpdateOutOfBounds is returned when the update index exceeds the total
	// number of sectors
	ErrUpdateOutOfBounds = errors.New("update index is out of bounds")
	// ErrOffsetOutOfBounds is returned when the offset exceeds and length
	// exceed the sector size.
	ErrOffsetOutOfBounds = errors.New("update section is out of bounds")
	// ErrUpdateProofSize is returned when a proof is requested for an update
	// operation that is not a multiple of 64 bytes.
	ErrUpdateProofSize = errors.New("update section is not a multiple of the segment size")
)

// handleHostStream handles streams routed to the "host" subscriber
func (sh *SessionHandler) handleHostStream(remoteAddr string, s *rhpv3.Stream) {
	defer s.Close() // close the stream when the RPC has completed

	done, err := sh.tg.Add() // add the RPC to the threadgroup
	if err != nil {
		return
	}
	defer done()

	s.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	rpcID, err := s.ReadID()
	if err != nil {
		sh.log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}

	rpcs := map[types.Specifier]func(*rhpv3.Stream, *zap.Logger) error{
		rhpv3.RPCAccountBalanceID:   sh.handleRPCAccountBalance,
		rhpv3.RPCUpdatePriceTableID: sh.handleRPCPriceTable,
		rhpv3.RPCExecuteProgramID:   sh.handleRPCExecute,
		rhpv3.RPCFundAccountID:      sh.handleRPCFundAccount,
		rhpv3.RPCLatestRevisionID:   sh.handleRPCLatestRevision,
		rhpv3.RPCRenewContractID:    sh.handleRPCRenew,
	}
	rpcFn, ok := rpcs[rpcID]
	if !ok {
		sh.log.Debug("unrecognized RPC ID", zap.String("rpc", rpcID.String()))
		return
	}

	log := sh.log.Named(rpcID.String()).With(zap.String("peerAddr", remoteAddr))
	start := time.Now()
	s.SetDeadline(time.Now().Add(time.Minute)) // set the initial deadline, may be overwritten by the handler
	if err = rpcFn(s, log); err != nil {
		log.Warn("RPC failed", zap.Error(err), zap.Duration("elapsed", time.Since(start)))
		return
	}
	log.Debug("RPC success", zap.Duration("elapsed", time.Since(start)))
}

// HostKey returns the host's ed25519 public key
func (sh *SessionHandler) HostKey() types.UnlockKey {
	return sh.privateKey.PublicKey().UnlockKey()
}

// Close closes the session handler and stops accepting new connections.
func (sh *SessionHandler) Close() error {
	sh.tg.Stop()
	return sh.listener.Close()
}

// Serve starts the host RPC server.
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
			t, err := rhpv3.NewHostTransport(rhp.NewConn(conn, sh.monitor, ingress, egress), sh.privateKey)
			if err != nil {
				sh.log.Debug("failed to upgrade conn", zap.Error(err), zap.String("remoteAddress", conn.RemoteAddr().String()))
				return
			}
			defer t.Close()

			for {
				stream, err := t.AcceptStream()
				if err != nil {
					sh.log.Debug("failed to accept stream", zap.Error(err), zap.String("remoteAddress", conn.RemoteAddr().String()))
					return
				}
				go sh.handleHostStream(conn.RemoteAddr().String(), stream)
			}
		}()
	}
}

// LocalAddr returns the address the host is listening on.
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new SessionHandler
func NewSessionHandler(l net.Listener, hostKey types.PrivateKey, chain ChainManager, tpool TransactionPool, wallet Wallet, accounts AccountManager, contracts ContractManager, registry RegistryManager, storage StorageManager, settings SettingsReporter, monitor rhp.DataMonitor, metrics MetricReporter, log *zap.Logger) (*SessionHandler, error) {
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,
		monitor:  monitor,
		tg:       threadgroup.New(),

		chain:  chain,
		tpool:  tpool,
		wallet: wallet,

		accounts:  accounts,
		contracts: contracts,
		metrics:   metrics,
		registry:  registry,
		settings:  settings,
		storage:   storage,
		log:       log,

		priceTables: newPriceTableManager(),
	}
	return sh, nil
}
