package rhp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/threadgroup"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type (
	// An AccountManager manages deposits and withdrawals for accounts.
	AccountManager interface {
		Balance(accountID rhp3.Account) (types.Currency, error)
		Credit(req accounts.FundAccountWithContract, refund bool) (balance types.Currency, err error)
		Budget(accountID rhp3.Account, amount types.Currency) (*accounts.Budget, error)
	}

	// A ContractManager manages the set of contracts that the host is currently
	// storing data for
	ContractManager interface {
		// Contract returns the last revision of the contract with the given ID.
		Contract(id types.FileContractID) (contracts.Contract, error)

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
		Write(root types.Hash256, data *[rhp2.SectorSize]byte) (release func() error, _ error)
		// Read reads the sector with the given root from the manager.
		Read(root types.Hash256) (*[rhp2.SectorSize]byte, error)
		// Sync syncs the data files of changed volumes.
		Sync() error

		// AddTemporarySectors adds the given sectors to the storage manager.
		// as temporary sectors. Temporary sectors are short-lived sectors not
		// associated with a contract.
		AddTemporarySectors([]storage.TempSector) error
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

	// SessionReporter reports session metrics
	SessionReporter interface {
		StartSession(conn *rhp.Conn, proto string, version int) (sessionID rhp.UID, end func())
		StartRPC(sessionID rhp.UID, rpc types.Specifier) (rpcID rhp.UID, end func(contracts.Usage, error))
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
		sessions  SessionReporter
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
func (sh *SessionHandler) handleHostStream(s *rhp3.Stream, sessionID rhp.UID, log *zap.Logger) {
	defer s.Close() // close the stream when the RPC has completed

	done, err := sh.tg.Add() // add the RPC to the threadgroup
	if err != nil {
		return
	}
	defer done()

	s.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	rpc, err := s.ReadID()
	if err != nil {
		log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}
	rpcs := map[types.Specifier]func(*rhp3.Stream, *zap.Logger) (contracts.Usage, error){
		rhp3.RPCAccountBalanceID:   sh.handleRPCAccountBalance,
		rhp3.RPCUpdatePriceTableID: sh.handleRPCPriceTable,
		rhp3.RPCExecuteProgramID:   sh.handleRPCExecute,
		rhp3.RPCFundAccountID:      sh.handleRPCFundAccount,
		rhp3.RPCLatestRevisionID:   sh.handleRPCLatestRevision,
		rhp3.RPCRenewContractID:    sh.handleRPCRenew,
	}
	rpcFn, ok := rpcs[rpc]
	if !ok {
		log.Debug("unrecognized RPC ID", zap.String("rpc", rpc.String()))
		return
	}

	rpcStart := time.Now()
	s.SetDeadline(time.Now().Add(time.Minute)) // set the initial deadline, may be overwritten by the handler

	rpcID, end := sh.sessions.StartRPC(sessionID, rpc)
	log = log.Named(rpc.String()).With(zap.Stringer("rpcID", rpcID))
	usage, err := rpcFn(s, log)
	end(usage, err)
	if err != nil {
		log.Warn("RPC failed", zap.Error(err), zap.Duration("elapsed", time.Since(rpcStart)))
		return
	}
	log.Info("RPC success", zap.Duration("elapsed", time.Since(rpcStart)))
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

			// wrap the conn with the bandwidth limiters
			ingress, egress := sh.settings.BandwidthLimiters()
			rhpConn := rhp.NewConn(conn, sh.monitor, ingress, egress)
			defer rhpConn.Close()

			// initiate the session
			sessionID, end := sh.sessions.StartSession(rhpConn, rhp.SessionProtocolTCP, 3)
			defer end()

			log := sh.log.With(zap.Stringer("sessionID", sessionID), zap.String("peerAddress", conn.RemoteAddr().String()))

			// upgrade the connection to RHP3
			t, err := rhp3.NewHostTransport(rhpConn, sh.privateKey)
			if err != nil {
				log.Debug("failed to upgrade conn", zap.Error(err))
				return
			}
			defer t.Close()

			for {
				stream, err := t.AcceptStream()
				if err != nil {
					if !isStreamClosedErr(err) {
						log.Debug("failed to accept stream", zap.Error(err))
					}
					return
				}

				go sh.handleHostStream(stream, sessionID, log)
			}
		}()
	}
}

// LocalAddr returns the address the host is listening on.
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new SessionHandler
func NewSessionHandler(l net.Listener, hostKey types.PrivateKey, chain ChainManager, tpool TransactionPool, wallet Wallet, accounts AccountManager, contracts ContractManager, storage StorageManager, settings SettingsReporter, monitor rhp.DataMonitor, sessions SessionReporter, log *zap.Logger) (*SessionHandler, error) {
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
		sessions:  sessions,
		settings:  settings,
		storage:   storage,
		log:       log,

		priceTables: newPriceTableManager(),
	}
	return sh, nil
}
