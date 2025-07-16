package rhp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// DefaultBatchSize is the maximum size in bytes of an RPC read and write request
	DefaultBatchSize = 20 * (1 << 20) // 20 MiB

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
		SectorRoots(id types.FileContractID) []types.Hash256
	}

	// Sectors reads and writes sectors to persistent storage
	Sectors interface {
		// Write writes a sector to persistent storage
		Write(root types.Hash256, data *[proto4.SectorSize]byte) error
		// ReadSector reads the sector with the given root from the manager.
		ReadSector(root types.Hash256) (*[proto4.SectorSize]byte, error)
		// Sync syncs the data files of changed volumes.
		Sync() error
	}

	// A ChainManager provides access to the current state of the blockchain.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		AddPoolTransactions([]types.Transaction) (known bool, err error)
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
	}

	// A Syncer broadcasts transactions to the network
	Syncer interface {
		BroadcastTransactionSet([]types.Transaction) error
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		Address() types.Address
		FundTransaction(txn *types.Transaction, amount types.Currency, unconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		ReleaseInputs(txn []types.Transaction, v2txn []types.V2Transaction) error
	}

	// A SettingsReporter reports the host's current configuration.
	SettingsReporter interface {
		RHP2Settings() (rhp2.HostSettings, error)
	}

	// A SessionHandler handles the host side of the renter-host protocol and
	// manages renter sessions
	SessionHandler struct {
		privateKey types.PrivateKey

		listener net.Listener
		tg       *threadgroup.ThreadGroup

		chain  ChainManager
		syncer Syncer
		wallet Wallet

		contracts ContractManager

		settings SettingsReporter
		sectors  Sectors
		log      *zap.Logger
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

	cs := sh.chain.TipState()
	// disable rhp2 after v2 require height
	if cs.Index.Height >= cs.Network.HardforkV2.RequireHeight {
		sess.t.WriteResponseErr(ErrV2Hardfork)
		return ErrV2Hardfork
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
	rpcID := hex.EncodeToString(frand.Bytes(8))
	log = log.Named(id.String()).With(zap.String("rpcID", rpcID))
	log.Debug("RPC start")
	if _, err := rpcFn(sess, log); err != nil {
		log.Warn("RPC error", zap.Error(err), zap.Duration("elapsed", time.Since(start)))
		return fmt.Errorf("RPC %q error: %w", id, err)
	}
	log.Info("RPC success", zap.Duration("elapsed", time.Since(start)))
	return nil
}

// upgrade performs the RHP2 handshake and begins handling RPCs
func (sh *SessionHandler) upgrade(conn net.Conn) error {
	t, err := rhp2.NewHostTransport(conn, sh.privateKey)
	if err != nil {
		return err
	}

	sessionID := hex.EncodeToString(frand.Bytes(8))
	sess := &session{
		t: t,
	}
	defer t.Close()

	defer func() {
		if sess.contract.Revision.ParentID != (types.FileContractID{}) {
			sh.contracts.Unlock(sess.contract.Revision.ParentID)
		}
	}()

	log := sh.log.With(zap.String("sessionID", sessionID), zap.Stringer("peerAddr", conn.RemoteAddr()))

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
func NewSessionHandler(l net.Listener, hostKey types.PrivateKey, cm ChainManager, s Syncer, wallet Wallet, contracts ContractManager, settings SettingsReporter, sectors Sectors, log *zap.Logger) *SessionHandler {
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,
		chain:    cm,
		syncer:   s,
		wallet:   wallet,

		contracts: contracts,
		settings:  settings,
		sectors:   sectors,

		log: log,

		tg: threadgroup.New(),
	}
	return sh
}
