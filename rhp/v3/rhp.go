package rhp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp2 "go.sia.tech/core/rhp/v2"
	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/accounts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/internal/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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

	// Sectors reads and writes sectors to persistent storage.
	Sectors interface {
		// HasSector returns true if the sector with the given root is stored
		HasSector(root types.Hash256) (bool, error)
		// Write writes a sector to persistent storage.
		Write(root types.Hash256, data *[rhp2.SectorSize]byte) error
		// ReadSector reads the sector with the given root from the manager.
		ReadSector(root types.Hash256) (*[rhp2.SectorSize]byte, error)
		// Sync syncs the data files of changed volumes.
		Sync() error

		// AddTemporarySectors adds the given sectors to the storage manager.
		// as temporary sectors. Temporary sectors are short-lived sectors not
		// associated with a contract.
		AddTemporarySectors([]storage.TempSector) error
	}

	// A RegistryManager manages registry entries stored in a RegistryStore.
	RegistryManager interface {
		Get(key rhp3.RegistryKey) (rhp3.RegistryValue, error)
		Put(value rhp3.RegistryEntry, expirationHeight uint64) (rhp3.RegistryValue, error)
		Entries() (count uint64, max uint64, err error)
	}

	// A ChainManager provides access to the current state of the blockchain.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		AddPoolTransactions([]types.Transaction) (known bool, err error)
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
		RecommendedFee() types.Currency
	}

	// A Syncer broadcasts transactions to the network
	Syncer interface {
		BroadcastTransactionSet([]types.Transaction)
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction)
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		Address() types.Address
		FundTransaction(txn *types.Transaction, amount types.Currency, unconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		ReleaseInputs(txn []types.Transaction, v2txn []types.V2Transaction)
	}

	// A SettingsReporter reports the host's current configuration.
	SettingsReporter interface {
		AcceptingContracts() bool
		RHP2Settings() (rhp2.HostSettings, error)
		RHP3PriceTable() (rhp3.HostPriceTable, error)
	}

	// A SessionHandler handles the host side of the renter-host protocol and
	// manages renter sessions
	SessionHandler struct {
		privateKey types.PrivateKey

		listener net.Listener

		accounts  AccountManager
		contracts ContractManager
		registry  RegistryManager
		sectors   Sectors
		settings  SettingsReporter

		chain  ChainManager
		syncer Syncer
		wallet Wallet

		log *zap.Logger
		tg  *threadgroup.ThreadGroup

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
func (sh *SessionHandler) handleHostStream(s *rhp3.Stream, log *zap.Logger) {
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

	rpcID := hex.EncodeToString(frand.Bytes(8))
	log = log.Named(rpc.String()).With(zap.String("rpcID", rpcID))
	if _, err := rpcFn(s, log); err != nil {
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

			// initiate the session
			sessionID := hex.EncodeToString(frand.Bytes(8))
			log := sh.log.With(zap.String("sessionID", sessionID), zap.String("peerAddress", conn.RemoteAddr().String()))

			// upgrade the connection to RHP3
			t, err := rhp3.NewHostTransport(conn, sh.privateKey)
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

				cs := sh.chain.TipState()
				// disable rhp3 after v2 require height
				if cs.Index.Height >= cs.Network.HardforkV2.RequireHeight {
					stream.WriteResponseErr(ErrV2Hardfork)
					return
				}

				go sh.handleHostStream(stream, log)
			}
		}()
	}
}

// LocalAddr returns the address the host is listening on.
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new SessionHandler
func NewSessionHandler(l net.Listener, hostKey types.PrivateKey, chain ChainManager, syncer Syncer, wallet Wallet, accounts AccountManager, contracts ContractManager, registry RegistryManager, sectors Sectors, settings SettingsReporter, log *zap.Logger) *SessionHandler {
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,

		chain:  chain,
		syncer: syncer,
		wallet: wallet,

		accounts:  accounts,
		contracts: contracts,
		registry:  registry,
		settings:  settings,
		sectors:   sectors,

		log: log,
		tg:  threadgroup.New(),

		priceTables: newPriceTableManager(),
	}
	return sh
}
