package rhp

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"time"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/registry"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/mux"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/time/rate"
	"lukechampine.com/frand"
)

const (
	// SectorSize is the size of a sector in bytes.
	SectorSize = 1 << 22 // 4 MiB

	// Version is the current version of the RHP3 protocol.
	Version = "2.0.0"
)

type (
	// An AccountManager manages deposits and withdrawals for accounts.
	AccountManager interface {
		Balance(accountID accounts.AccountID) (types.Currency, error)
		Credit(accountID accounts.AccountID, amount types.Currency) (balance types.Currency, err error)
		Budget(ctx context.Context, accountID accounts.AccountID, amount types.Currency) (accounts.Budget, error)
	}

	// A ContractManager manages the set of contracts that the host is currently
	// storing data for
	ContractManager interface {
		// Lock locks the contract with the given ID. Will wait for the given
		// duration before giving up. Unlock must be called to unlock the
		// contract.
		Lock(id types.FileContractID, wait time.Duration) (contracts.SignedRevision, error)
		// Unlock unlocks the contract with the given ID.
		Unlock(id types.FileContractID)

		SectorRoots(id types.FileContractID) ([]crypto.Hash, error)
		SetRoots(id types.FileContractID, roots []crypto.Hash) error

		AddContract(revision contracts.SignedRevision, txnset []types.Transaction) error
		ReviseContract(revision types.FileContractRevision, renterSig, hostSig []byte) error
	}

	// A RegistryManager manages registry entries stored in a RegistryStore.
	RegistryManager interface {
		Cap() uint64
		Len() uint64

		Get(key crypto.Hash) (registry.Value, error)
		Put(value registry.Value, expirationHeight uint64) (registry.Value, error)
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

		// HasSector returns true if the sector is stored on disk.
		HasSector(crypto.Hash) (bool, error)
		// AddSector adds a sector to the storage manager.
		AddSector(root crypto.Hash, sector []byte, refs int) error
		// DeleteSector deletes the sector with the given root.
		DeleteSector(root crypto.Hash, refs int) error
		// Sector reads a sector from the store
		Sector(root crypto.Hash) ([]byte, error)
	}

	// ConsensusSet contains the subset of the consensus set that the host
	// needs.
	ConsensusSet interface {
		Height() types.BlockHeight
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		Address() types.UnlockHash
		FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.SiacoinOutputID, func(), error)
		SignTransaction(*types.Transaction, []types.SiacoinOutputID) error
	}

	// A TransactionPool broadcasts transactions to the network.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		FeeEstimation() (min types.Currency, max types.Currency)
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
		privateKey ed25519.PrivateKey

		listener net.Listener
		router   *mux.SubscriberRouter

		accounts  AccountManager
		contracts ContractManager
		metrics   MetricReporter
		registry  RegistryManager
		storage   StorageManager

		consensus ConsensusSet
		settings  SettingsReporter
		tpool     TransactionPool
		wallet    Wallet

		priceTables *priceTableManager
	}
)

// handleHostStream handles streams routed to the "host" subscriber
func (sh *SessionHandler) handleHostStream(_ string, stream *mux.SubscriberStream) {
	sess := &rpcSession{
		stream: stream,
	}
	var rpcID Specifier
	if err := sess.ReadObject(&rpcID, 16, 30*time.Second); err != nil {
		log.Println("failed to read RPC ID:", err)
		return
	}

	log.Printf("handling %v", rpcID)

	var err error
	switch rpcID {
	case rpcAccountBalance:
		err = sh.handleRPCAccountBalance(sess)
	case rpcUpdatePriceTable:
		err = sh.handleRPCPriceTable(sess)
	case rpcExecuteProgram:
		// err = sh.handleRPCExecute(sess)
	case rpcFundAccount:
		err = sh.handleRPCFundAccount(sess)
	case rpcLatestRevision:
		err = sh.handleRPCLatestRevision(sess)
	case rpcRegistrySubscription:
	case rpcFormContract:
	case rpcRenewContract:
	default:
		err = fmt.Errorf("unrecognized RPC ID, %q", rpcID)
	}
	if err != nil {
		log.Printf("error handling RPC %q: %v", rpcID, err)
	}
}

// HostKey returns the host's ed25519 public key
func (sh *SessionHandler) HostKey() types.SiaPublicKey {
	pub := (*[32]byte)(sh.privateKey.Public().(ed25519.PublicKey))
	return types.Ed25519PublicKey(*pub)
}

// Close closes the session handler and stops accepting new connections.
func (sh *SessionHandler) Close() error {
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
		ingress, egress := sh.settings.BandwidthLimiters()
		go func() {
			if err := sh.router.Upgrade(newRPCConn(conn, ingress, egress)); err != nil {
				log.Println("failed to upgrade connection:", err)
			}
			conn.Close()
		}()
	}
}

// LocalAddr returns the address the host is listening on.
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new SessionHandler
func NewSessionHandler(hostKey ed25519.PrivateKey, addr string, cs ConsensusSet, tpool TransactionPool, wallet Wallet, accounts AccountManager, contracts ContractManager, registry RegistryManager, storage StorageManager, settings SettingsReporter, metrics MetricReporter) (*SessionHandler, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr: %w", err)
	}
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,
		router:   mux.NewSubscriberRouter(frand.Uint64n(math.MaxUint64), hostKey),

		consensus: cs,
		tpool:     tpool,
		wallet:    wallet,

		accounts:  accounts,
		contracts: contracts,
		metrics:   metrics,
		registry:  registry,
		settings:  settings,
		storage:   storage,

		priceTables: newPriceTableManager(),
	}
	sh.router.RegisterSubscriber("host", sh.handleHostStream)
	return sh, nil
}
