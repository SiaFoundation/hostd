package rhp

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	"go.sia.tech/hostd/rhp"
	"golang.org/x/time/rate"
)

const (
	// Version is the current version of the RHP3 protocol.
	Version = "2.0.0"
)

type (
	// An AccountManager manages deposits and withdrawals for accounts.
	AccountManager interface {
		Balance(accountID rhpv3.Account) (types.Currency, error)
		Credit(accountID rhpv3.Account, amount types.Currency) (balance types.Currency, err error)
		Budget(ctx context.Context, accountID rhpv3.Account, amount types.Currency) (accounts.Budget, error)
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

		SectorRoots(id types.FileContractID) ([]types.Hash256, error)
		SetRoots(id types.FileContractID, roots []types.Hash256) error

		AddContract(revision contracts.SignedRevision, txnset []types.Transaction) error
		ReviseContract(revision types.FileContractRevision, renterSig, hostSig types.Signature) error
	}

	// A RegistryManager manages registry entries stored in a RegistryStore.
	RegistryManager interface {
		Cap() uint64
		Len() uint64

		Get(key types.Hash256) (rhpv3.RegistryValue, error)
		Put(value rhpv3.RegistryValue, expirationHeight uint64) (rhpv3.RegistryValue, error)
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

		// HasSector returns true if the sector is stored on disk.
		HasSector(types.Hash256) (bool, error)
		// AddSector adds a sector to the storage manager.
		AddSector(root types.Hash256, sector *[rhpv2.SectorSize]byte, refs int) error
		// DeleteSector deletes the sector with the given root.
		DeleteSector(root types.Hash256, refs int) error
		// Sector reads a sector from the store
		Sector(root types.Hash256) (*[rhpv2.SectorSize]byte, error)
	}

	// A ChainManager provides access to the current state of the blockchain.
	ChainManager interface {
		Tip() consensus.State
	}

	// A Wallet manages funds and signs transactions
	Wallet interface {
		Address() types.Address
		FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.Hash256, func(), error)
		SignTransaction(*types.Transaction, []types.Hash256, types.CoveredFields) error
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

		accounts  AccountManager
		contracts ContractManager
		metrics   MetricReporter
		registry  RegistryManager
		storage   StorageManager

		chain    ChainManager
		settings SettingsReporter
		tpool    TransactionPool
		wallet   Wallet

		priceTables *priceTableManager
	}
)

// handleHostStream handles streams routed to the "host" subscriber
func (sh *SessionHandler) handleHostStream(s *rhpv3.Stream) {
	s.SetDeadline(time.Now().Add(30 * time.Second))
	rpcID, err := s.ReadID()
	s.SetDeadline(time.Time{})
	if err != nil {
		log.Println("failed to read RPC ID:", err)
		return
	}

	switch rpcID {
	case rhpv3.RPCAccountBalanceID:
		err = sh.handleRPCAccountBalance(s)
	case rhpv3.RPCUpdatePriceTableID:
		err = sh.handleRPCPriceTable(s)
	case rhpv3.RPCExecuteProgramID:
		// err = sh.handleRPCExecute(s)
	case rhpv3.RPCFundAccountID:
		err = sh.handleRPCFundAccount(s)
	case rhpv3.RPCLatestRevisionID:
		err = sh.handleRPCLatestRevision(s)
	case rhpv3.RPCRegistrySubscriptionID:
	case rhpv3.RPCFormContractID:
	case rhpv3.RPCRenewContractID:
	default:
		err = fmt.Errorf("unrecognized RPC ID, %q", rpcID)
	}
	if err != nil {
		log.Printf("error handling RPC %q: %v", rpcID, err)
	}
}

// HostKey returns the host's ed25519 public key
func (sh *SessionHandler) HostKey() types.UnlockKey {
	return sh.privateKey.PublicKey().UnlockKey()
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
		t, err := rhpv3.NewHostTransport(rhp.NewConn(conn, ingress, egress), sh.privateKey)
		if err != nil {
			return fmt.Errorf("failed to upgrade conn: %w", err)
		}

		go func() {
			defer t.Close()

			for {
				stream, err := t.AcceptStream()
				if err != nil {
					log.Println("failed to accept stream:", err)
					return
				}
				go sh.handleHostStream(stream)
			}
		}()
	}
}

// LocalAddr returns the address the host is listening on.
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new SessionHandler
func NewSessionHandler(hostKey types.PrivateKey, addr string, chain ChainManager, tpool TransactionPool, wallet Wallet, accounts AccountManager, contracts ContractManager, registry RegistryManager, storage StorageManager, settings SettingsReporter, metrics MetricReporter) (*SessionHandler, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr: %w", err)
	}
	sh := &SessionHandler{
		privateKey: hostKey,

		listener: l,

		chain:  chain,
		tpool:  tpool,
		wallet: wallet,

		accounts:  accounts,
		contracts: contracts,
		metrics:   metrics,
		registry:  registry,
		settings:  settings,
		storage:   storage,

		priceTables: newPriceTableManager(),
	}
	return sh, nil
}
