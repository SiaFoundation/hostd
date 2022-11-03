// Package rhp implements the host side of the Sia renter-host protocol version 2
package rhp

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/time/rate"
	"lukechampine.com/frand"
)

const (
	defaultBatchSize = 20 * (1 << 20) // 20 MiB

	// SectorSize is the size of a sector in bytes.
	SectorSize = 1 << 22 // 4 MiB

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
		Lock(id types.FileContractID, wait time.Duration) (contracts.SignedRevision, error)
		// Unlock unlocks the contract with the given ID.
		Unlock(id types.FileContractID)

		SectorRoots(id types.FileContractID) ([]crypto.Hash, error)
		SetRoots(id types.FileContractID, roots []crypto.Hash) error

		AddContract(revision contracts.SignedRevision, txnset []types.Transaction) error
		ReviseContract(revision types.FileContractRevision, renterSig, hostSig []byte) error
	}

	// A StorageManager manages the storage of sectors on disk.
	StorageManager interface {
		Usage() (used, total uint64, _ error)

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
		FeeEstimate() (min types.Currency, max types.Currency)
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

		consensus ConsensusSet
		tpool     TransactionPool
		wallet    Wallet

		contracts ContractManager
		metrics   MetricReporter
		settings  SettingsReporter
		storage   StorageManager
	}
)

// upgrade performs the RHP2 handshake and begins handling RPCs
func (sh *SessionHandler) upgrade(conn net.Conn) error {
	var req loopKeyExchangeRequest
	if err := req.readFrom(conn); err != nil {
		return fmt.Errorf("failed to read key exchange request: %w", err)
	}

	var supportsChaCha bool
	for _, c := range req.Ciphers {
		if c == cipherChaCha20Poly1305 {
			supportsChaCha = true
			break
		}
	}
	if !supportsChaCha {
		// note: ignore the write error since the connection will be closed
		(&loopKeyExchangeResponse{
			Cipher: cipherNoOverlap,
		}).writeTo(conn)
		return fmt.Errorf("renter does not support ChaCha20Poly1305")
	}
	xsk, xpk := crypto.GenerateX25519KeyPair()
	sigHash := blake2b.Sum256(append(append(make([]byte, 0, len(req.PublicKey)+len(xpk)), req.PublicKey[:]...), xpk[:]...))
	resp := &loopKeyExchangeResponse{
		Cipher:    cipherChaCha20Poly1305,
		PublicKey: xpk,
		Signature: ed25519.Sign(sh.privateKey, sigHash[:]),
	}
	if err := resp.writeTo(conn); err != nil {
		return fmt.Errorf("failed to write key exchange response: %w", err)
	}

	cipherKey := crypto.DeriveSharedSecret(xsk, req.PublicKey)
	aead, _ := chacha20poly1305.New(cipherKey[:]) // no error possible
	// wrap the conn with the bandwidth limiters
	ingressLimiter, egressLimiter := sh.settings.BandwidthLimiters()
	sess := &session{
		conn:      newRPCConn(conn, ingressLimiter, egressLimiter),
		aead:      aead,
		metrics:   sh.metrics,
		challenge: frand.Entropy128(),
	}
	defer sess.Close()

	recordEnd := sh.recordSessionStart(sess)
	defer recordEnd()
	defer func() {
		if sess.contract.Revision.ParentID != (types.FileContractID{}) {
			sh.contracts.Unlock(sess.contract.Revision.ParentID)
		}
	}()

	// hack: cast challenge to Specifier to make it a ProtocolObject
	if err := sess.writeMessage((*Specifier)(&sess.challenge)); err != nil {
		return fmt.Errorf("failed to write challenge: %w", err)
	}

	for {
		id, err := sess.ReadID()
		if errors.Is(err, ErrRenterClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to read RPC ID: %w", err)
		}

		var rpcFn func(*session) error
		switch id {
		case rpcFormContractID:
			rpcFn = sh.rpcFormContract
		case rpcRenewClearContractID:
			rpcFn = sh.rpcRenewAndClearContract
		case rpcLockID:
			rpcFn = sh.rpcLock
		case rpcUnlockID:
			rpcFn = sh.rpcUnlock
		case rpcSectorRootsID:
			rpcFn = sh.rpcSectorRoots
		case rpcReadID:
			rpcFn = sh.rpcRead
		case rpcSettingsID:
			rpcFn = sh.rpcSettings
		case rpcWriteID:
			rpcFn = sh.rpcWrite
		default:
			return sess.WriteError(fmt.Errorf("unknown RPC ID %q", id))
		}
		recordEnd := sh.recordRPC(id, sess)
		err = rpcFn(sess)
		recordEnd(err)
		if err != nil {
			return fmt.Errorf("RPC %q error: %w", id, err)
		}
	}
}

// Close closes the listener and stops accepting new connections
func (sh *SessionHandler) Close() error {
	return sh.listener.Close()
}

// Settings returns the host's current settings
func (sh *SessionHandler) Settings() (HostSettings, error) {
	settings, err := sh.settings.Settings()
	if err != nil {
		return HostSettings{}, fmt.Errorf("failed to get host settings: %w", err)
	}
	used, total, err := sh.storage.Usage()
	if err != nil {
		return HostSettings{}, fmt.Errorf("failed to get storage usage: %w", err)
	}
	return HostSettings{
		// protocol version
		Version: Version,

		// host info
		UnlockHash:       sh.wallet.Address(),
		NetAddress:       settings.NetAddress,
		TotalStorage:     total,
		RemainingStorage: total - used,

		// network defaults
		MaxDownloadBatchSize: defaultBatchSize,
		MaxReviseBatchSize:   defaultBatchSize,
		SectorSize:           SectorSize,
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
			ingress, egress := sh.settings.BandwidthLimiters()
			if err := sh.upgrade(newRPCConn(conn, ingress, egress)); err != nil {
				log.Printf("failed to upgrade connection: %v", err)
			}
			conn.Close()
		}()
	}
}

// LocalAddr returns the listener's listen address
func (sh *SessionHandler) LocalAddr() string {
	return sh.listener.Addr().String()
}

// NewSessionHandler creates a new RHP2 SessionHandler
func NewSessionHandler(hostKey ed25519.PrivateKey, addr string, cs ConsensusSet, tpool TransactionPool, wallet Wallet, contracts ContractManager, settings SettingsReporter, storage StorageManager, metrics MetricReporter) (*SessionHandler, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on addr: %w", err)
	}
	sh := &SessionHandler{
		privateKey: hostKey,

		listener:  l,
		consensus: cs,
		tpool:     tpool,
		wallet:    wallet,

		contracts: contracts,
		metrics:   metrics,
		settings:  settings,
		storage:   storage,
	}
	return sh, nil
}
