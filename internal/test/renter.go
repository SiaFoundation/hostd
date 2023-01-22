package test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"unsafe"

	"go.sia.tech/hostd/consensus"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.sia.tech/hostd/wallet"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (

	// A chainManager is used to manage the blockchain
	renterChainManager struct {
		cm *consensus.ChainManager
	}

	tpool struct {
		tp modules.TransactionPool
	}

	// A Renter is an ephemeral renter that can be used for testing
	Renter struct {
		*node

		privKey ed25519.PrivateKey
		store   *sqlite.Store
		cm      *renterChainManager
		wallet  *wallet.SingleAddressWallet
	}

	// An RHP2Session is used to interact with a host over the RHP2 protocol
	RHP2Session struct {
		privKey ed25519.PrivateKey

		cm *renterChainManager
		tp *tpool
		w  *wallet.SingleAddressWallet

		settings rhpv2.HostSettings
		sess     *rhpv2.Session
	}
)

// Synced returns true if the chain manager is synced
func (cm *renterChainManager) Synced() bool {
	return cm.cm.Synced()
}

// TipState returns the current tip of the blockchain
func (cm *renterChainManager) TipState() rhpv2.ConsensusState {
	tip := cm.cm.Tip()
	return rhpv2.ConsensusState{
		Index: rhpv2.ChainIndex{
			Height: tip.Index.Height,
			ID:     rhpv2.BlockID(tip.Index.ID),
		},
	}
}

// RecommendedFee returns the recommended fee to have a transaction added
// to the transaction pool
func (tp tpool) RecommendedFee() types.Currency {
	_, max := tp.tp.FeeEstimation()
	return max
}

// Transactions returns the transactions currently in the transaction pool
func (tp tpool) Transactions() []types.Transaction {
	return tp.tp.Transactions()
}

// AddTransactionSet adds a transaction set to the transaction pool
func (tp tpool) AddTransactionSet(txns []types.Transaction) error {
	return tp.tp.AcceptTransactionSet(txns)
}

// Close closes the session's underlying connection
func (rs *RHP2Session) Close() error {
	return rs.sess.Close()
}

// Append appends a sector to the session's contract
func (rs *RHP2Session) Append(ctx context.Context, p []byte, duration uint64) (crypto.Hash, error) {
	cost, collateral := rhpv2.RPCAppendCost(rs.settings, duration)
	root, err := rs.sess.Append(ctx, (*[rhpv2.SectorSize]byte)(p), cost, collateral)
	if err != nil {
		return crypto.Hash{}, fmt.Errorf("failed to append sector: %w", err)
	}
	return crypto.Hash(root), nil
}

// Read reads a sector from the host
func (rs *RHP2Session) Read(ctx context.Context, w io.Writer, sectorRoot crypto.Hash, offset, length uint64) error {
	sections := []rhpv2.RPCReadRequestSection{
		{
			MerkleRoot: rhpv2.Hash256(sectorRoot),
			Offset:     offset,
			Length:     length,
		},
	}
	cost := rhpv2.RPCReadCost(rs.settings, sections)
	return rs.sess.Read(ctx, w, sections, cost)
}

// Revision returns the current revision of the locked contract
func (rs *RHP2Session) Revision() types.FileContractRevision {
	return rs.sess.Contract().Revision
}

// SectorRoots returns the sector roots of the locked contract
func (rs *RHP2Session) SectorRoots(ctx context.Context, offset, n uint64) ([]crypto.Hash, error) {
	cost := rhpv2.RPCSectorRootsCost(rs.settings, n)
	roots, err := rs.sess.SectorRoots(ctx, offset, n, cost)
	if err != nil {
		return nil, fmt.Errorf("failed to get sector roots: %w", err)
	}
	return *(*[]crypto.Hash)(unsafe.Pointer(&roots)), nil
}

// TipState returns the current tip of the blockchain
func (r *Renter) TipState() rhpv2.ConsensusState {
	return r.cm.TipState()
}

// Close shutsdown the renter
func (r *Renter) Close() error {
	r.wallet.Close()
	r.store.Close()
	r.node.Close()
	return nil
}

// NewRHP2Session creates a new session, locks a contract, and retrieves the
// host's settings
func (r *Renter) NewRHP2Session(ctx context.Context, hostAddr string, hostKey ed25519.PublicKey, contractID types.FileContractID) (*RHP2Session, error) {
	sess, err := rhpv2.DialSession(ctx, hostAddr, *(*rhpv2.PublicKey)(hostKey), contractID, rhpv2.PrivateKey(r.privKey))
	if err != nil {
		return nil, fmt.Errorf("failed to dial session: %w", err)
	}
	settings, err := rhpv2.RPCSettings(ctx, sess.Transport())
	if err != nil {
		return nil, fmt.Errorf("failed to get host settings: %w", err)
	}
	return &RHP2Session{
		privKey: r.privKey,

		sess:     sess,
		settings: settings,
		cm:       r.cm,
		tp:       &tpool{tp: r.tp},
		w:        r.wallet,
	}, nil
}

// Settings returns the host's current settings
func (r *Renter) Settings(ctx context.Context, hostAddr string, hostKey ed25519.PublicKey) (rhpv2.HostSettings, error) {
	transport, err := dialTransport(ctx, hostAddr, *(*rhpv2.PublicKey)(hostKey))
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to create session: %w", err)
	}
	defer transport.Close()
	settings, err := rhpv2.RPCSettings(ctx, transport)
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to get settings: %w", err)
	}
	return settings, nil
}

// FormContract forms a contract with the host
func (r *Renter) FormContract(ctx context.Context, hostAddr string, hostKey ed25519.PublicKey, renterPayout, hostCollateral types.Currency, duration uint64) (rhpv2.Contract, error) {
	hostPub := *(*rhpv2.PublicKey)(hostKey)
	renterPriv := rhpv2.PrivateKey(r.privKey)
	index := r.cm.TipState().Index
	t, err := dialTransport(ctx, hostAddr, hostPub)
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to dial transport: %w", err)
	}
	defer t.Close()
	settings, err := rhpv2.RPCSettings(ctx, t)
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to get host settings: %w", err)
	}

	contract := rhpv2.PrepareContractFormation(renterPriv, hostPub, renterPayout, hostCollateral, index.Height+duration, settings, r.WalletAddress())
	formationCost := rhpv2.ContractFormationCost(contract, settings.ContractPrice)
	_, max := r.tp.FeeEstimation()
	feeEstimate := max.Mul64(2000)
	formationTxn := types.Transaction{
		MinerFees:     []types.Currency{feeEstimate},
		FileContracts: []types.FileContract{contract},
	}
	fundAmount := formationCost.Add(feeEstimate)

	toSign, release, err := r.wallet.FundTransaction(&formationTxn, fundAmount)
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()

	if err := r.wallet.SignTransaction(&formationTxn, toSign, explicitCoveredFields(formationTxn)); err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	revision, _, err := rhpv2.RPCFormContract(t, r.cm.TipState(), renterPriv, []types.Transaction{formationTxn})
	if err != nil {
		return rhpv2.Contract{}, fmt.Errorf("failed to form contract: %w", err)
	}
	return revision, nil
}

// WalletAddress returns the renter's wallet address
func (r *Renter) WalletAddress() types.UnlockHash {
	return r.wallet.Address()
}

// dialTransport is a convenience function that connects to the specified
// host
func dialTransport(ctx context.Context, hostIP string, hostKey rhpv2.PublicKey) (_ *rhpv2.Transport, err error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()

	t, err := rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return t, nil
}

// explicitCoveredFields returns a CoveredFields that covers all elements
// present in txn.
func explicitCoveredFields(txn types.Transaction) (cf types.CoveredFields) {
	for i := range txn.SiacoinInputs {
		cf.SiacoinInputs = append(cf.SiacoinInputs, uint64(i))
	}
	for i := range txn.SiacoinOutputs {
		cf.SiacoinOutputs = append(cf.SiacoinOutputs, uint64(i))
	}
	for i := range txn.FileContracts {
		cf.FileContracts = append(cf.FileContracts, uint64(i))
	}
	for i := range txn.FileContractRevisions {
		cf.FileContractRevisions = append(cf.FileContractRevisions, uint64(i))
	}
	for i := range txn.StorageProofs {
		cf.StorageProofs = append(cf.StorageProofs, uint64(i))
	}
	for i := range txn.SiafundInputs {
		cf.SiafundInputs = append(cf.SiafundInputs, uint64(i))
	}
	for i := range txn.SiafundOutputs {
		cf.SiafundOutputs = append(cf.SiafundOutputs, uint64(i))
	}
	for i := range txn.MinerFees {
		cf.MinerFees = append(cf.MinerFees, uint64(i))
	}
	for i := range txn.ArbitraryData {
		cf.ArbitraryData = append(cf.ArbitraryData, uint64(i))
	}
	for i := range txn.TransactionSignatures {
		cf.TransactionSignatures = append(cf.TransactionSignatures, uint64(i))
	}
	return
}

// NewRenter creates a new renter for testing
func NewRenter(privKey ed25519.PrivateKey, dir string) (*Renter, error) {
	node, err := newNode(privKey, dir)
	if err != nil {
		return nil, err
	}
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "renter.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}
	cm, err := consensus.NewChainManager(node.cs)
	if err != nil {
		return nil, fmt.Errorf("failed to create chain manager: %w", err)
	}
	wallet := wallet.NewSingleAddressWallet(privKey, cm, db)
	if err := node.cs.ConsensusSetSubscribe(wallet, modules.ConsensusChangeBeginning, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe wallet to consensus set: %w", err)
	}

	return &Renter{
		node:    node,
		cm:      &renterChainManager{cm},
		privKey: privKey,
		store:   db,
		wallet:  wallet,
	}, nil
}
