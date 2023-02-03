package test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/renterd/worker"
)

type (
	// A Renter is an ephemeral renter that can be used for testing
	Renter struct {
		*node

		privKey types.PrivateKey
		store   *sqlite.Store
		wallet  *wallet.SingleAddressWallet
	}
)

// Close shutsdown the renter
func (r *Renter) Close() error {
	r.wallet.Close()
	r.store.Close()
	r.node.Close()
	return nil
}

// NewRHP2Session creates a new session, locks a contract, and retrieves the
// host's settings
func (r *Renter) NewRHP2Session(ctx context.Context, hostAddr string, hostKey types.PublicKey, contractID types.FileContractID) (*worker.Session, error) {
	t, err := dialTransport(ctx, hostAddr, hostKey)
	if err != nil {
		return nil, err
	}
	settings, err := worker.RPCSettings(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("failed to get host settings: %w", err)
	}
	rev, err := worker.RPCLock(ctx, t, contractID, r.privKey, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to lock contract: %w", err)
	}
	return worker.NewSession(t, r.privKey, rev, settings), nil
}

// Settings returns the host's current settings
func (r *Renter) Settings(ctx context.Context, hostAddr string, hostKey types.PublicKey) (rhpv2.HostSettings, error) {
	t, err := dialTransport(ctx, hostAddr, hostKey)
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to create session: %w", err)
	}
	defer t.Close()
	settings, err := worker.RPCSettings(ctx, t)
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to get settings: %w", err)
	}
	return settings, nil
}

// FormContract forms a contract with the host
func (r *Renter) FormContract(ctx context.Context, hostAddr string, hostKey types.PublicKey, renterPayout, hostCollateral types.Currency, duration uint64) (rhpv2.ContractRevision, error) {
	t, err := dialTransport(ctx, hostAddr, hostKey)
	if err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("failed to dial transport: %w", err)
	}
	defer t.Close()
	settings, err := worker.RPCSettings(ctx, t)
	if err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("failed to get host settings: %w", err)
	}
	cs := r.TipState()
	contract := rhpv2.PrepareContractFormation(r.privKey, hostKey, renterPayout, hostCollateral, cs.Index.Height+duration, settings, r.WalletAddress())
	formationCost := rhpv2.ContractFormationCost(contract, settings.ContractPrice)
	feeEstimate := r.tp.RecommendedFee().Mul64(2000)
	formationTxn := types.Transaction{
		MinerFees:     []types.Currency{feeEstimate},
		FileContracts: []types.FileContract{contract},
	}
	fundAmount := formationCost.Add(feeEstimate)

	toSign, release, err := r.wallet.FundTransaction(&formationTxn, fundAmount)
	if err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer release()

	if err := r.wallet.SignTransaction(cs, &formationTxn, toSign, explicitCoveredFields(formationTxn)); err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	revision, _, err := worker.RPCFormContract(t, r.privKey, []types.Transaction{formationTxn})
	if err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("failed to form contract: %w", err)
	}
	return revision, nil
}

// WalletAddress returns the renter's wallet address
func (r *Renter) WalletAddress() types.Address {
	return r.wallet.Address()
}

// dialTransport is a convenience function that connects to the specified
// host
func dialTransport(ctx context.Context, hostIP string, hostKey types.PublicKey) (_ *rhpv2.Transport, err error) {
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
	for i := range txn.Signatures {
		cf.Signatures = append(cf.Signatures, uint64(i))
	}
	return
}

// NewRenter creates a new renter for testing
func NewRenter(privKey types.PrivateKey, dir string) (*Renter, error) {
	node, err := newNode(privKey, dir)
	if err != nil {
		return nil, err
	}
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "renter.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create sql store: %w", err)
	}
	wallet := wallet.NewSingleAddressWallet(privKey, node.cm, db)
	ccid, err := db.LastWalletChange()
	if err != nil {
		return nil, err
	} else if err := node.cs.ConsensusSetSubscribe(wallet, ccid, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe wallet to consensus set: %w", err)
	}
	node.tp.tp.TransactionPoolSubscribe(wallet)

	return &Renter{
		node:    node,
		privKey: privKey,
		store:   db,
		wallet:  wallet,
	}, nil
}
