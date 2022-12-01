package test

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"

	rhpv2 "go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter/proto"
	"lukechampine.com/us/renterhost"
)

// A Renter is an ephemeral renter that can be used for testing
type (
	usWallet struct {
		w *wallet.SingleAddressWallet
	}

	usTPool struct {
		tp modules.TransactionPool
	}

	Renter struct {
		node

		privKey ed25519.PrivateKey
	}

	RenterSession struct {
		privKey ed25519.PrivateKey

		cs modules.ConsensusSet
		tp proto.TransactionPool
		w  proto.Wallet

		sess *proto.Session
	}
)

// AcceptTransactionSet adds a transaction set to the transaction pool
func (tp *usTPool) AcceptTransactionSet(txns []types.Transaction) error {
	return tp.tp.AcceptTransactionSet(txns)
}

// UnconfirmedParents returns the unconfirmed parents of a transaction
func (tp *usTPool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	return nil, nil
}

// FeeEstimate returns the estimated fee to have a transaction added to the pool
func (tp *usTPool) FeeEstimate() (min, max types.Currency, _ error) {
	min, max = tp.tp.FeeEstimation()
	return
}

// Address returns the address of the wallet
func (w *usWallet) Address() (types.UnlockHash, error) {
	return w.w.Address(), nil
}

// FundTransaction funds a transaction
func (w *usWallet) FundTransaction(txn *types.Transaction, amount types.Currency) ([]crypto.Hash, func(), error) {
	outputIDs, release, err := w.w.FundTransaction(txn, amount, nil)
	outputs := make([]crypto.Hash, len(outputIDs))
	for i, id := range outputIDs {
		outputs[i] = crypto.Hash(id)
	}
	return outputs, release, err
}

// SignTransaction signs a transaction
func (w *usWallet) SignTransaction(txn *types.Transaction, toSign []crypto.Hash) error {
	ids := make([]types.SiacoinOutputID, len(toSign))
	for i, id := range toSign {
		ids[i] = types.SiacoinOutputID(id)
	}
	return w.w.SignTransaction(txn, ids)
}

func (rs *RenterSession) Close() error {
	return rs.sess.Close()
}

// Append appends a sector to the session's contract
func (rs *RenterSession) Append(p []byte) (crypto.Hash, error) {
	return rs.sess.Append((*[rhpv2.SectorSize]byte)(p))
}

// Read reads a sector from the host
func (rs *RenterSession) Read(w io.Writer, sectorRoot crypto.Hash, offset, length uint32) error {
	return rs.sess.Read(w, []renterhost.RPCReadRequestSection{
		{
			MerkleRoot: sectorRoot,
			Offset:     offset,
			Length:     length,
		},
	})
}

func (rs *RenterSession) Revision() types.FileContractRevision {
	return rs.sess.Revision().Revision
}

func (rs *RenterSession) SectorRoots(offset, n int) ([]crypto.Hash, error) {
	return rs.sess.SectorRoots(offset, n)
}

// Close shutsdown the renter
func (r *Renter) Close() error {
	r.node.Close()
	return nil
}

func (r *Renter) NewLockedSession(hostAddr string, hostKey ed25519.PublicKey, contractID types.FileContractID) (*RenterSession, error) {
	sess, err := proto.NewSession(modules.NetAddress(hostAddr), hostdb.HostKeyFromPublicKey(hostKey), contractID, r.privKey, r.cs.Height())
	if err != nil {
		return nil, err
	} else if _, err := sess.Settings(); err != nil {
		return nil, fmt.Errorf("failed to get settings: %w", err)
	}
	return &RenterSession{
		privKey: r.privKey,
		sess:    sess,
		cs:      r.cs,
		tp:      &usTPool{tp: r.tp},
		w:       &usWallet{w: r.w},
	}, nil
}

func (r *Renter) Settings(hostAddr string, hostKey ed25519.PublicKey) (rhpv2.HostSettings, error) {
	s, err := proto.NewUnlockedSession(modules.NetAddress(hostAddr), hostdb.HostKeyFromPublicKey(hostKey), r.cs.Height())
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to create session: %w", err)
	}
	defer s.Close()
	settings, err := s.Settings()
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to get settings: %w", err)
	}
	buf, err := json.Marshal(settings)
	if err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to marshal settings: %w", err)
	}
	var hs rhpv2.HostSettings
	if err := json.Unmarshal(buf, &hs); err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("failed to unmarshal settings: %w", err)
	}
	return hs, nil
}

func (r *Renter) FormContract(hostAddr string, hostKey ed25519.PublicKey, renterPayout types.Currency, duration uint64) (proto.ContractRevision, error) {
	startHeight := r.cs.Height()
	s, err := proto.NewUnlockedSession(modules.NetAddress(hostAddr), hostdb.HostKeyFromPublicKey(hostKey), startHeight)
	if err != nil {
		return proto.ContractRevision{}, fmt.Errorf("failed to create session: %w", err)
	}
	defer s.Close()
	if _, err := s.Settings(); err != nil {
		return proto.ContractRevision{}, fmt.Errorf("failed to get settings: %w", err)
	}
	revision, _, err := s.FormContract(&usWallet{r.w}, &usTPool{r.tp}, r.privKey, renterPayout, startHeight, startHeight+types.BlockHeight(duration))
	if err != nil {
		return proto.ContractRevision{}, fmt.Errorf("failed to form contract: %w", err)
	}
	return revision, nil
}

func NewEphemeralRenter(privKey ed25519.PrivateKey, dir string) (*Renter, error) {
	node, err := newNode(privKey, dir)
	if err != nil {
		return nil, err
	}
	return &Renter{
		node:    *node,
		privKey: privKey,
	}, nil
}
