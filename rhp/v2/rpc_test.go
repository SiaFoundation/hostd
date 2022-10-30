package rhp_test

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/hostd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter/proto" // tmp
	"lukechampine.com/us/renterhost"   // tmp
)

type stubConsensus struct{}

func (stubConsensus) Height() types.BlockHeight                           { return 300000 }
func (stubConsensus) BlockAtHeight(types.BlockHeight) (types.Block, bool) { return types.Block{}, true }
func (stubConsensus) BlockByID(types.BlockID) (types.Block, types.BlockHeight, bool) {
	return types.Block{}, 0, true
}

type stubWallet struct{}

func (stubWallet) Address() (_ types.UnlockHash) { return }
func (stubWallet) FundTransaction(txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.SiacoinOutputID, func(), error) {
	txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
		ParentID: types.SiacoinOutputID{1},
		UnlockConditions: types.UnlockConditions{
			SignaturesRequired: 0,
		},
	})
	return nil, func() {}, nil
}
func (stubWallet) SignTransaction(txn *types.Transaction, toSign []types.SiacoinOutputID) error {
	txn.TransactionSignatures = append(txn.TransactionSignatures, make([]types.TransactionSignature, len(toSign))...)
	return nil
}

type stubUsWallet struct{}

func (stubUsWallet) Address() (_ types.UnlockHash, _ error) { return }
func (stubUsWallet) FundTransaction(txn *types.Transaction, amount types.Currency) ([]crypto.Hash, func(), error) {
	txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
		ParentID: types.SiacoinOutputID{1},
		UnlockConditions: types.UnlockConditions{
			SignaturesRequired: 0,
		},
	})
	return nil, func() {}, nil
}
func (stubUsWallet) SignTransaction(txn *types.Transaction, toSign []crypto.Hash) error {
	txn.TransactionSignatures = append(txn.TransactionSignatures, make([]types.TransactionSignature, len(toSign))...)
	return nil
}

type stubMetricReporter struct{}

func (stubMetricReporter) Report(any) (_ error) { return }

type stubTpool struct{}

func (stubTpool) AcceptTransactionSet([]types.Transaction) (_ error)                    { return }
func (stubTpool) UnconfirmedParents(types.Transaction) (_ []types.Transaction, _ error) { return }
func (stubTpool) FeeEstimate() (_, _ types.Currency)                                    { return }

type stubUsTpool struct{}

func (stubUsTpool) AcceptTransactionSet([]types.Transaction) (_ error)                    { return }
func (stubUsTpool) UnconfirmedParents(types.Transaction) (_ []types.Transaction, _ error) { return }
func (stubUsTpool) FeeEstimate() (_, _ types.Currency, _ error)                           { return }

// createTestingPair creates a renter and host, initiates a Session between
// them, and forms and locks a contract.
func createTestingPair(tb testing.TB) (*proto.Session, *rhp.SessionHandler) {
	hostKey, renterKey := ed25519.NewKeyFromSeed(frand.Bytes(32)), ed25519.NewKeyFromSeed(frand.Bytes(32))

	storageManager := store.NewEphemeralStorageManager()
	contractManager := contracts.NewManager(store.NewEphemeralContractStore(), storageManager, stubConsensus{}, stubTpool{}, stubWallet{})
	settingsManager, err := settings.NewConfigManager(store.NewEphemeralSettingsStore())
	if err != nil {
		tb.Fatal("failed to initialize settings:", err)
	}
	settingsManager.UpdateSettings(settings.Settings{
		MaxContractDuration: 1000,
		MaxCollateral:       types.SiacoinPrecision.Mul64(100),
	})

	host, err := rhp.NewSessionHandler(hostKey, "localhost:0", stubConsensus{}, stubTpool{}, stubWallet{}, contractManager, settingsManager, storageManager, stubMetricReporter{})
	if err != nil {
		tb.Fatal("failed to create host:", err)
	}
	tb.Cleanup(func() { host.Close() })
	go func() {
		if err := host.Serve(); err != nil {
			tb.Fatal("host.Serve:", err)
		}
	}()

	s, err := proto.NewUnlockedSession(modules.NetAddress(host.LocalAddr()), hostdb.HostKeyFromPublicKey(hostKey.Public().(ed25519.PublicKey)), 300000)
	if err != nil {
		tb.Fatal(err)
	} else if _, err := s.Settings(); err != nil {
		tb.Fatal(err)
	}

	rev, _, err := s.FormContract(stubUsWallet{}, stubUsTpool{}, renterKey, types.ZeroCurrency, 300000, 301000)
	if err != nil {
		tb.Fatal(err)
	}
	err = s.Lock(rev.ID(), renterKey, 0)
	if err != nil {
		tb.Fatal(err)
	}
	return s, host
}

func TestSession(t *testing.T) {
	renter, _ := createTestingPair(t)
	defer renter.Close()

	sector := [rhp.SectorSize]byte{0: 1}
	sectorRoot, err := renter.Append(&sector)
	if err != nil {
		t.Fatal(err)
	}

	roots, err := renter.SectorRoots(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if roots[0] != sectorRoot {
		t.Fatal("reported sector root does not match actual sector root")
	}

	var sectorBuf bytes.Buffer
	err = renter.Read(&sectorBuf, []renterhost.RPCReadRequestSection{{
		MerkleRoot: sectorRoot,
		Offset:     0,
		Length:     rhp.SectorSize,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sectorBuf.Bytes(), sector[:]) {
		t.Fatal("downloaded sector does not match uploaded sector")
	}

	err = renter.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}
