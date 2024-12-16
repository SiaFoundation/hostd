package rhp_test

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/internal/testutil"
	"go.sia.tech/hostd/rhp"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type fundAndSign struct {
	w  *wallet.SingleAddressWallet
	pk types.PrivateKey
}

func (fs *fundAndSign) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return fs.w.FundV2Transaction(txn, amount, true)
}
func (fs *fundAndSign) ReleaseInputs(txns []types.V2Transaction) {
	fs.w.ReleaseInputs(nil, txns)
}

func (fs *fundAndSign) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	fs.w.SignV2Inputs(txn, toSign)
}
func (fs *fundAndSign) SignHash(h types.Hash256) types.Signature {
	return fs.pk.SignHash(h)
}
func (fs *fundAndSign) PublicKey() types.PublicKey {
	return fs.pk.PublicKey()
}
func (fs *fundAndSign) Address() types.Address {
	return fs.w.Address()
}

func testRenterHostPair(tb testing.TB, hostKey types.PrivateKey, hn *testutil.HostNode, log *zap.Logger) rhp4.TransportClient {
	rs := rhp4.NewServer(hostKey, hn.Chain, hn.Syncer, hn.Contracts, hn.Wallet, hn.Settings, hn.Volumes, rhp4.WithPriceTableValidity(2*time.Minute))

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })
	go rhp.ServeRHP4SiaMux(l, rs, log.Named("siamux"))

	transport, err := rhp4.DialSiaMux(context.Background(), l.Addr().String(), hostKey.PublicKey())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { transport.Close() })

	return transport
}

func TestSettings(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey := types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	testutil.MineAndSync(t, hn, hn.Wallet.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	} else if settings.Prices.ValidUntil.Before(time.Now()) {
		t.Fatal("settings expired")
	}

	// verify the signature
	sigHash := settings.Prices.SigHash()
	if !hostKey.PublicKey().VerifyHash(sigHash, settings.Prices.Signature) {
		t.Fatal("signature verification failed")
	}

	// adjust the calculated fields to match the expected values
	expected := hn.Settings.RHP4Settings()
	expected.ProtocolVersion = settings.ProtocolVersion
	expected.Prices.Signature = settings.Prices.Signature
	expected.Prices.ValidUntil = settings.Prices.ValidUntil
	expected.Prices.TipHeight = settings.Prices.TipHeight

	if !reflect.DeepEqual(settings, expected) {
		t.Error("retrieved", settings)
		t.Error("expected", expected)
		t.Fatal("settings mismatch")
	}
}

func TestFormContract(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the transaction set is valid
	if known, err := hn.Chain.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}

	sigHash := cm.TipState().ContractSigHash(result.Contract.Revision)
	if !renterKey.PublicKey().VerifyHash(sigHash, result.Contract.Revision.RenterSignature) {
		t.Fatal("renter signature verification failed")
	} else if !hostKey.PublicKey().VerifyHash(sigHash, result.Contract.Revision.HostSignature) {
		t.Fatal("host signature verification failed")
	}
}

func TestRPCRefresh(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	hn := testutil.NewHostNode(t, hostKey, n, genesis, log)
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, log.Named("renterhost"))

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	fundAndSign := &fundAndSign{w, renterKey}

	formContractUploadSector := func(t *testing.T, renterAllowance, hostCollateral, accountBalance types.Currency) rhp4.ContractRevision {
		t.Helper()

		result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w.Address(),
			Allowance:       renterAllowance,
			Collateral:      hostCollateral,
			ProofHeight:     cm.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		revision := result.Contract

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		// mine a few blocks to confirm the contract
		testutil.MineAndSync(t, hn, types.VoidAddress, 10)

		// fund an account to transfer funds to the host
		cs := cm.TipState()
		account := proto4.Account(renterKey.PublicKey())
		fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
			{Account: account, Amount: accountBalance},
		})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = fundResult.Revision

		// upload data
		at := proto4.AccountToken{
			Account:    account,
			ValidUntil: time.Now().Add(5 * time.Minute),
		}
		at.Signature = renterKey.SignHash(at.SigHash())
		wRes, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, at, bytes.NewReader(bytes.Repeat([]byte{1}, proto4.LeafSize)), proto4.LeafSize)
		if err != nil {
			t.Fatal(err)
		}
		aRes, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{wRes.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = aRes.Revision

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if rs.Renewed {
			t.Fatal("expected contract to not be renewed")
		} else if !rs.Revisable {
			t.Fatal("expected contract to be revisable")
		}
		return revision
	}

	t.Run("no allowance or collateral", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// refresh the contract
		_, err = rhp4.RPCRefreshContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRefreshContractParams{
			ContractID: revision.ID,
			Allowance:  types.ZeroCurrency,
			Collateral: types.ZeroCurrency,
		})
		if err == nil {
			t.Fatal(err)
		} else if !strings.Contains(err.Error(), "allowance must be greater than zero") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid refresh", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))
		// refresh the contract
		refreshResult, err := rhp4.RPCRefreshContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRefreshContractParams{
			ContractID: revision.ID,
			Allowance:  types.Siacoins(10),
			Collateral: types.Siacoins(20),
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(refreshResult.RenewalSet.Basis, refreshResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(refreshResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, refreshResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, refreshResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})
}

func TestRPCRenew(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	fundAndSign := &fundAndSign{w, renterKey}

	formContractUploadSector := func(t *testing.T, renterAllowance, hostCollateral, accountBalance types.Currency) rhp4.ContractRevision {
		t.Helper()

		result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w.Address(),
			Allowance:       renterAllowance,
			Collateral:      hostCollateral,
			ProofHeight:     cm.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		revision := result.Contract
		sigHash := cm.TipState().ContractSigHash(revision.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, revision.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, revision.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		// mine a few blocks to confirm the contract
		testutil.MineAndSync(t, hn, types.VoidAddress, 10)

		// fund an account to transfer funds to the host
		cs := cm.TipState()
		account := proto4.Account(renterKey.PublicKey())
		fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
			{Account: account, Amount: accountBalance},
		})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = fundResult.Revision

		// upload data
		at := proto4.AccountToken{
			Account:    account,
			ValidUntil: time.Now().Add(5 * time.Minute),
		}
		at.Signature = renterKey.SignHash(at.SigHash())
		wRes, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, at, bytes.NewReader(bytes.Repeat([]byte{1}, proto4.LeafSize)), proto4.LeafSize)
		if err != nil {
			t.Fatal(err)
		}
		aRes, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{wRes.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = aRes.Revision
		return revision
	}

	t.Run("same duration", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		_, err = rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight,
		})
		if err == nil {
			t.Fatal(err)
		} else if !strings.Contains(err.Error(), "renewal proof height must be greater than existing proof height") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("partial rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})

	t.Run("full rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(50),
			Collateral:  types.Siacoins(100),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})

	t.Run("no rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})
}

func TestAccounts(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	account := proto4.Account(renterKey.PublicKey())

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()

	balance, err := rhp4.RPCAccountBalance(context.Background(), transport, account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.IsZero() {
		t.Fatal("expected zero balance")
	}

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revised := fundResult.Revision

	renterOutputValue := revision.Revision.RenterOutput.Value.Sub(accountFundAmount)
	hostOutputValue := revision.Revision.HostOutput.Value.Add(accountFundAmount)

	// verify the contract was modified correctly
	switch {
	case fundResult.Balances[0].Account != account:
		t.Fatalf("expected %v, got %v", account, fundResult.Balances[0].Account)
	case fundResult.Balances[0].Balance != accountFundAmount:
		t.Fatalf("expected %v, got %v", accountFundAmount, fundResult.Balances[0].Balance)
	case !fundResult.Usage.RenterCost().Equals(accountFundAmount):
		t.Fatalf("expected %v, got %v", accountFundAmount, fundResult.Usage.RenterCost())
	case !revised.HostOutput.Value.Equals(hostOutputValue):
		t.Fatalf("expected %v, got %v", hostOutputValue, revised.HostOutput.Value)
	case !revised.RenterOutput.Value.Equals(renterOutputValue):
		t.Fatalf("expected %v, got %v", renterOutputValue, revised.RenterOutput.Value)
	case !revised.MissedHostValue.Equals(revision.Revision.MissedHostValue):
		t.Fatalf("expected %v, got %v", revision.Revision.MissedHostValue, revised.MissedHostValue)
	case revised.RevisionNumber != revision.Revision.RevisionNumber+1:
		t.Fatalf("expected %v, got %v", revision.Revision.RevisionNumber+1, revised.RevisionNumber)
	}

	revisionSigHash := cs.ContractSigHash(revised)
	if !renterKey.PublicKey().VerifyHash(revisionSigHash, revised.RenterSignature) {
		t.Fatal("revision signature verification failed")
	} else if !hostKey.PublicKey().VerifyHash(revisionSigHash, revised.HostSignature) {
		t.Fatal("revision signature verification failed")
	}

	// verify the account balance
	balance, err = rhp4.RPCAccountBalance(context.Background(), transport, account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(accountFundAmount) {
		t.Fatalf("expected %v, got %v", accountFundAmount, balance)
	}
}

func TestReadWriteSector(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 10, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	data := frand.Bytes(1024)

	// store the sector
	writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if writeResult.Root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// read the sector back
	buf := bytes.NewBuffer(nil)
	_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, writeResult.Root, 0, 64)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data[:64]) {
		t.Fatal("data mismatch")
	}
}

func TestAppendSectors(t *testing.T) {
	const sectors = 10
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), sectors, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	assertLastRevision := func(t *testing.T) {
		t.Helper()

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		}
		lastRev := rs.Contract
		if !reflect.DeepEqual(lastRev, revision.Revision) {
			t.Log(lastRev)
			t.Log(revision.Revision)
			t.Fatalf("expected last revision to match")
		}

		sigHash := cm.TipState().ContractSigHash(revision.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, lastRev.RenterSignature) {
			t.Fatal("renter signature invalid")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, lastRev.HostSignature) {
			t.Fatal("host signature invalid")
		}
	}
	assertLastRevision(t)

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	assertLastRevision(t)

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	// store random sectors
	roots := make([]types.Hash256, 0, 10)
	for i := 0; i < 10; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)

		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sector[:]), proto4.SectorSize)
		if err != nil {
			t.Fatal(err)
		} else if writeResult.Root != root {
			t.Fatal("root mismatch")
		}
		roots = append(roots, root)
	}

	// corrupt a random root
	excludedIndex := frand.Intn(len(roots))
	roots[excludedIndex] = frand.Entropy256()

	// append the sectors to the contract
	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		t.Fatal(err)
	} else if len(appendResult.Sectors) != len(roots)-1 {
		t.Fatalf("expected %v, got %v", len(roots)-1, len(appendResult.Sectors))
	}
	roots = append(roots[:excludedIndex], roots[excludedIndex+1:]...)
	if appendResult.Revision.FileMerkleRoot != proto4.MetaRoot(roots) {
		t.Fatal("root mismatch")
	}
	revision.Revision = appendResult.Revision
	assertLastRevision(t)

	// read the sectors back
	buf := bytes.NewBuffer(make([]byte, 0, proto4.SectorSize))
	for _, root := range roots {
		buf.Reset()

		_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, root, 0, proto4.SectorSize)
		if err != nil {
			t.Fatal(err)
		} else if proto4.SectorRoot((*[proto4.SectorSize]byte)(buf.Bytes())) != root {
			t.Fatal("data mismatch")
		}
	}
}

func TestVerifySector(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	data := frand.Bytes(1024)

	// store the sector
	writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if writeResult.Root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// verify the host is storing the sector
	_, err = rhp4.RPCVerifySector(context.Background(), transport, settings.Prices, token, writeResult.Root)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCFreeSectors(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	roots := make([]types.Hash256, 10)
	for i := range roots {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = writeResult.Root
	}

	assertRevision := func(t *testing.T, revision types.V2FileContract, roots []types.Hash256) {
		t.Helper()

		expectedRoot := proto4.MetaRoot(roots)
		n := len(roots)

		if revision.Filesize/proto4.SectorSize != uint64(n) {
			t.Fatalf("expected %v sectors, got %v", n, revision.Filesize/proto4.SectorSize)
		} else if revision.FileMerkleRoot != expectedRoot {
			t.Fatalf("expected %v, got %v", expectedRoot, revision.FileMerkleRoot)
		}
	}

	// append all the sector roots to the contract
	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		t.Fatal(err)
	}
	assertRevision(t, appendResult.Revision, roots)
	revision.Revision = appendResult.Revision

	// randomly remove half the sectors
	indices := make([]uint64, len(roots)/2)
	for i, n := range frand.Perm(len(roots))[:len(roots)/2] {
		indices[i] = uint64(n)
	}
	newRoots := append([]types.Hash256(nil), roots...)
	for i, n := range indices {
		newRoots[n] = newRoots[len(newRoots)-i-1]
	}
	newRoots = newRoots[:len(newRoots)-len(indices)]

	removeResult, err := rhp4.RPCFreeSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, indices)
	if err != nil {
		t.Fatal(err)
	}
	assertRevision(t, removeResult.Revision, newRoots)
}

func TestRPCSectorRoots(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 64, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	roots := make([]types.Hash256, 0, 50)

	checkRoots := func(t *testing.T, expected []types.Hash256) {
		t.Helper()

		rootsResult, err := rhp4.RPCSectorRoots(context.Background(), transport, cs, settings.Prices, renterKey, revision, 0, uint64(len(expected)))
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expected) {
			t.Fatalf("expected %v roots, got %v", len(expected), len(roots))
		}
		for i := range rootsResult.Roots {
			if roots[i] != expected[i] {
				t.Fatalf("expected %v, got %v", expected[i], roots[i])
			}
		}
		revision.Revision = rootsResult.Revision
	}

	for i := 0; i < cap(roots); i++ {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, writeResult.Root)

		appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{writeResult.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = appendResult.Revision
		checkRoots(t, roots)
	}
}

func TestPrune(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(t.TempDir(), "test.dat"), 10, results); err != nil {
		t.Fatal(err)
	} else if err := <-results; err != nil {
		t.Fatal(err)
	}

	testutil.MineAndSync(t, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(t, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	data := frand.Bytes(1024)

	// store the sector
	writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if writeResult.Root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// read the sector back
	buf := bytes.NewBuffer(nil)
	_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, writeResult.Root, 0, 64)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data[:64]) {
		t.Fatal("data mismatch")
	}
}

func BenchmarkWrite(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(b, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(b.TempDir(), "test.dat"), uint64(b.N), results); err != nil {
		b.Fatal(err)
	} else if err := <-results; err != nil {
		b.Fatal(err)
	}

	testutil.MineAndSync(b, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(b, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

	var sectors [][proto4.SectorSize]byte
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	for i := 0; i < b.N; i++ {
		// store the sector
		_, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(b, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(b.TempDir(), "test.dat"), uint64(b.N), results); err != nil {
		b.Fatal(err)
	} else if err := <-results; err != nil {
		b.Fatal(err)
	}

	testutil.MineAndSync(b, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(b, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

	var sectors [][proto4.SectorSize]byte
	roots := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		}
		roots = append(roots, writeResult.Root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	buf := bytes.NewBuffer(make([]byte, 0, proto4.SectorSize))
	for i := 0; i < b.N; i++ {
		buf.Reset()
		// store the sector
		_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, roots[i], 0, proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		} else if !bytes.Equal(buf.Bytes(), sectors[i][:]) {
			b.Fatal("data mismatch")
		}
	}
}

func BenchmarkContractUpload(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	hn := testutil.NewHostNode(b, hostKey, n, genesis, zap.NewNop())
	cm := hn.Chain
	w := hn.Wallet

	results := make(chan error, 1)
	if _, err := hn.Volumes.AddVolume(context.Background(), filepath.Join(b.TempDir(), "test.dat"), uint64(b.N), results); err != nil {
		b.Fatal(err)
	} else if err := <-results; err != nil {
		b.Fatal(err)
	}

	testutil.MineAndSync(b, hn, w.Address(), int(n.MaturityDelay+20))

	transport := testRenterHostPair(b, hostKey, hn, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

	var sectors [][proto4.SectorSize]byte
	roots := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)
		roots = append(roots, proto4.SectorRoot(&sector))
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
			if err != nil {
				b.Error(err)
			} else if writeResult.Root != roots[i] {
				b.Errorf("expected %v, got %v", roots[i], writeResult.Root)
			}
		}(i)
	}

	wg.Wait()

	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		b.Fatal(err)
	} else if appendResult.Revision.Filesize != uint64(b.N)*proto4.SectorSize {
		b.Fatalf("expected %v sectors, got %v", b.N, appendResult.Revision.Filesize/proto4.SectorSize)
	}
}
