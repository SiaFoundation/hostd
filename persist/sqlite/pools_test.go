package sqlite

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func addV2Contract(t *testing.T, db *Store, renterKey, hostKey types.PrivateKey) contracts.V2Contract {
	t.Helper()
	c := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  renterKey.PublicKey(),
			HostPublicKey:    hostKey.PublicKey(),
			ProofHeight:      100,
			ExpirationHeight: 200,
		},
	}
	if err := db.AddV2Contract(c, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRHP4Pools(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	contract := addV2Contract(t, db, renterKey, hostKey)

	pool1 := proto4.Account(types.GeneratePrivateKey().PublicKey())
	pool2 := proto4.Account(types.GeneratePrivateKey().PublicKey())

	balances, err := db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	} else if len(balances) != 2 {
		t.Fatalf("expected 2 balances, got %d", len(balances))
	} else if !balances[0].IsZero() || !balances[1].IsZero() {
		t.Fatalf("expected zero balances, got %v / %v", balances[0], balances[1])
	}

	contract.V2FileContract.RevisionNumber++
	balances, err = db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool1, Amount: types.Siacoins(10)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(10)})
	if err != nil {
		t.Fatal(err)
	} else if len(balances) != 1 || !balances[0].Equals(types.Siacoins(10)) {
		t.Fatalf("expected pool1 balance 10 SC, got %v", balances)
	}

	contract.V2FileContract.RevisionNumber++
	balances, err = db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool2, Amount: types.Siacoins(20)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(20)})
	if err != nil {
		t.Fatal(err)
	} else if !balances[0].Equals(types.Siacoins(20)) {
		t.Fatalf("expected pool2 balance 20 SC, got %v", balances[0])
	}

	balances, err = db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	} else if !balances[0].Equals(types.Siacoins(10)) || !balances[1].Equals(types.Siacoins(20)) {
		t.Fatalf("expected balances [10, 20], got %v", balances)
	}

	contract.V2FileContract.RevisionNumber++
	balances, err = db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool1, Amount: types.Siacoins(5)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(5)})
	if err != nil {
		t.Fatal(err)
	} else if !balances[0].Equals(types.Siacoins(15)) {
		t.Fatalf("expected pool1 balance 15 SC after top-up, got %v", balances[0])
	}
}

func TestRHP4PoolAttachDetach(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	contract := addV2Contract(t, db, renterKey, hostKey)

	account := proto4.Account(types.GeneratePrivateKey().PublicKey())
	pool := proto4.Account(types.GeneratePrivateKey().PublicKey())
	missingPool := proto4.Account(types.GeneratePrivateKey().PublicKey())

	err = db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: missingPool, ValidUntil: time.Now().Add(time.Minute)},
	})
	if !errors.Is(err, proto4.ErrPoolNotFound) {
		t.Fatalf("expected ErrPoolNotFound, got %v", err)
	}

	contract.V2FileContract.RevisionNumber++
	if _, err := db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool, Amount: types.Siacoins(5)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(5)}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: pool, ValidUntil: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: pool, ValidUntil: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatalf("re-attaching should be idempotent, got %v", err)
	}

	otherAccount := proto4.Account(types.GeneratePrivateKey().PublicKey())
	if err := db.RHP4DetachPools([]proto4.PoolDetachment{
		{Account: otherAccount, Pool: pool, ValidUntil: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatalf("detach of unattached pair should no-op, got %v", err)
	}

	if err := db.RHP4DetachPools([]proto4.PoolDetachment{
		{Account: account, Pool: pool, ValidUntil: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4DebitAccount(account, proto4.Usage{RPC: types.NewCurrency64(1)}); !errors.Is(err, proto4.ErrNotEnoughFunds) {
		t.Fatalf("expected ErrNotEnoughFunds after detach, got %v", err)
	}
}

func TestRHP4PoolDebitDrainage(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	contract := addV2Contract(t, db, renterKey, hostKey)

	account := proto4.Account(types.GeneratePrivateKey().PublicKey())
	pool1 := proto4.Account(types.GeneratePrivateKey().PublicKey())
	pool2 := proto4.Account(types.GeneratePrivateKey().PublicKey())

	contract.V2FileContract.RevisionNumber++
	if _, err := db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool1, Amount: types.Siacoins(10)},
		{Account: pool2, Amount: types.Siacoins(20)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(30)}); err != nil {
		t.Fatal(err)
	}
	contract.V2FileContract.RevisionNumber++
	if _, err := db.RHP4CreditAccounts([]proto4.AccountDeposit{
		{Account: account, Amount: types.Siacoins(5)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(5)}); err != nil {
		t.Fatal(err)
	}

	validUntil := time.Now().Add(time.Minute)
	if err := db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: pool1, ValidUntil: validUntil},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: pool2, ValidUntil: validUntil},
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4DebitAccount(account, proto4.Usage{RPC: types.Siacoins(7)}); err != nil {
		t.Fatal(err)
	}
	if bal, err := db.RHP4AccountBalance(account); err != nil {
		t.Fatal(err)
	} else if !bal.IsZero() {
		t.Fatalf("account: expected 0, got %v", bal)
	}
	balances, err := db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	} else if !balances[0].Equals(types.Siacoins(8)) {
		t.Fatalf("pool1 after first debit: expected 8 SC, got %v", balances[0])
	} else if !balances[1].Equals(types.Siacoins(20)) {
		t.Fatalf("pool2 after first debit: expected 20 SC, got %v", balances[1])
	}

	if err := db.RHP4DebitAccount(account, proto4.Usage{RPC: types.Siacoins(9)}); err != nil {
		t.Fatal(err)
	}
	balances, err = db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	} else if !balances[0].IsZero() {
		t.Fatalf("pool1 after second debit: expected 0, got %v", balances[0])
	} else if !balances[1].Equals(types.Siacoins(19)) {
		t.Fatalf("pool2 after second debit: expected 19 SC, got %v", balances[1])
	}

	preAccount, err := db.RHP4AccountBalance(account)
	if err != nil {
		t.Fatal(err)
	}
	prePools, err := db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RHP4DebitAccount(account, proto4.Usage{RPC: types.Siacoins(20)}); !errors.Is(err, proto4.ErrNotEnoughFunds) {
		t.Fatalf("expected ErrNotEnoughFunds, got %v", err)
	}
	postAccount, err := db.RHP4AccountBalance(account)
	if err != nil {
		t.Fatal(err)
	}
	postPools, err := db.RHP4PoolBalances([]proto4.Account{pool1, pool2})
	if err != nil {
		t.Fatal(err)
	}
	if !preAccount.Equals(postAccount) || !prePools[0].Equals(postPools[0]) || !prePools[1].Equals(postPools[1]) {
		t.Fatalf("balances changed despite ErrNotEnoughFunds: account %v→%v pools %v→%v", preAccount, postAccount, prePools, postPools)
	}
}

func TestRHP4PoolFundingDistribution(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	c1 := addV2Contract(t, db, renterKey, hostKey)
	c2 := addV2Contract(t, db, renterKey, hostKey)

	pool := proto4.Account(types.GeneratePrivateKey().PublicKey())
	account := proto4.Account(types.GeneratePrivateKey().PublicKey())

	c1.V2FileContract.RevisionNumber++
	if _, err := db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool, Amount: types.Siacoins(3)},
	}, c1.ID, c1.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(3)}); err != nil {
		t.Fatal(err)
	}
	c2.V2FileContract.RevisionNumber++
	if _, err := db.RHP4CreditPools([]proto4.AccountDeposit{
		{Account: pool, Amount: types.Siacoins(3)},
	}, c2.ID, c2.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(3)}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4AttachPools([]proto4.PoolAttachment{
		{Account: account, Pool: pool, ValidUntil: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.RHP4DebitAccount(account, proto4.Usage{RPC: types.Siacoins(4)}); err != nil {
		t.Fatal(err)
	}

	contractsList, _, err := db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	}
	var totalRPC, totalAccountFunding types.Currency
	for _, c := range contractsList {
		totalRPC = totalRPC.Add(c.Usage.RPC)
		totalAccountFunding = totalAccountFunding.Add(c.Usage.AccountFunding)
	}
	if !totalRPC.Equals(types.Siacoins(4)) {
		t.Fatalf("expected total RPC revenue 4 SC, got %v", totalRPC)
	} else if !totalAccountFunding.Equals(types.Siacoins(2)) {
		t.Fatalf("expected remaining account_funding 2 SC, got %v", totalAccountFunding)
	}
}
