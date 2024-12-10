package accounts_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/internal/testutil"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestUsageTotal(t *testing.T) {
	var u accounts.Usage
	uv := reflect.ValueOf(&u).Elem()

	var total types.Currency
	for i := 0; i < uv.NumField(); i++ {
		v := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		total = total.Add(v)
		uv.Field(i).Set(reflect.ValueOf(v))
	}

	if u.Total() != total {
		t.Fatal("total mismatch")
	}
}

func TestUsageAdd(t *testing.T) {
	var ua, ub accounts.Usage
	var expected accounts.Usage
	uav := reflect.ValueOf(&ua).Elem()
	ubv := reflect.ValueOf(&ub).Elem()
	ev := reflect.ValueOf(&expected).Elem()

	for i := 0; i < uav.NumField(); i++ {
		va := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		vb := types.NewCurrency(frand.Uint64n(math.MaxUint64), 0)
		total := va.Add(vb)

		uav.Field(i).Set(reflect.ValueOf(va))
		ubv.Field(i).Set(reflect.ValueOf(vb))
		ev.Field(i).Set(reflect.ValueOf(total))
	}

	total := ua.Add(ub)
	tv := reflect.ValueOf(total)
	for i := 0; i < tv.NumField(); i++ {
		va := ev.Field(i).Interface().(types.Currency)
		vb := tv.Field(i).Interface().(types.Currency)
		if !va.Equals(vb) {
			t.Fatalf("field %v: expected %v, got %v", tv.Type().Field(i).Name, va, vb)
		}
	}
}

func TestBudget(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey := types.GeneratePrivateKey()
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, hostKey, network, genesis, log)

	rev := contracts.SignedRevision{
		Revision: types.FileContractRevision{
			ParentID: frand.Entropy256(),
			UnlockConditions: types.UnlockConditions{
				PublicKeys: []types.UnlockKey{
					{Algorithm: types.SpecifierEd25519, Key: frand.Bytes(32)},
					{Algorithm: types.SpecifierEd25519, Key: frand.Bytes(32)},
				},
			},
		},
	}
	amount := types.NewCurrency64(100)
	if err := node.Contracts.AddContract(rev, []types.Transaction{{}}, types.Siacoins(1), contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	am := accounts.NewManager(node.Store, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
	accountID := frand.Entropy256()
	expectedFunding := amount
	req := accounts.FundAccountWithContract{
		Account:    accountID,
		Amount:     amount,
		Cost:       types.NewCurrency64(1),
		Revision:   rev,
		Expiration: time.Now().Add(time.Minute),
	}
	// credit the account
	if _, err := am.Credit(req, false); err != nil {
		t.Fatal("expected successful credit", err)
	} else if sources, err := am.AccountFunding(accountID); err != nil {
		t.Fatal("expected successful funding", err)
	} else if len(sources) != 1 {
		t.Fatalf("expected 1 funding source, got %v", len(sources))
	} else if sources[0].ContractID != rev.Revision.ParentID {
		t.Fatalf("expected funding source to be %v, got %v", rev.Revision.ParentID, sources[0].ContractID)
	} else if sources[0].Amount.Cmp(expectedFunding) != 0 {
		t.Fatalf("expected funding amount to be %v, got %v", expectedFunding, sources[0].Amount)
	}

	contract, err := node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	expectedBalance := amount

	if m, err := node.Store.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedBalance) {
		t.Fatalf("expected account balance to be %v, got %v", expectedBalance, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}

	// initialize a new budget for half the account balance
	budgetAmount := amount.Div64(2)
	budget, err := am.Budget(accountID, budgetAmount)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

	// try to spend more than the budget
	if err := budget.Spend(accounts.Usage{RPCRevenue: budgetAmount.Mul64(2)}); !errors.Is(err, accounts.ErrInsufficientFunds) {
		t.Fatal("expected insufficient funds error, got", err)
	}

	// check that the in-memory state is consistent
	expectedBalance = expectedBalance.Sub(budgetAmount)
	balance, err := am.Balance(accountID)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, balance)
	}

	// spend half of the budget
	spendAmount := amount.Div64(4)
	if err := budget.Spend(accounts.Usage{RPCRevenue: spendAmount}); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state did not change
	balance, err = am.Balance(accountID)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, balance)
	}

	// create a new budget to hold the balance in-memory
	b2, err := am.Budget(accountID, types.NewCurrency64(0))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Rollback()

	// commit the budget
	expectedFunding = expectedFunding.Sub(spendAmount)
	if err := budget.Commit(); err != nil {
		t.Fatal(err)
	} else if sources, err := am.AccountFunding(accountID); err != nil {
		t.Fatal("expected successful funding", err)
	} else if len(sources) != 1 {
		t.Fatalf("expected 1 funding source, got %v", len(sources))
	} else if sources[0].ContractID != rev.Revision.ParentID {
		t.Fatalf("expected funding source to be %v, got %v", rev.Revision.ParentID, sources[0].ContractID)
	} else if sources[0].Amount.Cmp(expectedFunding) != 0 {
		t.Fatalf("expected funding amount to be %v, got %v", expectedFunding, sources[0].Amount)
	}

	// check that the contract's usage has been updated
	contract, err = node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	expectedBalance = amount.Sub(spendAmount)
	// check that the in-memory state has been updated
	balance, err = am.Balance(accountID)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, balance)
	}

	if m, err := node.Store.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedBalance) {
		t.Fatalf("expected account balance to be %v, got %v", expectedBalance, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}

	// check that the account balance has been updated and only the spent
	// amount has been deducted
	if balance, err := node.Store.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected balance to be equal to %d, got %d", expectedBalance, balance)
	}

	// spend the remainder of the account balance
	budget, err = am.Budget(accountID, expectedBalance)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

	if err := budget.Spend(accounts.Usage{RPCRevenue: expectedBalance}); err != nil {
		t.Fatal(err)
	} else if err := budget.Commit(); err != nil {
		t.Fatal(err)
	} else if sources, err := am.AccountFunding(accountID); err != nil {
		t.Fatal("expected successful funding", err)
	} else if len(sources) != 0 { // exhausted funding source should be deleted
		t.Fatalf("expected no funding sources, got %v", len(sources))
	}

	// check that the contract's usage has been updated
	contract, err = node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.IsZero() {
		t.Fatalf("expected contract usage to be %v, got %v", types.ZeroCurrency, contract.Usage.AccountFunding)
	}

	if m, err := node.Store.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.IsZero() {
		t.Fatalf("expected account balance to be %v, got %v", types.ZeroCurrency, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active accounts, got %v", m.Accounts.Active)
	}
}
