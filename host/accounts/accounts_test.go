package accounts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type ephemeralSettings struct {
	maxBalance types.Currency
}

func (s ephemeralSettings) Settings() settings.Settings {
	return settings.Settings{
		MaxAccountBalance: s.maxBalance,
	}
}

func testLog(t *testing.T) *zap.Logger {
	opt := zap.NewDevelopmentConfig()
	opt.OutputPaths = []string{filepath.Join(t.TempDir(), "hostd.log")}
	log, err := opt.Build()
	if err != nil {
		t.Fatal(err)
	}
	return log
}

func TestCredit(t *testing.T) {
	log := testLog(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}

	am := NewManager(db, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
	accountID := frand.Entropy256()

	// attempt to credit the account
	amount := types.NewCurrency64(50)
	if _, err := am.Credit(accountID, amount, time.Now().Add(time.Minute)); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := am.store.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(amount) != 0 {
		t.Fatalf("expected balance %v to be equal to amount %v", balance, amount)
	}

	// attempt to credit the account over the max balance
	amount = types.NewCurrency64(100)
	if _, err := am.Credit(accountID, amount, time.Now().Add(time.Minute)); err != ErrBalanceExceeded {
		t.Fatalf("expected ErrBalanceExceeded, got %v", err)
	}
}

func TestBudget(t *testing.T) {
	log := testLog(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}

	am := NewManager(db, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
	accountID := frand.Entropy256()

	// credit the account
	amount := types.NewCurrency64(100)
	if _, err := am.Credit(accountID, amount, time.Now().Add(time.Minute)); err != nil {
		t.Fatal("expected successful credit", err)
	}

	expectedBalance := amount

	// initialize a new budget for half the account balance
	budgetAmount := amount.Div64(2)
	budget, err := am.Budget(context.Background(), accountID, budgetAmount)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

	// check that the in-memory state is consistent
	expectedBalance = expectedBalance.Sub(budgetAmount)
	if !am.balances[accountID].balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, am.balances[accountID].balance)
	}

	// spend half of the budget
	spendAmount := amount.Div64(4)
	if err := budget.Spend(spendAmount); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state did not change
	if !am.balances[accountID].balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, am.balances[accountID].balance)
	}

	// create a new budget to hold the balance in-memory
	b2, err := am.Budget(context.Background(), accountID, types.NewCurrency64(0))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Rollback()

	// commit the budget
	if err := budget.Commit(); err != nil {
		t.Fatal(err)
	}

	expectedBalance = amount.Sub(spendAmount)
	// check that the in-memory state has been updated
	if balance, exists := am.balances[accountID]; !exists {
		t.Fatal("expected in-memory balance to exist")
	} else if !balance.balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, balance.balance)
	}

	// check that the account balance has been updated and only the spent
	// amount has been deducted
	if balance, err := am.store.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected balance to be equal to %d, got %d", expectedBalance, balance)
	}
}
