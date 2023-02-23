package accounts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/persist/sqlite"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestCredit(t *testing.T) {
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}

	am := NewManager(db)
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
}

func TestBudget(t *testing.T) {
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}

	am := NewManager(db)
	accountID := frand.Entropy256()

	// credit the account
	amount := types.NewCurrency64(50)
	if _, err := am.Credit(accountID, amount, time.Now().Add(time.Minute)); err != nil {
		t.Fatal("expected successful credit", err)
	}

	expectedBalance := amount

	// initialize a new budget for half the account balance
	budget, err := am.Budget(context.Background(), accountID, amount.Div64(2))
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

	// check that the in-memory state is consistent
	budgetAmount := amount.Div64(2)
	expectedBalance = expectedBalance.Sub(budgetAmount)
	if am.balances[accountID].balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected in-memory balance to be %v, got %v", expectedBalance, am.balances[accountID].balance)
	}

	// spend half of the budget
	spendAmount := amount.Div64(4)
	if err := budget.Spend(spendAmount); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state did not change
	if am.balances[accountID].balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected in-memory balance to be %v, got %v", expectedBalance, am.balances[accountID].balance)
	}

	// commit the budget
	if err := budget.Commit(); err != nil {
		t.Fatal(err)
	}

	// check that the in-memory state has been cleared
	if _, exists := am.balances[accountID]; exists {
		t.Fatal("expected in-memory balance to be cleared")
	}

	// check that the account balance has been updated and only the spent
	// amount has been deducted
	expectedBalance = expectedBalance.Add(budgetAmount.Sub(spendAmount))
	if balance, err := am.store.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(expectedBalance) != 0 {
		t.Fatalf("expected balance to be equal to %v, got %v", expectedBalance, balance)
	}
}
