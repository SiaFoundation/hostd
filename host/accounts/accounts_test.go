package accounts_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/internal/store"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

func newTestAccountManager(t *testing.T) *accounts.AccountManager {
	cm, err := settings.NewConfigManager(store.NewEphemeralSettingsStore())
	if err != nil {
		panic(err)
	}
	cm.UpdateSettings(settings.Settings{
		MaxAccountBalance: types.SiacoinPrecision,
	})
	am := accounts.NewManager(store.NewEphemeralAccountStore(), cm)
	t.Cleanup(func() { am.Close() })
	return am
}

func testCancelledDebit(t *testing.T) {
	ac := newTestAccountManager(t)
	accountID := frand.Entropy256()

	// start a new context and immediately cancel it
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// attempt to withdraw from the account
	if _, err := ac.Debit(ctx, accountID, types.NewCurrency64(50)); !errors.Is(err, context.Canceled) {
		t.Fatal("expected context to be cancelled", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// attempt to withdraw from the account
	if _, err := ac.Debit(ctx, accountID, types.NewCurrency64(50)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expected context to be deadline exceeded", err)
	}
}

func testBlockedDebit(t *testing.T) {
	ac := newTestAccountManager(t)
	accountID := frand.Entropy256()

	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := ac.Credit(accountID, types.NewCurrency64(25)); err != nil {
				panic(fmt.Errorf("expected no error: %w", err))
			}
		}
	}()

	waitingDebits := 10
	var wg sync.WaitGroup
	wg.Add(waitingDebits)
	for i := 0; i < waitingDebits; i++ {
		go func(i int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// attempt to withdraw from the account
			if _, err := ac.Debit(ctx, accountID, types.NewCurrency64(50)); err != nil {
				panic(fmt.Errorf("expected successful withdrawal, got %v", err))
			}
		}(i)
	}

	wg.Wait()
}

func TestDebit(t *testing.T) {
	t.Run("blocked withdrawal", testBlockedDebit)
	t.Run("cancelled withdrawal", testCancelledDebit)
}

func TestCredit(t *testing.T) {
	ac := newTestAccountManager(t)
	accountID := frand.Entropy256()

	// attempt to credit the account
	amount := types.NewCurrency64(50)
	if _, err := ac.Credit(accountID, amount); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := ac.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(amount) != 0 {
		t.Fatal("expected balance to be equal to amount", balance, amount)
	}

	// attempt to credit the account over the max balance
	amount = types.SiacoinPrecision
	if _, err := ac.Credit(accountID, amount); err == nil {
		t.Fatal("expected failed credit")
	}
}

func TestRefund(t *testing.T) {
	ac := newTestAccountManager(t)
	accountID := frand.Entropy256()

	// fill the account to the max balance
	fundAmount, expectedBalance := types.SiacoinPrecision, types.SiacoinPrecision
	if _, err := ac.Credit(accountID, fundAmount); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := ac.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(expectedBalance) != 0 {
		t.Fatal("expected balance to be equal to amount", balance, expectedBalance)
	}

	// refund the account another 50H; refunds do not consider the max balance
	fundAmount = types.NewCurrency64(50)
	expectedBalance = expectedBalance.Add(fundAmount)
	if _, err := ac.Refund(accountID, fundAmount); err != nil {
		t.Fatal("expected successful refund", err)
	} else if balance, err := ac.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(expectedBalance) != 0 {
		t.Fatal("expected balance to be equal to amount", balance, expectedBalance)
	}

	// withdraw from the account
	debitAmount := types.SiacoinPrecision
	expectedBalance = expectedBalance.Sub(debitAmount)
	if _, err := ac.Debit(context.Background(), accountID, debitAmount); err != nil {
		t.Fatal("expected successful debit", err)
	} else if balance, err := ac.Balance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(expectedBalance) != 0 {
		t.Fatal("expected balance to be equal to amount", balance, expectedBalance)
	}
}
