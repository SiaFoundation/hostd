package accounts_test

import (
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/accounts"
	"go.sia.tech/hostd/host/alerts"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/chain"
	"go.sia.tech/hostd/persist/sqlite"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.uber.org/zap/zaptest"
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

func TestCredit(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	stp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		t.Fatal(err)
	}
	tp := chain.NewTPool(stp)
	defer tp.Close()

	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	w, err := wallet.NewSingleAddressWallet(types.NewPrivateKeyFromSeed(frand.Bytes(32)), cm, tp, db, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	a := alerts.NewManager()
	sm, err := storage.NewVolumeManager(db, a, cm, log.Named("storage"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	com, err := contracts.NewManager(db, a, sm, cm, tp, w, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

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
	if err := com.AddContract(rev, []types.Transaction{{}}, types.Siacoins(1), contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	am := accounts.NewManager(db, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
	accountID := frand.Entropy256()

	// attempt to credit the account
	amount := types.NewCurrency64(50)
	if _, err := am.Credit(accountID, amount, rev.Revision.ParentID, time.Now().Add(time.Minute), false); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := db.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(amount) != 0 {
		t.Fatalf("expected balance %v to be equal to amount %v", balance, amount)
	}

	// attempt to credit the account over the max balance
	amount = types.NewCurrency64(100)
	if _, err := am.Credit(accountID, amount, rev.Revision.ParentID, time.Now().Add(time.Minute), false); err != accounts.ErrBalanceExceeded {
		t.Fatalf("expected ErrBalanceExceeded, got %v", err)
	}

	// refund the account over the max balance
	if _, err := am.Credit(accountID, amount, rev.Revision.ParentID, time.Now().Add(time.Minute), true); err != nil {
		t.Fatal("expected successful credit", err)
	}
}

func TestBudget(t *testing.T) {
	log := zaptest.NewLogger(t)
	dir := t.TempDir()
	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("accounts"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	g, err := gateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	cs, errCh := consensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	stp, err := transactionpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		t.Fatal(err)
	}
	tp := chain.NewTPool(stp)
	defer tp.Close()

	cm, err := chain.NewManager(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	w, err := wallet.NewSingleAddressWallet(types.NewPrivateKeyFromSeed(frand.Bytes(32)), cm, tp, db, log.Named("wallet"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	a := alerts.NewManager()
	sm, err := storage.NewVolumeManager(db, a, cm, log.Named("storage"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	com, err := contracts.NewManager(db, a, sm, cm, tp, w, log.Named("contracts"))
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

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
	if err := com.AddContract(rev, []types.Transaction{{}}, types.Siacoins(1), contracts.Usage{AccountFunding: amount}); err != nil {
		t.Fatal(err)
	}

	am := accounts.NewManager(db, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
	accountID := frand.Entropy256()
	// credit the account
	if _, err := am.Credit(accountID, amount, rev.Revision.ParentID, time.Now().Add(time.Minute), false); err != nil {
		t.Fatal("expected successful credit", err)
	}

	expectedBalance := amount

	// initialize a new budget for half the account balance
	budgetAmount := amount.Div64(2)
	budget, err := am.Budget(accountID, budgetAmount)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Rollback()

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
	if err := budget.Commit(); err != nil {
		t.Fatal(err)
	}

	expectedBalance = amount.Sub(spendAmount)
	// check that the in-memory state has been updated
	balance, err = am.Balance(accountID)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected in-memory balance to be %d, got %d", expectedBalance, balance)
	}

	// check that the account balance has been updated and only the spent
	// amount has been deducted
	if balance, err := db.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if !balance.Equals(expectedBalance) {
		t.Fatalf("expected balance to be equal to %d, got %d", expectedBalance, balance)
	}
}
