package accounts_test

import (
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/alerts"
	"go.sia.tech/hostd/host/accounts"
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
	expectedFunding := amount
	req := accounts.FundAccountWithContract{
		Account:    accountID,
		Amount:     amount,
		Cost:       types.NewCurrency64(1),
		Revision:   rev,
		Expiration: time.Now().Add(time.Minute),
	}
	if _, err := am.Credit(req, false); err != nil {
		t.Fatal("expected successful credit", err)
	} else if balance, err := db.AccountBalance(accountID); err != nil {
		t.Fatal("expected successful balance", err)
	} else if balance.Cmp(amount) != 0 {
		t.Fatalf("expected balance %v to be equal to amount %v", balance, amount)
	} else if sources, err := am.AccountFunding(accountID); err != nil {
		t.Fatal("expected successful funding", err)
	} else if len(sources) != 1 {
		t.Fatalf("expected 1 funding source, got %v", len(sources))
	} else if sources[0].ContractID != rev.Revision.ParentID {
		t.Fatalf("expected funding source to be %v, got %v", rev.Revision.ParentID, sources[0].ContractID)
	} else if sources[0].Amount.Cmp(expectedFunding) != 0 {
		t.Fatalf("expected funding amount to be %v, got %v", expectedFunding, sources[0].Amount)
	}

	contract, err := com.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedFunding) {
		t.Fatalf("expected account balance to be %v, got %v", expectedFunding, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}

	// attempt to credit the account over the max balance
	amount = types.NewCurrency64(100)
	req = accounts.FundAccountWithContract{
		Account:    accountID,
		Amount:     amount,
		Cost:       types.NewCurrency64(1),
		Revision:   rev,
		Expiration: time.Now().Add(time.Minute),
	}
	if _, err := am.Credit(req, false); err != accounts.ErrBalanceExceeded {
		t.Fatalf("expected ErrBalanceExceeded, got %v", err)
	} else if sources, err := am.AccountFunding(accountID); err != nil {
		t.Fatal("expected successful funding", err)
	} else if len(sources) != 1 {
		t.Fatalf("expected 1 funding source, got %v", len(sources))
	} else if sources[0].ContractID != rev.Revision.ParentID {
		t.Fatalf("expected funding source to be %v, got %v", rev.Revision.ParentID, sources[0].ContractID)
	} else if sources[0].Amount.Cmp(expectedFunding) != 0 {
		t.Fatalf("expected funding amount to be %v, got %v", expectedFunding, sources[0].Amount)
	}

	contract, err = com.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedFunding) {
		t.Fatalf("expected account balance to be %v, got %v", expectedFunding, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}

	// refund the account over the max balance
	expectedFunding = expectedFunding.Add(amount)
	if _, err := am.Credit(req, true); err != nil {
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

	contract, err = com.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := db.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedFunding) {
		t.Fatalf("expected account balance to be %v, got %v", expectedFunding, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}
}
