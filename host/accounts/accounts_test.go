package accounts_test

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/accounts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/internal/testutil"
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
	network, genesis := testutil.V1Network()
	node := testutil.NewHostNode(t, types.GeneratePrivateKey(), network, genesis, log)

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
	if err := node.Contracts.AddContract(rev, []types.Transaction{{}}, types.Siacoins(1), contracts.Usage{}); err != nil {
		t.Fatal(err)
	}

	am := accounts.NewManager(node.Store, ephemeralSettings{maxBalance: types.NewCurrency64(100)})
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
	} else if balance, err := node.Store.AccountBalance(accountID); err != nil {
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

	contract, err := node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := node.Store.Metrics(time.Now()); err != nil {
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

	contract, err = node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := node.Store.Metrics(time.Now()); err != nil {
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

	contract, err = node.Contracts.Contract(rev.Revision.ParentID)
	if err != nil {
		t.Fatal(err)
	} else if !contract.Usage.AccountFunding.Equals(expectedFunding) {
		t.Fatalf("expected contract usage to be %v, got %v", expectedFunding, contract.Usage.AccountFunding)
	}

	if m, err := node.Store.Metrics(time.Now()); err != nil {
		t.Fatal(err)
	} else if !m.Accounts.Balance.Equals(expectedFunding) {
		t.Fatalf("expected account balance to be %v, got %v", expectedFunding, m.Accounts.Balance)
	} else if m.Accounts.Active != 1 {
		t.Fatalf("expected 1 active account, got %v", m.Accounts.Active)
	}
}
