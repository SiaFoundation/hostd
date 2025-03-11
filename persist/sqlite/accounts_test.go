package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/index"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestRHP4Accounts(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	c, count, err := db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}

	// add a contract to the database
	contract := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey:  renterKey.PublicKey(),
			HostPublicKey:    hostKey.PublicKey(),
			ProofHeight:      100,
			ExpirationHeight: 200,
		},
	}

	if err := db.AddV2Contract(contract, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
	}

	checkMetricConsistency := func(t *testing.T, potential, earned proto4.Usage) {
		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case m.Revenue.Potential.Ingress != potential.Ingress:
			t.Fatalf("expected potential ingress %v, got %v", potential.Ingress, m.Revenue.Potential.Ingress)
		case m.Revenue.Potential.Egress != potential.Egress:
			t.Fatalf("expected potential egress %v, got %v", potential.Egress, m.Revenue.Potential.Egress)
		case m.Revenue.Potential.Storage != potential.Storage:
			t.Fatalf("expected storage %v, got %v", potential.Storage, m.Revenue.Potential.Storage)
		case m.Revenue.Potential.RPC != potential.RPC:
			t.Fatalf("expect RPC %v, got %v", potential.RPC, m.Revenue.Potential.RPC)
		case m.Revenue.Earned.Ingress != earned.Ingress:
			t.Fatalf("expected ingress %v, got %v", earned.Ingress, m.Revenue.Earned.Ingress)
		case m.Revenue.Earned.Egress != earned.Egress:
			t.Fatalf("expected egree %v, got %v", earned.Egress, m.Revenue.Earned.Egress)
		case m.Revenue.Earned.Storage != earned.Storage:
			t.Fatalf("expected storage %v, got %v", earned.Storage, m.Revenue.Earned.Storage)
		case m.Revenue.Earned.RPC != earned.RPC:
			t.Fatalf("expected RPC %v, got %v", earned.RPC, m.Revenue.Earned.RPC)
		}
	}

	sk := types.GeneratePrivateKey()
	account := proto4.Account(sk.PublicKey())

	balance, err := db.RHP4AccountBalance(account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.IsZero() {
		t.Fatal("expected balance to be 0")
	}

	// deposit funds
	balances, err := db.RHP4CreditAccounts([]proto4.AccountDeposit{
		{Account: account, Amount: types.Siacoins(10)},
	}, contract.ID, contract.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(10)})
	if err != nil {
		t.Fatal(err)
	} else if len(balances) != 1 {
		t.Fatalf("expected %d balances, got %d", 1, len(balances))
	} else if !balances[0].Equals(types.Siacoins(10)) {
		t.Fatalf("expected balance %v, got %v", types.Siacoins(10), balances[0])
	}

	balance, err = db.RHP4AccountBalance(account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.Siacoins(10)) {
		t.Fatalf("expected balance to be %v, got %v", types.Siacoins(10), balance)
	}

	// try to spend more than the account balance
	expectedUsage := proto4.Usage{
		Ingress: types.Siacoins(15),
	}
	if err := db.RHP4DebitAccount(account, expectedUsage); err == nil {
		t.Fatalf("expected insufficient funds error")
	}

	// spend some funds
	expectedUsage = proto4.Usage{
		Storage: types.Siacoins(3),
		Ingress: types.Siacoins(2),
		Egress:  types.Siacoins(1),
	}
	if err := db.RHP4DebitAccount(account, expectedUsage); err != nil {
		t.Fatal(err)
	}

	balance, err = db.RHP4AccountBalance(account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(types.Siacoins(4)) {
		t.Fatalf("expected balance to be %v, got %v", types.Siacoins(4), balance)
	}

	// pending accounts do not affect metrics
	checkMetricConsistency(t, proto4.Usage{}, proto4.Usage{})

	// confirm the contract
	err = db.UpdateChainState(func(itx index.UpdateTx) error {
		return itx.ApplyContracts(types.ChainIndex{}, contracts.StateChanges{
			ConfirmedV2: []types.V2FileContractElement{
				{
					ID: contract.ID,
				},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that the metrics now reflect the spending
	checkMetricConsistency(t, expectedUsage, proto4.Usage{})

	// try to spend more than the account balance
	err = db.RHP4DebitAccount(account, proto4.Usage{
		Ingress: types.Siacoins(15),
	})
	if err == nil {
		t.Fatalf("expected insufficient funds error")
	}

	// check that the metrics did not change
	checkMetricConsistency(t, expectedUsage, proto4.Usage{})

	// spend the rest of the balance
	err = db.RHP4DebitAccount(account, proto4.Usage{
		RPC: types.Siacoins(4),
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedUsage.RPC = types.Siacoins(4)
	// check that the metrics did not change
	checkMetricConsistency(t, expectedUsage, proto4.Usage{})

	err = db.UpdateChainState(func(itx index.UpdateTx) error {
		return itx.ApplyContracts(types.ChainIndex{}, contracts.StateChanges{
			SuccessfulV2: []types.FileContractID{
				contract.ID,
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that the metrics were confirmed
	checkMetricConsistency(t, proto4.Usage{}, expectedUsage)
}

func TestRHP4AccountsDistribution(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	renterKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))
	hostKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	c, count, err := db.V2Contracts(contracts.V2ContractFilter{})
	if err != nil {
		t.Fatal(err)
	} else if len(c) != 0 {
		t.Fatal("expected no contracts")
	} else if count != 0 {
		t.Fatal("expected no contracts")
	}

	// add two contracts to the database
	c1 := contracts.V2Contract{
		ID: frand.Entropy256(),
		V2FileContract: types.V2FileContract{
			RenterPublicKey: renterKey.PublicKey(),
			HostPublicKey:   hostKey.PublicKey(),
		},
	}

	if err := db.AddV2Contract(c1, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
	}

	c2 := contracts.V2Contract{
		ID:             frand.Entropy256(),
		V2FileContract: types.V2FileContract{},
	}
	if err := db.AddV2Contract(c2, rhp4.TransactionSet{}); err != nil {
		t.Fatal(err)
	}

	// confirm both contracts
	err = db.UpdateChainState(func(itx index.UpdateTx) error {
		return itx.ApplyContracts(types.ChainIndex{}, contracts.StateChanges{
			ConfirmedV2: []types.V2FileContractElement{
				{
					ID: c1.ID,
				},
				{
					ID: c2.ID,
				},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	checkUsageConsistency := func(t *testing.T, expected proto4.Usage) {
		t.Helper()

		contracts, _, err := db.V2Contracts(contracts.V2ContractFilter{})
		if err != nil {
			t.Fatal(err)
		}
		var total proto4.Usage
		for _, c := range contracts {
			total = total.Add(c.Usage)
		}

		switch {
		case total.Ingress != expected.Ingress:
			t.Fatalf("expected ingress %v, got %v", expected.Ingress, total.Ingress)
		case total.Egress != expected.Egress:
			t.Fatalf("expected egress %v, got %v", expected.Egress, total.Egress)
		case total.Storage != expected.Storage:
			t.Fatalf("expected storage %v, got %v", expected.Storage, total.Storage)
		case total.RPC != expected.RPC:
			t.Fatalf("expected rpc %v, got %v", expected.RPC, total.RPC)
		case total.RiskedCollateral != expected.RiskedCollateral:
			t.Fatalf("expected risked collateral %v, got %v", expected.RiskedCollateral, total.RiskedCollateral)
		case total.AccountFunding != expected.AccountFunding:
			t.Fatalf("expected account funding %v, got %v", expected.AccountFunding, total.AccountFunding)
		}
	}

	checkMetricConsistency := func(t *testing.T, potential, earned proto4.Usage) {
		t.Helper()

		m, err := db.Metrics(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case m.Revenue.Potential.Ingress != potential.Ingress:
			t.Fatalf("expected potential ingress %v, got %v", potential.Ingress, m.Revenue.Potential.Ingress)
		case m.Revenue.Potential.Egress != potential.Egress:
			t.Fatalf("expected potential egress %v, got %v", potential.Egress, m.Revenue.Potential.Egress)
		case m.Revenue.Potential.Storage != potential.Storage:
			t.Fatalf("expected storage %v, got %v", potential.Storage, m.Revenue.Potential.Storage)
		case m.Revenue.Potential.RPC != potential.RPC:
			t.Fatalf("expect RPC %v, got %v", potential.RPC, m.Revenue.Potential.RPC)
		case m.Revenue.Earned.Ingress != earned.Ingress:
			t.Fatalf("expected ingress %v, got %v", earned.Ingress, m.Revenue.Earned.Ingress)
		case m.Revenue.Earned.Egress != earned.Egress:
			t.Fatalf("expected egree %v, got %v", earned.Egress, m.Revenue.Earned.Egress)
		case m.Revenue.Earned.Storage != earned.Storage:
			t.Fatalf("expected storage %v, got %v", earned.Storage, m.Revenue.Earned.Storage)
		case m.Revenue.Earned.RPC != earned.RPC:
			t.Fatalf("expected RPC %v, got %v", earned.RPC, m.Revenue.Earned.RPC)
		}
	}

	sk := types.GeneratePrivateKey()
	account := proto4.Account(sk.PublicKey())

	balances, err := db.RHP4CreditAccounts([]proto4.AccountDeposit{
		{Account: account, Amount: types.Siacoins(3)},
	}, c1.ID, c1.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(3)})
	if err != nil {
		t.Fatal(err)
	} else if len(balances) != 1 {
		t.Fatalf("expected %d balances, got %d", 1, len(balances))
	} else if !balances[0].Equals(types.Siacoins(3)) {
		t.Fatalf("expected balance %v, got %v", types.Siacoins(3), balances[0])
	}

	checkUsageConsistency(t, proto4.Usage{
		AccountFunding: types.Siacoins(3),
	})

	balances, err = db.RHP4CreditAccounts([]proto4.AccountDeposit{
		{Account: account, Amount: types.Siacoins(3)},
	}, c2.ID, c2.V2FileContract, proto4.Usage{AccountFunding: types.Siacoins(3)})
	if err != nil {
		t.Fatal(err)
	} else if len(balances) != 1 {
		t.Fatalf("expected %d balances, got %d", 1, len(balances))
	} else if !balances[0].Equals(types.Siacoins(6)) {
		t.Fatalf("expected balance %v, got %v", types.Siacoins(6), balances[0])
	}

	checkMetricConsistency(t, proto4.Usage{}, proto4.Usage{})
	checkUsageConsistency(t, proto4.Usage{
		AccountFunding: types.Siacoins(6),
	})

	err = db.RHP4DebitAccount(account, proto4.Usage{
		RPC: types.Siacoins(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	checkMetricConsistency(t, proto4.Usage{
		RPC: types.Siacoins(1),
	}, proto4.Usage{})
	checkUsageConsistency(t, proto4.Usage{
		RPC:            types.Siacoins(1),
		AccountFunding: types.Siacoins(5),
	})

	err = db.RHP4DebitAccount(account, proto4.Usage{
		Storage: types.Siacoins(3),
	})
	if err != nil {
		t.Fatal(err)
	}

	checkMetricConsistency(t, proto4.Usage{
		RPC:     types.Siacoins(1),
		Storage: types.Siacoins(3),
	}, proto4.Usage{})
	checkUsageConsistency(t, proto4.Usage{
		RPC:            types.Siacoins(1),
		Storage:        types.Siacoins(3),
		AccountFunding: types.Siacoins(2),
	})
}
