package contracts_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	rhp2 "go.sia.tech/core/rhp/v2"
	crhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/internal/test"
	rhp3 "go.sia.tech/hostd/internal/test/rhp/v3"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func checkRevenueConsistency(s *sqlite.Store, potential, earned metrics.Revenue) error {
	time.Sleep(time.Second) // commit time

	m, err := s.Metrics(time.Now())
	if err != nil {
		return fmt.Errorf("failed to get metrics: %v", err)
	}

	actualPotentialValue := reflect.ValueOf(m.Revenue.Potential)
	expPotentialValue := reflect.ValueOf(potential)
	for i := 0; i < actualPotentialValue.NumField(); i++ {
		name := actualPotentialValue.Type().Field(i).Name
		fa, fe := actualPotentialValue.Field(i), expPotentialValue.Field(i)
		av, ev := fa.Interface().(types.Currency), fe.Interface().(types.Currency)

		if !av.Equals(ev) {
			return fmt.Errorf("potential revenue field %q does not match. expected %d, got %d", name, ev, av)
		}
	}

	actualEarnedValue := reflect.ValueOf(m.Revenue.Earned)
	expEarnedValue := reflect.ValueOf(earned)
	for i := 0; i < actualEarnedValue.NumField(); i++ {
		name := actualEarnedValue.Type().Field(i).Name
		fa, fe := actualEarnedValue.Field(i), expEarnedValue.Field(i)
		av, ev := fa.Interface().(types.Currency), fe.Interface().(types.Currency)

		if !av.Equals(ev) {
			return fmt.Errorf("earned revenue field %q does not match. expected %d, got %d", name, ev, av)
		}
	}

	return nil
}

func TestRevenueMetrics(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		renter, host, err := test.NewTestingPair(t.TempDir(), zaptest.NewLogger(t))
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		defer renter.Close()

		var expectedPotential, expectedEarned metrics.Revenue
		// check that the host has no revenue
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		settings, err := host.RHP2Settings()
		if err != nil {
			t.Fatal(err)
		}

		revision, err := renter.FormContract(context.Background(), host.RHP2Addr(), host.PublicKey(), types.Siacoins(100), types.Siacoins(200), host.TipState().Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(settings.ContractPrice)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the contract is active
		if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		// start an RHP3 session
		sess, err := renter.NewRHP3Session(context.Background(), host.RHP3Addr(), host.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()

		accountID := crhp3.Account(renter.PublicKey())
		contractPayment := rhp3.ContractPayment(&revision, renter.PrivateKey(), accountID)
		// register a price table
		pt, err := sess.RegisterPriceTable(contractPayment)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.UpdatePriceTableCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund an account
		if _, err := sess.FundAccount(accountID, contractPayment, types.Siacoins(1)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund the account again
		if _, err := sess.FundAccount(accountID, contractPayment, types.Siacoins(2)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		accountPayment := rhp3.AccountPayment(accountID, renter.PrivateKey())
		// upload a sector
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhp2.SectorRoot(&sector)
		usage := pt.BaseCost().Add(pt.AppendSectorCost(revision.Revision.WindowEnd - pt.HostBlockHeight))
		budget, _ := usage.Total()
		_, err = sess.AppendSector(&sector, &revision, renter.PrivateKey(), accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Storage = expectedPotential.Storage.Add(usage.Storage)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)
		time.Sleep(100 * time.Millisecond) // commit time
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// read a sector
		usage = pt.BaseCost().Add(pt.ReadSectorCost(rhp2.SectorSize))
		budget, _ = usage.Total()
		_, _, err = sess.ReadSector(root, 0, rhp2.SectorSize, accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)

		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the contract is successful
		if err := host.MineBlocks(host.WalletAddress(), int(revision.Revision.WindowEnd-host.TipState().Index.Height+1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// check that the revenue metrics were updated
		expectedEarned = expectedPotential
		expectedPotential = metrics.Revenue{}
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// register a new price table using the account funding after the
		// contract has expired
		pt, err = sess.RegisterPriceTable(accountPayment)
		if err != nil {
			t.Fatal(err)
		}
		expectedEarned.RPC = expectedEarned.RPC.Add(pt.UpdatePriceTableCost)

		// check that the earned revenue metrics were updated since the contract
		// was successful
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// read a sector using the account funding after the contract has expired
		usage = pt.BaseCost().Add(pt.ReadSectorCost(rhp2.SectorSize))
		budget, _ = usage.Total()
		_, _, err = sess.ReadSector(root, 0, rhp2.SectorSize, accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedEarned.RPC = expectedEarned.RPC.Add(usage.Base)
		expectedEarned.Ingress = expectedEarned.Ingress.Add(usage.Ingress)
		expectedEarned.Egress = expectedEarned.Egress.Add(usage.Egress)

		// check that the earned revenue metrics were updated since the contract
		// was successful
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		renter, host, err := test.NewTestingPair(t.TempDir(), zaptest.NewLogger(t))
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		defer renter.Close()

		var expectedPotential, expectedEarned metrics.Revenue
		// check that the host has no revenue
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		settings, err := host.RHP2Settings()
		if err != nil {
			t.Fatal(err)
		}

		revision, err := renter.FormContract(context.Background(), host.RHP2Addr(), host.PublicKey(), types.Siacoins(100), types.Siacoins(200), host.TipState().Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(settings.ContractPrice)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the contract is active
		if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		// start an RHP3 session
		sess, err := renter.NewRHP3Session(context.Background(), host.RHP3Addr(), host.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()

		accountID := crhp3.Account(renter.PublicKey())
		contractPayment := rhp3.ContractPayment(&revision, renter.PrivateKey(), accountID)
		// register a price table
		pt, err := sess.RegisterPriceTable(contractPayment)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.UpdatePriceTableCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund an account
		if _, err := sess.FundAccount(accountID, contractPayment, types.Siacoins(1)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund the account again
		if _, err := sess.FundAccount(accountID, contractPayment, types.Siacoins(2)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		accountPayment := rhp3.AccountPayment(accountID, renter.PrivateKey())
		// upload a sector
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhp2.SectorRoot(&sector)
		usage := pt.BaseCost().Add(pt.AppendSectorCost(revision.Revision.WindowEnd - pt.HostBlockHeight))
		budget, _ := usage.Total()
		_, err = sess.AppendSector(&sector, &revision, renter.PrivateKey(), accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Storage = expectedPotential.Storage.Add(usage.Storage)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)
		time.Sleep(100 * time.Millisecond) // commit time
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// read the sector
		usage = pt.BaseCost().Add(pt.ReadSectorCost(rhp2.SectorSize))
		budget, _ = usage.Total()
		_, _, err = sess.ReadSector(root, 0, rhp2.SectorSize, accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)

		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// force remove the sector so a proof can't be submitted
		if err := host.Storage().RemoveSector(root); err != nil {
			t.Fatal(err)
		}

		// mine until the contract is expired
		if err := host.MineBlocks(host.WalletAddress(), int(revision.Revision.WindowEnd-host.TipState().Index.Height+1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// check that the revenue metrics were updated
		expectedEarned = metrics.Revenue{} // failed contracts do not earn revenue
		expectedPotential = metrics.Revenue{}
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// register a new price table using the account funding after the
		// contract has expired
		pt, err = sess.RegisterPriceTable(accountPayment)
		if err != nil {
			t.Fatal(err)
		}

		// check that the earned revenue metrics were not updated since the
		// contract failed
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("multi-contract", func(t *testing.T) {
		renter, host, err := test.NewTestingPair(t.TempDir(), zaptest.NewLogger(t))
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		defer renter.Close()

		var expectedPotential, expectedEarned metrics.Revenue
		// check that the host has no revenue
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		settings, err := host.RHP2Settings()
		if err != nil {
			t.Fatal(err)
		}

		r1, err := renter.FormContract(context.Background(), host.RHP2Addr(), host.PublicKey(), types.Siacoins(100), types.Siacoins(200), host.TipState().Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(settings.ContractPrice)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the contract is active
		if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		// form a second contract
		r2, err := renter.FormContract(context.Background(), host.RHP2Addr(), host.PublicKey(), types.Siacoins(100), types.Siacoins(200), host.TipState().Index.Height+200)
		if err != nil {
			t.Fatal(err)
		}

		// mine until the contract is active
		if err := host.MineBlocks(host.WalletAddress(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // sync time

		expectedPotential.RPC = expectedPotential.RPC.Add(settings.ContractPrice)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// start an RHP3 session
		sess, err := renter.NewRHP3Session(context.Background(), host.RHP3Addr(), host.PublicKey())
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()

		accountID := crhp3.Account(renter.PublicKey())
		contractPayment := rhp3.ContractPayment(&r1, renter.PrivateKey(), accountID)
		// register a price table
		pt, err := sess.RegisterPriceTable(contractPayment)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.UpdatePriceTableCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund an account using the first contract
		if _, err := sess.FundAccount(accountID, contractPayment, types.NewCurrency64(1)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// fund the account again using the second contract
		if _, err := sess.FundAccount(accountID, rhp3.ContractPayment(&r2, renter.PrivateKey(), accountID), types.Siacoins(1)); err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.FundAccountCost)
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		accountPayment := rhp3.AccountPayment(accountID, renter.PrivateKey())
		// upload a sector
		var sector [rhp2.SectorSize]byte
		frand.Read(sector[:256])
		root := rhp2.SectorRoot(&sector)
		usage := pt.BaseCost().Add(pt.AppendSectorCost(r1.Revision.WindowEnd - pt.HostBlockHeight))
		budget, _ := usage.Total()
		_, err = sess.AppendSector(&sector, &r1, renter.PrivateKey(), accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Storage = expectedPotential.Storage.Add(usage.Storage)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)
		time.Sleep(100 * time.Millisecond) // commit time
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// read a sector
		usage = pt.BaseCost().Add(pt.ReadSectorCost(rhp2.SectorSize))
		budget, _ = usage.Total()
		_, _, err = sess.ReadSector(root, 0, rhp2.SectorSize, accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(usage.Base)
		expectedPotential.Ingress = expectedPotential.Ingress.Add(usage.Ingress)
		expectedPotential.Egress = expectedPotential.Egress.Add(usage.Egress)

		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the first contract is successful
		if err := host.MineBlocks(host.WalletAddress(), int(r1.Revision.WindowEnd-host.TipState().Index.Height+1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		contract, err := host.Contracts().Contract(r1.Revision.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		// subtract the successful contract's revenue from the expected potential revenue
		expectedPotential.RPC = expectedPotential.RPC.Sub(contract.Usage.RPCRevenue)
		expectedPotential.Storage = expectedPotential.Storage.Sub(contract.Usage.StorageRevenue)
		expectedPotential.Ingress = expectedPotential.Ingress.Sub(contract.Usage.IngressRevenue)
		expectedPotential.Egress = expectedPotential.Egress.Sub(contract.Usage.EgressRevenue)
		expectedEarned.RPC = contract.Usage.RPCRevenue
		expectedEarned.Storage = contract.Usage.StorageRevenue
		expectedEarned.Ingress = contract.Usage.IngressRevenue
		expectedEarned.Egress = contract.Usage.EgressRevenue
		// check that the revenue metrics were updated
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// register a new price table using the account funding after the
		// contract has expired. All revenue should be going to the second
		// contract.
		pt, err = sess.RegisterPriceTable(accountPayment)
		if err != nil {
			t.Fatal(err)
		}
		expectedPotential.RPC = expectedPotential.RPC.Add(pt.UpdatePriceTableCost)

		// check that the earned revenue metrics were updated since the contract
		// was successful
		if err := checkRevenueConsistency(host.Store(), expectedPotential, expectedEarned); err != nil {
			t.Fatal(err)
		}

		// mine until the second contract is successful
		if err := host.MineBlocks(host.WalletAddress(), int(r2.Revision.WindowEnd-host.TipState().Index.Height+1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // sync time

		// all revenue should now be earned
		expectedEarned.RPC = expectedEarned.RPC.Add(expectedPotential.RPC)
		expectedEarned.Storage = expectedEarned.Storage.Add(expectedPotential.Storage)
		expectedEarned.Ingress = expectedEarned.Ingress.Add(expectedPotential.Ingress)
		expectedEarned.Egress = expectedEarned.Egress.Add(expectedPotential.Egress)
		expectedPotential = metrics.Revenue{}

		// read a sector using the account funding after the second contract has
		// expired. All revenue should be earned
		usage = pt.BaseCost().Add(pt.ReadSectorCost(rhp2.SectorSize))
		budget, _ = usage.Total()
		_, _, err = sess.ReadSector(root, 0, rhp2.SectorSize, accountPayment, budget)
		if err != nil {
			t.Fatal(err)
		}
		expectedEarned.RPC = expectedEarned.RPC.Add(usage.Base)
		expectedEarned.Ingress = expectedEarned.Ingress.Add(usage.Ingress)
		expectedEarned.Egress = expectedEarned.Egress.Add(usage.Egress)
	})
}
