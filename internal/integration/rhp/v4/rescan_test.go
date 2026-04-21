package rhp_test

import (
	"context"
	"errors"
	"testing"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	coretestutil "go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/index"
	"go.sia.tech/hostd/v2/internal/testutil"
	"go.uber.org/zap"
)

func syncWallet(t testing.TB, cm *chain.Manager, w *wallet.SingleAddressWallet, store *coretestutil.EphemeralWalletStore) {
	t.Helper()

	for {
		tip, err := store.Tip()
		if err != nil {
			t.Fatal(err)
		} else if tip == cm.Tip() {
			return
		}

		reverted, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func restartHostWithWalletKey(t testing.TB, hn *testutil.HostNode, hostKey, walletKey types.PrivateKey, log *zap.Logger) {
	t.Helper()

	hn.Indexer.Close()
	if err := hn.Settings.Close(); err != nil {
		t.Fatal(err)
	} else if err := hn.Contracts.Close(); err != nil {
		t.Fatal(err)
	} else if err := hn.Wallet.Close(); err != nil {
		t.Fatal(err)
	}

	// Match hostd startup behavior when the configured recovery phrase changes:
	// reset wallet/chain-indexed state, update the wallet hash, and then start
	// the same host identity with a wallet derived from the new seed.
	walletHash := types.HashBytes(walletKey[:])
	if err := hn.Store.VerifyWalletKey(walletHash); errors.Is(err, wallet.ErrDifferentSeed) {
		if err := hn.Store.ResetChainState(); err != nil {
			t.Fatal(err)
		} else if err := hn.Store.UpdateWalletHash(walletHash); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}

	wm, err := wallet.NewSingleAddressWallet(walletKey, hn.Chain, hn.Store, &testutil.MockSyncer{})
	if err != nil {
		t.Fatal("failed to create wallet:", err)
	}
	t.Cleanup(func() { wm.Close() })

	cm, err := contracts.NewManager(hn.Store, hn.Volumes, hn.Chain, wm, contracts.WithRejectAfter(10), contracts.WithRevisionSubmissionBuffer(5), contracts.WithLog(log))
	if err != nil {
		t.Fatal("failed to create contracts manager:", err)
	}
	t.Cleanup(func() { cm.Close() })

	initialSettings := settings.DefaultSettings
	initialSettings.AcceptingContracts = true
	initialSettings.NetAddress = "127.0.0.1"
	initialSettings.WindowSize = 10
	sm, err := settings.NewConfigManager(hostKey, hn.Store, hn.Chain, hn.Volumes, wm, settings.WithAnnounceInterval(10), settings.WithValidateNetAddress(false), settings.WithInitialSettings(initialSettings), settings.WithCertificates(hn.Certs), settings.WithSyncerLimits(hn.SyncerIngressLimiter, hn.SyncerEgressLimiter))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sm.Close() })

	idx, err := index.NewManager(hn.Store, hn.Chain, cm, wm, sm, hn.Volumes, index.WithLog(log.Named("index")), index.WithBatchSize(1))
	if err != nil {
		t.Fatal("failed to create index manager:", err)
	}
	t.Cleanup(func() { idx.Close() })

	hn.Wallet = wm
	hn.Contracts = cm
	hn.Settings = sm
	hn.Indexer = idx
	testutil.WaitForSync(t, hn.Chain, hn.Indexer)
}

func TestRenewHostWalletSeedChange(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	log := zap.NewNop()

	hn := testutil.NewHostNode(t, hostKey, n, genesis, log)
	testutil.MineAndSync(t, hn, hn.Wallet.Address(), int(n.MaturityDelay+20))

	renterStore := coretestutil.NewEphemeralWalletStore()
	renterWallet, err := wallet.NewSingleAddressWallet(renterKey, hn.Chain, renterStore, &testutil.MockSyncer{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { renterWallet.Close() })
	testutil.MineBlocks(t, &hn.ConsensusNode, renterWallet.Address(), int(n.MaturityDelay+20))
	testutil.WaitForSync(t, hn.Chain, hn.Indexer)
	syncWallet(t, hn.Chain, renterWallet, renterStore)

	transport := testRenterHostPair(t, hostKey, hn, log.Named("renterhost"))
	settingsResp, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	renterSigner := &fundAndSign{w: renterWallet, pk: renterKey}
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, hn.Chain, renterSigner, hn.Chain.TipState(), settingsResp.Prices, hostKey.PublicKey(), settingsResp.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   renterWallet.Address(),
		Allowance:       types.Siacoins(100),
		Collateral:      types.Siacoins(200),
		ProofHeight:     hn.Chain.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hn.Chain.AddV2PoolTransactions(formResult.FormationSet.Basis, formResult.FormationSet.Transactions); err != nil {
		t.Fatal(err)
	}
	testutil.MineAndSync(t, hn, types.VoidAddress, 10)
	syncWallet(t, hn.Chain, renterWallet, renterStore)

	oldHostAddress := formResult.Contract.Revision.HostOutput.Address
	restartHostWithWalletKey(t, hn, hostKey, types.GeneratePrivateKey(), log.Named("restart"))
	if hn.Wallet.Address() == oldHostAddress {
		t.Fatal("host wallet address did not change")
	}

	testutil.MineAndSync(t, hn, hn.Wallet.Address(), int(n.MaturityDelay+20))
	syncWallet(t, hn.Chain, renterWallet, renterStore)

	transport = testRenterHostPair(t, hostKey, hn, log.Named("renterhost-restarted"))
	settingsResp, err = rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	} else if settingsResp.WalletAddress != hn.Wallet.Address() {
		t.Fatalf("expected host settings wallet address %v, got %v", hn.Wallet.Address(), settingsResp.WalletAddress)
	}

	renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, hn.Chain, renterSigner, hn.Chain.TipState(), settingsResp.Prices, settingsResp.WalletAddress, formResult.Contract.Revision, proto4.RPCRenewContractParams{
		ContractID:  formResult.Contract.ID,
		Allowance:   types.Siacoins(150),
		Collateral:  types.Siacoins(300),
		ProofHeight: formResult.Contract.Revision.ProofHeight + 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the renewed contract uses the new host wallet address
	if renewResult.Contract.Revision.HostOutput.Address != hn.Wallet.Address() {
		t.Fatalf("expected host output address %v, got %v", hn.Wallet.Address(), renewResult.Contract.Revision.HostOutput.Address)
	}

	// verify the transaction set is valid
	if _, err := hn.Chain.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
		t.Fatal(err)
	}
}
