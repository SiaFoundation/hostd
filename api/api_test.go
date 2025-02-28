package api_test

import (
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/api"
	"go.sia.tech/hostd/internal/testutil"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func startAPI(t testing.TB, sk types.PrivateKey, host *testutil.HostNode, log *zap.Logger) *api.Client {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	s := &http.Server{
		Handler:     jape.BasicAuth("test")(api.NewServer("test", sk.PublicKey(), host.Chain, host.Syncer, host.Accounts, host.Contracts, host.Volumes, host.Wallet, host.Store, host.Settings, host.Indexer)),
		ReadTimeout: 30 * time.Second,
	}
	t.Cleanup(func() { s.Close() })
	go func() {
		if err := s.Serve(l); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	client := api.NewClient("http://"+l.Addr().String(), "test")
	return client
}

func TestConsensusEndpoints(t *testing.T) {
	log := zap.NewNop()
	n, genesis := testutil.V1Network()
	hostKey := types.GeneratePrivateKey()
	host := testutil.NewHostNode(t, hostKey, n, genesis, log)
	client := startAPI(t, hostKey, host, log)

	testutil.MineAndSync(t, host, types.VoidAddress, 10)
	tip, err := client.ConsensusTip()
	if err != nil {
		t.Fatal(err)
	} else if tip.Height != 10 {
		t.Fatalf("expected tip height 10, got %v", tip.Height)
	}

	cs, err := client.ConsensusTipState()
	if err != nil {
		t.Fatal(err)
	} else if cs.Index != tip {
		t.Fatalf("expected tip %v, got %v", tip, cs.Index)
	} else if !reflect.DeepEqual(cs.Network, n) {
		t.Fatalf("expected network %v, got %v", n, cs.Network)
	}

	it, err := client.IndexTip()
	if err != nil {
		t.Fatal(err)
	} else if it != tip {
		t.Fatalf("expected index tip %v, got %v", tip, it)
	}
}
