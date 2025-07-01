package connectivity_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/explorer/connectivity"
)

type (
	mockExplorer struct {
		mu sync.Mutex
		fn func(ctx context.Context, host explorer.Host) (explorer.TestResult, error)
	}

	mockSettings struct {
		mu               sync.Mutex
		rhp4NetAddresses []chain.NetAddress
	}
)

// RHP4NetAddresses returns the RHP4 net addresses for the mock settings.
func (m *mockSettings) RHP4NetAddresses() []chain.NetAddress {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rhp4NetAddresses
}

func (m *mockSettings) SetRHP4NetAddresses(addresses []chain.NetAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rhp4NetAddresses = addresses
}

// TestConnection is a mock implementation of the Explorer interface
func (m *mockExplorer) TestConnection(ctx context.Context, host explorer.Host) (explorer.TestResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fn(ctx, host)
}

func (m *mockExplorer) SetFn(fn func(ctx context.Context, host explorer.Host) (explorer.TestResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fn = fn
}

var (
	validConnectionResult = func(_ context.Context, host explorer.Host) (explorer.TestResult, error) {
		res := explorer.TestResult{
			PublicKey: host.PublicKey,
		}
		for _, addr := range host.RHP4NetAddresses {
			res.RHP4 = append(res.RHP4, explorer.RHP4Result{
				NetAddress: addr,
			})
		}
		return res, nil
	}
	invalidConnectionResult = func(_ context.Context, host explorer.Host) (explorer.TestResult, error) {
		res := explorer.TestResult{
			PublicKey: host.PublicKey,
		}
		for _, addr := range host.RHP4NetAddresses {
			res.RHP4 = append(res.RHP4, explorer.RHP4Result{
				NetAddress: addr,
				Errors:     []string{"test error"},
			})
		}
		return res, nil
	}
)

func TestTestConnection(t *testing.T) {
	mexp := &mockExplorer{
		fn: invalidConnectionResult,
	}

	hostKey := types.GeneratePrivateKey().PublicKey()
	sm := &mockSettings{
		rhp4NetAddresses: []chain.NetAddress{
			{Protocol: quic.Protocol, Address: "1.2.3.4:9984"},
		},
	}

	am := alerts.NewManager()

	cm, err := connectivity.NewManager(hostKey, sm, mexp, connectivity.WithAlerts(am))
	if err != nil {
		t.Fatalf("failed to create connectivity manager: %v", err)
	}
	defer cm.Close()

	_, success, err := cm.TestConnection(context.Background())
	if err != nil {
		t.Fatalf("TestConnection failed: %v", err)
	} else if success {
		t.Fatal("expected TestConnection to return false on errors")
	}

	mexp.SetFn(validConnectionResult)
	_, success, err = cm.TestConnection(context.Background())
	if err != nil {
		t.Fatalf("TestConnection failed: %v", err)
	} else if !success {
		t.Fatal("expected TestConnection to return true on successful connection")
	}
}

func TestConnectivityAlerts(t *testing.T) {
	mexp := &mockExplorer{
		fn: invalidConnectionResult,
	}

	hostKey := types.GeneratePrivateKey().PublicKey()
	sm := &mockSettings{
		rhp4NetAddresses: []chain.NetAddress{
			{Protocol: quic.Protocol, Address: "1.2.3.4:9984"},
		},
	}
	am := alerts.NewManager()

	cm, err := connectivity.NewManager(hostKey, sm, mexp,
		connectivity.WithAlerts(am),
		connectivity.WithMaxCheckInterval(100*time.Millisecond),
		connectivity.WithBackoff(func(int) time.Duration { return 100 * time.Millisecond }))
	if err != nil {
		t.Fatalf("failed to create connectivity manager: %v", err)
	}
	defer cm.Close()

	time.Sleep(500 * time.Millisecond) // wait for the first test to run

	active := am.Active()
	if len(active) != 1 {
		t.Fatalf("expected 1 active alert, got %d", len(active))
	} else if active[0].Category != "connectionTest" {
		t.Fatalf("expected active alert category to be 'connectionTest', got %q", active[0].Category)
	} else if active[0].Severity != alerts.SeverityWarning {
		t.Fatalf("expected active alert severity to be 'warning', got %s", active[0].Severity)
	} else if !strings.Contains(active[0].Message, "test error") {
		t.Fatalf("expected active alert message to contain 'test error', got %q", active[0].Message)
	}

	mexp.SetFn(validConnectionResult)
	time.Sleep(500 * time.Millisecond) // wait for the next test to run
	if len(am.Active()) != 0 {
		t.Fatalf("expected no active alerts, got %d", len(am.Active()))
	}
}
