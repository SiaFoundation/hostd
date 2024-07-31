package sqlite

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/coreutils/syncer"
	"go.uber.org/zap/zaptest"
)

func TestAddPeer(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log.Named("sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ps, err := NewPeerStore(db)
	if err != nil {
		t.Fatal(err)
	}

	const peer = "1.2.3.4:9981"

	if err := ps.AddPeer(peer); err != nil {
		t.Fatal(err)
	}

	lastConnect := time.Now().UTC().Truncate(time.Second) // stored as unix milliseconds
	syncedBlocks := uint64(15)
	syncDuration := 5 * time.Second

	err = ps.UpdatePeerInfo(peer, func(info *syncer.PeerInfo) {
		info.LastConnect = lastConnect
		info.SyncedBlocks = syncedBlocks
		info.SyncDuration = syncDuration
	})
	if err != nil {
		t.Fatal(err)
	}

	info, err := ps.PeerInfo(peer)
	if err != nil {
		t.Fatal(err)
	}

	if !info.LastConnect.Equal(lastConnect) {
		t.Errorf("expected LastConnect = %v; got %v", lastConnect, info.LastConnect)
	}
	if info.SyncedBlocks != syncedBlocks {
		t.Errorf("expected SyncedBlocks = %d; got %d", syncedBlocks, info.SyncedBlocks)
	}
	if info.SyncDuration != 5*time.Second {
		t.Errorf("expected SyncDuration = %s; got %s", syncDuration, info.SyncDuration)
	}

	peers, err := ps.Peers()
	if err != nil {
		t.Fatal(err)
	} else if len(peers) != 1 {
		t.Fatalf("expected 1 peer; got %d", len(peers))
	} else if peerInfo := peers[0]; peerInfo.Address != peer {
		t.Errorf("expected peer address = %q; got %q", peer, peerInfo.Address)
	} else if peerInfo.LastConnect != lastConnect {
		t.Errorf("expected LastConnect = %v; got %v", lastConnect, peerInfo.LastConnect)
	} else if peerInfo.SyncedBlocks != syncedBlocks {
		t.Errorf("expected SyncedBlocks = %d; got %d", syncedBlocks, peerInfo.SyncedBlocks)
	} else if peerInfo.SyncDuration != syncDuration {
		t.Errorf("expected SyncDuration = %s; got %s", syncDuration, peerInfo.SyncDuration)
	} else if peerInfo.FirstSeen.IsZero() {
		t.Errorf("expected FirstSeen to be non-zero; got %v", peerInfo.FirstSeen)
	}
}

func TestBanPeer(t *testing.T) {
	log := zaptest.NewLogger(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "test.db"), log.Named("sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ps, err := NewPeerStore(db)
	if err != nil {
		t.Fatal(err)
	}

	const peer = "1.2.3.4"

	if banned, err := ps.Banned(peer); err != nil || banned {
		t.Fatal("expected peer to not be banned", err)
	}

	// ban the peer
	ps.Ban(peer, 5*time.Second, "test")

	if banned, err := ps.Banned(peer); err != nil || !banned {
		t.Fatal("expected peer to be banned", err)
	}

	// wait for the ban to expire
	time.Sleep(5 * time.Second)

	if banned, err := ps.Banned(peer); err != nil || banned {
		t.Fatal("expected peer to not be banned", err)
	}

	// ban a subnet
	_, subnet, err := net.ParseCIDR(peer + "/24")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("banning", subnet)
	ps.Ban(subnet.String(), time.Second, "test")
	if banned, err := ps.Banned(peer); err != nil || !banned {
		t.Fatal("expected peer to be banned", err)
	}
}
