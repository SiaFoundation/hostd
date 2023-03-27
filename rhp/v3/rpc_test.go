package rhp_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	nlog "gitlab.com/NebulousLabs/log"
	nmux "gitlab.com/NebulousLabs/siamux"
	smux "gitlab.com/NebulousLabs/siamux/mux"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/internal/test"
	smods "go.sia.tech/siad/modules"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func startHostTransport(priv types.PrivateKey) (string, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	go func() {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		} else if err != nil {
			panic(err)
		}

		t, err := rhpv3.NewHostTransport(conn, priv)
		if err != nil {
			panic(err)
		}

		stream, err := t.AcceptStream()
		if err != nil {
			panic(err)
		} else if err := stream.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			panic(err)
		}

		id, err := stream.ReadID()
		if err != nil {
			panic(err)
		} else if id != rhpv3.RPCUpdatePriceTableID {
			panic("unexpected id")
		}

		resp := rhpv3.RPCUpdatePriceTableResponse{
			PriceTableJSON: []byte("hello world"),
		}
		if err := stream.WriteResponse(&resp); err != nil {
			panic(err)
		}
	}()
	return l.Addr().String(), nil
}

func TestTransportReadWriteCompat(t *testing.T) {
	privKey := types.NewPrivateKeyFromSeed(frand.Bytes(32))

	addr, err := startHostTransport(privKey)
	if err != nil {
		t.Fatal(err)
	}

	m, err := nmux.New(":0", ":0", nlog.DiscardLogger, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	pubKey := privKey.PublicKey()
	var muxKey smux.ED25519PublicKey
	copy(muxKey[:], pubKey[:])
	stream, err := m.NewStream("host", addr, muxKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	if err := stream.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := smods.RPCWrite(stream, smods.RPCUpdatePriceTable); err != nil {
		t.Fatal(err)
	}

	var resp smods.RPCUpdatePriceTableResponse
	if err := smods.RPCRead(stream, &resp); err != nil {
		t.Fatal(err)
	} else if string(resp.PriceTableJSON) != "hello world" {
		t.Fatal("unexpected response")
	}
}

func TestPriceTable(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	pt, err := host.RHPv3PriceTable()
	if err != nil {
		t.Fatal(err)
	}

	session, err := renter.NewRHP3Session(context.Background(), host.RHPv3Addr(), host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	retrieved, err := session.ScanPriceTable()
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}

	// pay for a price table using a contract payment
	revision, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(10), types.Siacoins(20), 200)
	if err != nil {
		t.Fatal(err)
	}

	account := rhpv3.Account(renter.PublicKey())
	contractSession := session.WithContractPayment(&revision, renter.PrivateKey(), account)
	defer contractSession.Close()

	retrieved, err = contractSession.RegisterPriceTable()
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}

	// fund an account
	_, err = contractSession.FundAccount(account, types.Siacoins(1))
	if err != nil {
		t.Fatal(err)
	}

	// pay for a price table using an account
	retrieved, err = session.WithAccountPayment(account, renter.PrivateKey()).RegisterPriceTable()
	if err != nil {
		t.Fatal(err)
	}
	// clear the UID field
	pt.UID = retrieved.UID
	if !reflect.DeepEqual(pt, retrieved) {
		t.Fatal("price tables don't match")
	}
}

func TestAppendSector(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	session, err := renter.NewRHP3Session(context.Background(), host.RHPv3Addr(), host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	revision, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(50), types.Siacoins(100), 200)
	if err != nil {
		t.Fatal(err)
	}

	// register the price table
	contractSession := session.WithContractPayment(&revision, renter.PrivateKey(), rhpv3.Account(renter.PublicKey()))
	pt, err := contractSession.RegisterPriceTable()
	if err != nil {
		t.Fatal(err)
	}

	// fund an account
	account := rhpv3.Account(renter.PublicKey())
	_, err = contractSession.FundAccount(account, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}

	// upload a sector
	accountSession := contractSession.WithAccountPayment(account, renter.PrivateKey())
	// calculate the cost of the upload
	usage := pt.AppendSectorCost(revision.Revision.WindowStart - renter.TipState().Index.Height)
	cost, _ := usage.Total()
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:256])
	root := rhpv2.SectorRoot(&sector)
	err = accountSession.AppendSector(&sector, &revision, renter.PrivateKey(), cost)
	if err != nil {
		t.Fatal(err)
	}

	// download the sector
	usage = pt.ReadSectorCost(rhpv2.SectorSize)
	cost, _ = usage.Total()
	downloaded, err := accountSession.ReadSector(root, 0, rhpv2.SectorSize, cost)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(downloaded, sector[:]) {
		t.Fatal("downloaded sector doesn't match")
	}
}

func TestStoreSector(t *testing.T) {
	log := zaptest.NewLogger(t)
	renter, host, err := test.NewTestingPair(t.TempDir(), log)
	if err != nil {
		t.Fatal(err)
	}
	defer renter.Close()
	defer host.Close()

	session, err := renter.NewRHP3Session(context.Background(), host.RHPv3Addr(), host.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	revision, err := renter.FormContract(context.Background(), host.RHPv2Addr(), host.PublicKey(), types.Siacoins(50), types.Siacoins(100), 200)
	if err != nil {
		t.Fatal(err)
	}

	// register the price table
	contractSession := session.WithContractPayment(&revision, renter.PrivateKey(), rhpv3.Account(renter.PublicKey()))
	pt, err := contractSession.RegisterPriceTable()
	if err != nil {
		t.Fatal(err)
	}

	// fund an account
	account := rhpv3.Account(renter.PublicKey())
	_, err = contractSession.FundAccount(account, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}

	// upload a sector
	accountSession := contractSession.WithAccountPayment(account, renter.PrivateKey())
	// calculate the cost of the upload
	usage := pt.StoreSectorCost(10)
	cost, _ := usage.Total()
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:256])
	root := rhpv2.SectorRoot(&sector)
	err = accountSession.StoreSector(&sector, 10, cost)
	if err != nil {
		t.Fatal(err)
	}

	// download the sector
	usage = pt.ReadSectorCost(rhpv2.SectorSize)
	cost, _ = usage.Total()
	downloaded, err := accountSession.ReadSector(root, 0, rhpv2.SectorSize, cost)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(downloaded, sector[:]) {
		t.Fatal("downloaded sector doesn't match")
	}

	// mine until the sector expires
	if err := host.MineBlocks(types.VoidAddress, 10); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // sync time

	// prune the sectors
	if err := host.Storage().PruneSectors(); err != nil {
		t.Fatal(err)
	}

	// check that the sector was deleted
	usage = pt.ReadSectorCost(rhpv2.SectorSize)
	cost, _ = usage.Total()
	_, err = accountSession.ReadSector(root, 0, rhpv2.SectorSize, cost)
	if err == nil {
		t.Fatal("expected error when reading sector")
	}
}
