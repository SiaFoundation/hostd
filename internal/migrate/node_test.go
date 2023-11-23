package migrate_test

import (
	"fmt"
	"path/filepath"

	"gitlab.com/NebulousLabs/siamux"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	sconsensus "go.sia.tech/siad/modules/consensus"
	sgateway "go.sia.tech/siad/modules/gateway"
	shost "go.sia.tech/siad/modules/host"
	stpool "go.sia.tech/siad/modules/transactionpool"
	swallet "go.sia.tech/siad/modules/wallet"
)

type siadNode struct {
	gateway   modules.Gateway
	consensus modules.ConsensusSet
	tpool     modules.TransactionPool
	wallet    modules.Wallet
	host      modules.Host
	mux       *siamux.SiaMux
}

func (sn *siadNode) Close() error {
	sn.mux.Close()
	sn.host.Close()
	sn.wallet.Close()
	sn.tpool.Close()
	sn.consensus.Close()
	sn.gateway.Close()
	return nil
}

func (sn *siadNode) Gateway() modules.Gateway {
	return sn.gateway
}

func (sn *siadNode) ConsensusSet() modules.ConsensusSet {
	return sn.consensus
}

func (sn *siadNode) TransactionPool() modules.TransactionPool {
	return sn.tpool
}

func (sn *siadNode) Wallet() modules.Wallet {
	return sn.wallet
}

func (sn *siadNode) Host() modules.Host {
	return sn.host
}

func startSiad(dir string) (*siadNode, error) {
	g, err := sgateway.New(":0", false, filepath.Join(dir, "gateway"))
	if err != nil {
		return nil, fmt.Errorf("failed to create gateway: %v", err)
	}

	cs, errCh := sconsensus.New(g, false, filepath.Join(dir, "consensus"))
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("failed to create consensus: %v", err)
	}

	tp, err := stpool.New(cs, g, filepath.Join(dir, "transactionpool"))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction pool: %v", err)
	}

	w, err := swallet.New(cs, tp, filepath.Join(dir, "wallet"))
	if err != nil {
		return nil, fmt.Errorf("failed to create wallet: %v", err)
	}

	key := crypto.GenerateSiaKey(crypto.TypeDefaultWallet)
	if _, err := w.Encrypt(key); err != nil {
		return nil, fmt.Errorf("failed to encrypt wallet: %v", err)
	} else if err := w.Unlock(key); err != nil {
		return nil, fmt.Errorf("failed to unlock wallet: %v", err)
	}

	mux, _, err := modules.NewSiaMux(filepath.Join(dir, "siamux"), dir, ":0", ":0")
	if err != nil {
		return nil, fmt.Errorf("failed to create siamux: %v", err)
	}

	h, err := shost.New(cs, g, tp, w, mux, ":0", filepath.Join(dir, "host"))
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %v", err)
	}

	return &siadNode{
		gateway:   g,
		consensus: cs,
		tpool:     tp,
		wallet:    w,
		host:      h,
		mux:       mux,
	}, nil
}
