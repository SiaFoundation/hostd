package wallet

import (
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

// Wrapper wraps a SingleAddressWallet to implement the WalletManager
// interface.
type Wrapper struct {
	priv types.PrivateKey
	*wallet.SingleAddressWallet
}

// WalletHash returns the hash of the wallet seed.
func (w *Wrapper) WalletHash() types.Hash256 {
	return types.HashBytes(w.priv[:])
}

// NewSingleAddressWallet creates a new single address wallet and wraps it to
// implement the WalletManager interface.
func NewSingleAddressWallet(priv types.PrivateKey, cm wallet.ChainManager, store wallet.SingleAddressStore, syncer wallet.Syncer, opts ...wallet.Option) (*Wrapper, error) {
	w, err := wallet.NewSingleAddressWallet(priv, cm, store, syncer, opts...)
	if err != nil {
		return nil, err
	}
	return &Wrapper{priv: priv, SingleAddressWallet: w}, nil
}
