package api

import "go.sia.tech/core/types"

type (
	// AddVolumeRequest is the request body for the [POST] /volume endpoint.
	AddVolumeRequest struct {
		LocalPath  string `json:"localPath"`
		MaxSectors uint64 `json:"maxSectors"`
	}

	// UpdateVolumeRequest is the request body for the [PUT] /volume/:id endpoint.
	UpdateVolumeRequest struct {
		ReadOnly bool `json:"readOnly"`
	}

	// ResizeVolumeRequest is the request body for the [PUT] /volume/:id/resize endpoint.
	ResizeVolumeRequest struct {
		MaxSectors uint64 `json:"maxSectors"`
	}

	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		ScanHeight  uint64         `json:"scanHeight"`
		Address     types.Address  `json:"address"`
		Spendable   types.Currency `json:"spendable"`
		Confirmed   types.Currency `json:"confirmed"`
		Unconfirmed types.Currency `json:"unconfirmed"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send endpoint.
	WalletSendSiacoinsRequest struct {
		Address types.Address  `json:"address"`
		Amount  types.Currency `json:"amount"`
	}

	// A Peer is a peer in the network.
	Peer struct {
		Address string `json:"address"`
		Version string `json:"version"`
	}
)
