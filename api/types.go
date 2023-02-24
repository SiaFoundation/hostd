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

	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		ScanHeight uint64         `json:"scanHeight"`
		Spendable  types.Currency `json:"spendable"`
		Confirmed  types.Currency `json:"confirmed"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send endpoint.
	WalletSendSiacoinsRequest struct {
		Address types.Address  `json:"address"`
		Amount  types.Currency `json:"amount"`
	}
)
