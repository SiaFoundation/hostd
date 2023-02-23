package api

import "go.sia.tech/core/types"

type (
	AddVolumeRequest struct {
		LocalPath  string `json:"localPath"`
		MaxSectors uint64 `json:"maxSectors"`
	}

	UpdateVolumeRequest struct {
		ReadOnly bool `json:"readOnly"`
	}

	WalletSendSiacoinsRequest struct {
		Address types.Address  `json:"address"`
		Amount  types.Currency `json:"amount"`
	}
)
