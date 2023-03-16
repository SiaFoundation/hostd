package api

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"go.sia.tech/core/types"
)

type (
	// ContractIntegrityResponse is the response body for the [POST] /contracts/:id/check endpoint.
	ContractIntegrityResponse struct {
		BadSectors   []types.Hash256 `json:"badSectors"`
		TotalSectors uint64          `json:"totalSectors"`
	}

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

	// UpdateSettingsRequest is the request body for the [PUT] /settings
	// endpoint. It will be merged with the current settings.
	UpdateSettingsRequest map[string]any
)

// WithAcceptingContracts sets the AcceptingContracts field of the request
func (ur UpdateSettingsRequest) WithAcceptingContracts(value bool) {
	ur["acceptingContracts"] = value
}

// WithNetAddress sets the NetAddress field of the request
func (ur UpdateSettingsRequest) WithNetAddress(value string) {
	ur["netAddress"] = value
}

// WithMaxContractDuration sets the MaxContractDuration field of the request
func (ur UpdateSettingsRequest) WithMaxContractDuration(value uint64) {
	ur["maxContractDuration"] = strconv.FormatUint(value, 10)
}

// WithContractPrice sets the ContractPrice field of the request
func (ur UpdateSettingsRequest) WithContractPrice(value types.Currency) {
	ur["contractPrice"] = value.ExactString()
}

// WithBaseRPCPrice sets the BaseRPCPrice field of the request
func (ur UpdateSettingsRequest) WithBaseRPCPrice(value types.Currency) {
	ur["baseRPCPrice"] = value.ExactString()
}

// WithSectorAccessPrice sets the SectorAccessPrice field of the request
func (ur UpdateSettingsRequest) WithSectorAccessPrice(value types.Currency) {
	ur["sectorAccessPrice"] = value.ExactString()
}

// WithCollateral sets the Collateral field of the request
func (ur UpdateSettingsRequest) WithCollateral(value types.Currency) {
	ur["collateral"] = value.ExactString()
}

// WithMaxCollateral sets the MaxCollateral field of the request
func (ur UpdateSettingsRequest) WithMaxCollateral(value types.Currency) {
	ur["maxCollateral"] = value.ExactString()
}

// WithMaxAccountBalance sets the MaxAccountBalance field of the request
func (ur UpdateSettingsRequest) WithMaxAccountBalance(value types.Currency) {
	ur["maxAccountBalance"] = value.ExactString()
}

// WithMinStoragePrice sets the MinStoragePrice field of the request
func (ur UpdateSettingsRequest) WithMinStoragePrice(value types.Currency) {
	ur["minStoragePrice"] = value.ExactString()
}

// WithMinEgressPrice sets the MinEgressPrice field of the request
func (ur UpdateSettingsRequest) WithMinEgressPrice(value types.Currency) {
	ur["minEgressPrice"] = value.ExactString()
}

// WithMinIngressPrice sets the MinIngressPrice field of the request
func (ur UpdateSettingsRequest) WithMinIngressPrice(value types.Currency) {
	ur["minIngressPrice"] = value.ExactString()
}

// WithIngressLimit sets the IngressLimit field of the request
func (ur UpdateSettingsRequest) WithIngressLimit(value uint64) {
	ur["ingressLimit"] = strconv.FormatUint(value, 10)
}

// WithEgressLimit sets the EgressLimit field of the request
func (ur UpdateSettingsRequest) WithEgressLimit(value uint64) {
	ur["egressLimit"] = strconv.FormatUint(value, 10)
}

// WithMaxRegistryEntries sets the MaxRegistryEntries field of the request
func (ur UpdateSettingsRequest) WithMaxRegistryEntries(value uint64) {
	ur["maxRegistryEntries"] = strconv.FormatUint(value, 10)
}

// WithAccountExpiry sets the AccountExpiry field of the request
func (ur UpdateSettingsRequest) WithAccountExpiry(value time.Duration) {
	ur["accountExpiry"] = strconv.FormatInt(int64(value), 10)
}

// patchSettings merges two settings maps. returns an error if the two maps are
// not compatible.
func patchSettings(a, b map[string]any) (map[string]any, error) {
	for k, vb := range b {
		va, ok := a[k]
		if !ok {
			return nil, errors.New("unknown setting " + k)
		} else if reflect.TypeOf(va) != reflect.TypeOf(vb) {
			return nil, errors.New("invalid value for setting " + k)
		}

		switch vb := vb.(type) {
		case map[string]any:
			var err error
			a[k], err = patchSettings(a[k].(map[string]any), vb)
			if err != nil {
				return nil, fmt.Errorf("invalid value for setting %s: %w", k, err)
			}
		default:
			a[k] = vb
		}
	}
	return a, nil
}
