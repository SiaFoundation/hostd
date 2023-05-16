package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/logging"
)

type (
	// SyncerConnectRequest is the request body for the [PUT] /syncer/peers endpoint.
	SyncerConnectRequest struct {
		Address string `json:"address"`
	}

	// BuildState contains static information about the build.
	BuildState struct {
		Network   string    `json:"network"`
		Version   string    `json:"version"`
		Commit    string    `json:"commit"`
		OS        string    `json:"OS"`
		BuildTime time.Time `json:"buildTime"`
	}

	// HostState is the response body for the [GET] /state/host endpoint.
	HostState struct {
		PublicKey     types.PublicKey `json:"publicKey"`
		WalletAddress types.Address   `json:"walletAddress"`
		BuildState
	}

	// ConsensusState is the response body for the [GET] /consensus endpoint.
	ConsensusState struct {
		Synced     bool             `json:"synced"`
		ChainIndex types.ChainIndex `json:"chainIndex"`
	}

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

	// JSONErrors is a slice of errors that can be marshaled to and unmarshaled
	// from JSON.
	JSONErrors []error

	// VolumeMeta is a volume with its metadata. It overrides the marshalling
	// of the storage.VolumeMeta type to handle error messages.
	VolumeMeta struct {
		storage.VolumeMeta
		Errors JSONErrors `json:"errors"`
	}

	// UpdateVolumeRequest is the request body for the [PUT] /volume/:id endpoint.
	UpdateVolumeRequest struct {
		ReadOnly bool `json:"readOnly"`
	}

	// ResizeVolumeRequest is the request body for the [PUT] /volume/:id/resize endpoint.
	ResizeVolumeRequest struct {
		MaxSectors uint64 `json:"maxSectors"`
	}

	// ContractsResponse is the response body for the [POST] /contracts endpoint.
	ContractsResponse struct {
		Count     int                  `json:"count"`
		Contracts []contracts.Contract `json:"contracts"`
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

	// SystemDirResponse is the response body for the [GET] /system/dir endpoint.
	SystemDirResponse struct {
		Path        string   `json:"path"`
		TotalBytes  uint64   `json:"totalBytes"`
		FreeBytes   uint64   `json:"freeBytes"`
		Directories []string `json:"directories"`
	}

	// LogResponse is the response body for the [GET] /log/entries endpoint.
	LogResponse struct {
		Count   int             `json:"count"`
		Entries []logging.Entry `json:"entries"`
	}
)

// MarshalJSON implements json.Marshaler
func (je JSONErrors) MarshalJSON() ([]byte, error) {
	if len(je) == 0 {
		return []byte("null"), nil
	}

	var errs []string
	for _, e := range je {
		if e != nil {
			errs = append(errs, e.Error())
		}
	}
	return json.Marshal(errs)
}

// UnmarshalJSON implements json.Unmarshaler
func (je *JSONErrors) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	var errs []string
	if err := json.Unmarshal(b, &errs); err != nil {
		return err
	}

	for _, e := range errs {
		*je = append(*je, errors.New(e))
	}
	return nil
}

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

// WithPriceTableValidity sets the PriceTableValidity field of the request
func (ur UpdateSettingsRequest) WithPriceTableValidity(value time.Duration) {
	ur["priceTableValidity"] = strconv.FormatInt(int64(value), 10)
}

// patchSettings merges two settings maps. returns an error if the two maps are
// not compatible.
func patchSettings(a, b map[string]any) (map[string]any, error) {
	for k, vb := range b {
		va, ok := a[k]
		if !ok {
			return nil, errors.New("unknown setting " + k)
		} else if va != nil && vb != nil && reflect.TypeOf(va) != reflect.TypeOf(vb) {
			return nil, fmt.Errorf("invalid type for setting %s: expected %T, got %T", k, va, vb)
		}

		switch vb := vb.(type) {
		case map[string]any:
			if a[k] == nil {
				a[k] = vb
			}
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
