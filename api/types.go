package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/metrics"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
)

// JSON keys for host setting fields
const (
	settingAcceptingContracts  = "acceptingContracts"
	settingNetAddress          = "netAddress"
	settingMaxContractDuration = "maxContractDuration"
	settingContractPrice       = "contractPrice"
	settingBaseRPCPrice        = "baseRPCPrice"
	settingSectorAccessPrice   = "sectorAccessPrice"
	settingCollateral          = "collateral"
	settingMaxCollateral       = "maxCollateral"
	settingMaxAccountBalance   = "maxAccountBalance"
	settingStoragePrice        = "storagePrice"
	settingEgressPrice         = "egressPrice"
	settingIngressPrice        = "ingressPrice"
	settingIngressLimit        = "ingressLimit"
	settingEgressLimit         = "egressLimit"
	settingMaxRegistryEntries  = "maxRegistryEntries"
	settingAccountExpiry       = "accountExpiry"
	settingPriceTableValidity  = "priceTableValidity"
)

type (
	// SyncerConnectRequest is the request body for the [PUT] /syncer/peers endpoint.
	SyncerConnectRequest struct {
		Address string `json:"address"`
	}

	// BuildState contains static information about the build.
	BuildState struct {
		Version   string    `json:"version"`
		Commit    string    `json:"commit"`
		OS        string    `json:"os"`
		BuildTime time.Time `json:"buildTime"`
	}

	// ExplorerState contains static information about explorer data sources.
	ExplorerState struct {
		Enabled bool   `json:"enabled"`
		URL     string `json:"url"`
	}

	// State is the response body for the [GET] /state endpoint.
	State struct {
		Name             string                `json:"name,omitempty"`
		PublicKey        types.PublicKey       `json:"publicKey"`
		LastAnnouncement settings.Announcement `json:"lastAnnouncement"`
		StartTime        time.Time             `json:"startTime"`
		Explorer         ExplorerState         `json:"explorer"`
		BuildState
	}

	// HostSettings is the response body for the [GET] /settings endpoint.
	HostSettings settings.Settings

	// Metrics is the response body for the [GET] /metrics endpoint.
	Metrics metrics.Metrics

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

	// V2ContractsResponse is the response body for the [POST] /v2/contracts endpoint.
	V2ContractsResponse struct {
		Count     int                    `json:"count"`
		Contracts []contracts.V2Contract `json:"contracts"`
	}

	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		wallet.Balance

		Address types.Address `json:"address"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send endpoint.
	WalletSendSiacoinsRequest struct {
		Address          types.Address  `json:"address"`
		Amount           types.Currency `json:"amount"`
		SubtractMinerFee bool           `json:"subtractMinerFee"`
	}

	// A Peer is a currently-connected peer.
	Peer struct {
		Address string `json:"address"`
		Inbound bool   `json:"inbound"`
		Version string `json:"version"`

		FirstSeen      time.Time     `json:"firstSeen,omitempty"`
		ConnectedSince time.Time     `json:"connectedSince,omitempty"`
		SyncedBlocks   uint64        `json:"syncedBlocks,omitempty"`
		SyncDuration   time.Duration `json:"syncDuration,omitempty"`
	}

	// A Setting updates a single setting on the host. It can be combined with
	// other settings to update multiple settings at once.
	Setting func(map[string]any)

	// SystemDirResponse is the response body for the [GET] /system/dir endpoint.
	SystemDirResponse struct {
		Path        string   `json:"path"`
		TotalBytes  uint64   `json:"totalBytes"`
		FreeBytes   uint64   `json:"freeBytes"`
		Directories []string `json:"directories"`
	}

	// A CreateDirRequest is the request body for the [POST] /system/dir endpoint.
	CreateDirRequest struct {
		Path string `json:"path"`
	}

	// A BackupRequest is the request body for the [POST] /system/backup endpoint.
	BackupRequest struct {
		Path string `json:"path"`
	}

	// VerifySectorResponse is the response body for the [GET] /sectors/:root/verify endpoint.
	VerifySectorResponse struct {
		storage.SectorReference
		Error string `json:"error,omitempty"`
	}

	// RegisterWebHookRequest is the request body for the [POST] /webhooks endpoint.
	RegisterWebHookRequest struct {
		CallbackURL string   `json:"callbackURL"`
		Scopes      []string `json:"scopes"`
	}

	// TPoolResp is the response body for the [GET] /tpool/fee endpoint
	TPoolResp types.Currency

	// VolumeResp is the response body for the [GET] /volumes endpoint
	VolumeResp []VolumeMeta

	// AlertResp is the response body for the [GET] /alerts endpoint
	AlertResp []alerts.Alert

	// PeerResp is the response body for the [GET] /syncer/address endpoint
	PeerResp []Peer

	// ConsensusIndexResp is the response body for the [GET] /consensus/tip endpoint
	ConsensusIndexResp types.ChainIndex

	// ConsensusStateResp is the response body for the [GET] /consensus/state endpoint
	ConsensusStateResp consensus.State

	// ConsensusCheckpointResponse is the response body for the [GET] /consensus/checkpoint/:id endpoint
	ConsensusCheckpointResponse struct {
		State consensus.State `json:"state"`
		Block types.Block     `json:"block"`
	}

	// IndexTipResp is the response body for the [GET] /index/tip endpoint
	IndexTipResp types.ChainIndex

	// SyncerAddrResp is the response body for the [GET] /syncer/peers endpoint
	SyncerAddrResp string

	// WalletTransactionsResp is the response body for the [GET] /wallet/transactions endpoint
	WalletTransactionsResp []wallet.Event

	// WalletPendingResp is the response body for the [GET] /wallet/pending endpoint
	WalletPendingResp []wallet.Event
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

// MarshalText implements test.Marshaler
func (tr TPoolResp) MarshalText() ([]byte, error) {
	return types.Currency(tr).MarshalText()
}

// UnmarshalText implements test.Unmarshaler
func (tr *TPoolResp) UnmarshalText(b []byte) error {
	return (*types.Currency)(tr).UnmarshalText(b)
}

// SetAcceptingContracts sets the AcceptingContracts field of the request
func SetAcceptingContracts(value bool) Setting {
	return func(v map[string]any) {
		v[settingAcceptingContracts] = value
	}
}

// SetNetAddress sets the NetAddress field of the request
func SetNetAddress(addr string) Setting {
	return func(v map[string]any) {
		v[settingNetAddress] = addr
	}
}

// SetMaxContractDuration sets the MaxContractDuration field of the request
func SetMaxContractDuration(duration uint64) Setting {
	return func(v map[string]any) {
		v[settingMaxContractDuration] = duration
	}
}

// SetContractPrice sets the ContractPrice field of the request
func SetContractPrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingContractPrice] = price
	}
}

// SetBaseRPCPrice sets the BaseRPCPrice field of the request
func SetBaseRPCPrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingBaseRPCPrice] = price
	}
}

// SetSectorAccessPrice sets the SectorAccessPrice field of the request
func SetSectorAccessPrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingSectorAccessPrice] = price
	}
}

// SetCollateral sets the Collateral field of the request
func SetCollateral(collateral types.Currency) Setting {
	return func(v map[string]any) {
		v[settingCollateral] = collateral
	}
}

// SetMaxCollateral sets the MaxCollateral
func SetMaxCollateral(collateral types.Currency) Setting {
	return func(v map[string]any) {
		v[settingMaxCollateral] = collateral
	}
}

// SetMaxAccountBalance sets the MaxAccountBalance
func SetMaxAccountBalance(value types.Currency) Setting {
	return func(v map[string]any) {
		v[settingMaxAccountBalance] = value
	}
}

// SetMinStoragePrice sets the MinStoragePrice in bytes/block
func SetMinStoragePrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingStoragePrice] = price
	}
}

// SetMinEgressPrice sets the MinEgressPrice in bytes
func SetMinEgressPrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingEgressPrice] = price
	}
}

// SetMinIngressPrice sets the MinIngressPrice in bytes
func SetMinIngressPrice(price types.Currency) Setting {
	return func(v map[string]any) {
		v[settingIngressPrice] = price
	}
}

// SetIngressLimit sets the IngressLimit in bytes per second
func SetIngressLimit(limit uint64) Setting {
	return func(v map[string]any) {
		v[settingIngressLimit] = limit
	}
}

// SetEgressLimit sets the EgressLimit in bytes per second
func SetEgressLimit(limit uint64) Setting {
	return func(v map[string]any) {
		v[settingEgressLimit] = limit
	}
}

// SetMaxRegistryEntries sets the MaxRegistryEntries field of the request
func SetMaxRegistryEntries(value uint64) Setting {
	return func(v map[string]any) {
		v[settingMaxRegistryEntries] = value
	}
}

// SetAccountExpiry sets the AccountExpiry field of the request
func SetAccountExpiry(value time.Duration) Setting {
	return func(v map[string]any) {
		v[settingAccountExpiry] = int64(value)
	}
}

// SetPriceTableValidity sets the PriceTableValidity field of the request
func SetPriceTableValidity(value time.Duration) Setting {
	return func(v map[string]any) {
		v[settingPriceTableValidity] = int64(value)
	}
}

// patchSettings merges two settings maps. returns an error if the two maps are
// not compatible.
func patchSettings(a, b map[string]any) error {
	for k, vb := range b {
		va, ok := a[k]
		if !ok || va == nil {
			a[k] = vb // value doesn't exist, set it
		} else if vb != nil && reflect.TypeOf(va) != reflect.TypeOf(vb) {
			return fmt.Errorf("invalid type for setting %q: expected %T, got %T", k, va, vb)
		}

		switch vb := vb.(type) {
		case json.RawMessage:
			vaf, vbf := make(map[string]any), make(map[string]any)
			if err := json.Unmarshal(vb, &vbf); err != nil {
				return fmt.Errorf("failed to unmarshal fields %q: %w", k, err)
			} else if err := json.Unmarshal(va.(json.RawMessage), &vaf); err != nil {
				return fmt.Errorf("failed to unmarshal current fields %q: %w", k, err)
			}
			if err := patchSettings(vaf, vbf); err != nil {
				return fmt.Errorf("failed to patch fields %q: %w", k, err)
			}

			buf, err := json.Marshal(vaf)
			if err != nil {
				return fmt.Errorf("failed to marshal patched fields %q: %w", k, err)
			}
			a[k] = json.RawMessage(buf)
		case map[string]any:
			var err error
			err = patchSettings(a[k].(map[string]any), vb)
			if err != nil {
				return fmt.Errorf("invalid value for setting %q: %w", k, err)
			}
		default:
			a[k] = vb
		}
	}
	return nil
}
