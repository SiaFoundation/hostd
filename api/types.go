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
		// Address is the network address of the peer in host:port format.
		Address string `json:"address"`
	}

	// BuildState contains static information about the build.
	BuildState struct {
		// Version is the semantic version of the hostd build.
		Version string `json:"version"`
		// Commit is the git commit hash of the build.
		Commit string `json:"commit"`
		// OS is the operating system the host is running on.
		OS string `json:"os"`
		// BuildTime is the time the build was created.
		BuildTime time.Time `json:"buildTime"`
	}

	// ExplorerState contains static information about explorer data sources.
	ExplorerState struct {
		// Enabled indicates whether the explorer data source is enabled.
		Enabled bool `json:"enabled"`
		// URL is the base URL of the explorer data source. Empty if
		// the explorer is not enabled.
		URL string `json:"url"`
	}

	// State is the response body for the [GET] /state endpoint.
	State struct {
		// Name is the user-defined name of the host.
		Name string `json:"name,omitempty"`
		// PublicKey is the ed25519 public key of the host.
		PublicKey types.PublicKey `json:"publicKey"`
		// LastAnnouncement is the most recent host announcement on the
		// blockchain.
		LastAnnouncement settings.Announcement `json:"lastAnnouncement"`
		// StartTime is the time the hostd process started.
		StartTime time.Time `json:"startTime"`
		// Explorer contains the state of the explorer data source.
		Explorer ExplorerState `json:"explorer"`
		BuildState
	}

	// HostSettings is the response body for the [GET] /settings endpoint.
	HostSettings settings.Settings

	// Metrics is the response body for the [GET] /metrics endpoint.
	Metrics metrics.Metrics

	// ContractIntegrityResponse is a legacy response type for contract integrity checks.
	ContractIntegrityResponse struct {
		// BadSectors is a list of sector roots that failed integrity
		// checks.
		BadSectors []types.Hash256 `json:"badSectors"`
		// TotalSectors is the total number of sectors in the contract.
		TotalSectors uint64 `json:"totalSectors"`
	}

	// AddVolumeRequest is the request body for the [POST] /volume endpoint.
	AddVolumeRequest struct {
		// LocalPath is the absolute filesystem path where the volume
		// data will be stored.
		LocalPath string `json:"localPath"`
		// MaxSectors is the initial number of sectors the volume can
		// store. Each sector is 4 MiB.
		MaxSectors uint64 `json:"maxSectors"`
	}

	// JSONErrors is a slice of errors that can be marshaled to and unmarshaled
	// from JSON.
	JSONErrors []error

	// VolumeMeta is a volume with its metadata. It overrides the marshalling
	// of the storage.VolumeMeta type to handle error messages.
	VolumeMeta struct {
		storage.VolumeMeta
		// Errors contains any errors encountered during volume
		// operations.
		Errors JSONErrors `json:"errors"`
	}

	// UpdateVolumeRequest is the request body for the [PUT] /volumes/:id endpoint.
	UpdateVolumeRequest struct {
		// ReadOnly sets whether the volume is read-only. A read-only
		// volume will not accept new sectors.
		ReadOnly bool `json:"readOnly"`
	}

	// ResizeVolumeRequest is the request body for the [PUT] /volumes/:id/resize endpoint.
	ResizeVolumeRequest struct {
		// MaxSectors is the new maximum number of sectors the volume
		// can store. Each sector is 4 MiB.
		MaxSectors uint64 `json:"maxSectors"`
	}

	// ContractsResponse is the response body for the [POST] /contracts endpoint.
	ContractsResponse struct {
		// Count is the total number of contracts matching the filter.
		Count int `json:"count"`
		// Contracts is the list of contracts for the current page.
		Contracts []contracts.Contract `json:"contracts"`
	}

	// V2ContractsResponse is the response body for the [POST] /v2/contracts endpoint.
	V2ContractsResponse struct {
		// Count is the total number of v2 contracts matching the
		// filter.
		Count int `json:"count"`
		// Contracts is the list of v2 contracts for the current page.
		Contracts []contracts.V2Contract `json:"contracts"`
	}

	// WalletResponse is the response body for the [GET] /wallet endpoint.
	WalletResponse struct {
		wallet.Balance

		// Address is the host wallet's Siacoin address.
		Address types.Address `json:"address"`
	}

	// WalletSendSiacoinsRequest is the request body for the [POST] /wallet/send endpoint.
	WalletSendSiacoinsRequest struct {
		// Address is the recipient's Siacoin address.
		Address types.Address `json:"address"`
		// Amount is the amount of hastings to send.
		Amount types.Currency `json:"amount"`
		// SubtractMinerFee if true subtracts the miner fee from the
		// amount instead of adding it.
		SubtractMinerFee bool `json:"subtractMinerFee"`
	}

	// A Peer is a currently-connected peer.
	Peer struct {
		// Address is the network address of the peer.
		Address string `json:"address"`
		// Inbound indicates whether the peer initiated the connection.
		Inbound bool `json:"inbound"`
		// Version is the protocol version of the peer.
		Version string `json:"version"`

		// FirstSeen is the time the peer was first seen.
		FirstSeen time.Time `json:"firstSeen"`
		// ConnectedSince is the time the current connection was
		// established.
		ConnectedSince time.Time `json:"connectedSince"`
		// SyncedBlocks is the number of blocks synced from this peer
		// in the current session.
		SyncedBlocks uint64 `json:"syncedBlocks,omitempty"`
		// SyncDuration is the total time spent syncing blocks from
		// this peer.
		SyncDuration time.Duration `json:"syncDuration,omitempty"`
	}

	// A Setting updates a single setting on the host. It can be combined with
	// other settings to update multiple settings at once.
	Setting func(map[string]any)

	// SystemDirResponse is the response body for the [GET] /system/dir endpoint.
	SystemDirResponse struct {
		// Path is the absolute path of the inspected directory.
		Path string `json:"path"`
		// TotalBytes is the total disk space on the volume containing
		// the path.
		TotalBytes uint64 `json:"totalBytes"`
		// FreeBytes is the available disk space on the volume
		// containing the path.
		FreeBytes uint64 `json:"freeBytes"`
		// Directories is a list of subdirectory names in the
		// inspected directory.
		Directories []string `json:"directories"`
	}

	// A CreateDirRequest is the request body for the [POST] /system/dir endpoint.
	CreateDirRequest struct {
		// Path is the absolute filesystem path of the directory to
		// create.
		Path string `json:"path"`
	}

	// A BackupRequest is the request body for the [POST] /system/backup endpoint.
	BackupRequest struct {
		// Path is the absolute filesystem path where the backup file
		// will be written.
		Path string `json:"path"`
	}

	// VerifySectorResponse is the response body for the [GET] /sectors/:root/verify endpoint.
	VerifySectorResponse struct {
		storage.SectorReference
		// Error contains the error message if sector verification
		// failed. Empty if the sector is valid.
		Error string `json:"error,omitempty"`
	}

	// RegisterWebHookRequest is the request body for the [POST] /webhooks endpoint.
	RegisterWebHookRequest struct {
		// CallbackURL is the URL that will receive webhook event POST
		// requests.
		CallbackURL string `json:"callbackURL"`
		// Scopes is a list of event scopes to subscribe to.
		Scopes []string `json:"scopes"`
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
		// State is the consensus state at the specified block.
		State consensus.State `json:"state"`
		// Block is the full block at the specified block ID.
		Block types.Block `json:"block"`
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
