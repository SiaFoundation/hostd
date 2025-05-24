package api

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/v2/explorer"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/metrics"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/webhooks"
	"go.sia.tech/jape"
)

// A Client is a client for the hostd API.
type Client struct {
	c jape.Client

	mu sync.Mutex // protects the following fields
	n  *consensus.Network
}

// State returns the current state of the host
func (c *Client) State() (resp State, err error) {
	err = c.c.GET(context.Background(), "/state", &resp)
	return
}

// ConsensusNetwork returns the node's consensus network
func (c *Client) ConsensusNetwork() (network consensus.Network, err error) {
	err = c.c.GET(context.Background(), "/consensus/network", &network)
	return
}

// ConsensusTip returns the current consensus tip
func (c *Client) ConsensusTip() (tip types.ChainIndex, err error) {
	err = c.c.GET(context.Background(), "/consensus/tip", &tip)
	return
}

// ConsensusTipState returns the current consensus tip state
func (c *Client) ConsensusTipState() (state consensus.State, err error) {
	err = c.c.GET(context.Background(), "/consensus/tipstate", &state)
	if err != nil {
		return
	}
	c.mu.Lock()
	if c.n == nil {
		n, err := c.ConsensusNetwork()
		if err != nil {
			c.mu.Unlock()
			return consensus.State{}, fmt.Errorf("failed to get consensus network: %w", err)
		}
		c.n = &n
	}
	state.Network = c.n
	c.mu.Unlock()
	return
}

// IndexTip returns the last chain index processed by the host.
func (c *Client) IndexTip() (tip types.ChainIndex, err error) {
	err = c.c.GET(context.Background(), "/index/tip", &tip)
	return
}

// SyncerAddress returns the address of the syncer.
func (c *Client) SyncerAddress() (addr string, err error) {
	err = c.c.GET(context.Background(), "/syncer/address", &addr)
	return
}

// SyncerPeers returns the currently connected peers of the syncer.
func (c *Client) SyncerPeers() (peers []Peer, err error) {
	err = c.c.GET(context.Background(), "/syncer/peers", &peers)
	return
}

// SyncerConnect connects to a peer.
func (c *Client) SyncerConnect(address string) error {
	return c.c.PUT(context.Background(), "/syncer/peers", SyncerConnectRequest{address})
}

// Announce announces the host to the network. The announced address is
// determined by the host's current settings.
func (c *Client) Announce() error {
	return c.c.POST(context.Background(), "/settings/announce", nil, nil)
}

// Settings returns the current settings of the host.
func (c *Client) Settings() (settings settings.Settings, err error) {
	err = c.c.GET(context.Background(), "/settings", &settings)
	return
}

// UpdateSettings updates the host's settings.
func (c *Client) UpdateSettings(updated ...Setting) (settings settings.Settings, err error) {
	values := make(map[string]any)
	for _, s := range updated {
		s(values)
	}
	err = c.c.PATCH(context.Background(), "/settings", values, &settings)
	return
}

// TestDDNS tests the dynamic DNS settings of the host.
func (c *Client) TestDDNS() error {
	return c.c.PUT(context.Background(), "/settings/ddns/update", nil)
}

// Metrics returns the metrics of the host at the specified time.
func (c *Client) Metrics(at time.Time) (metrics metrics.Metrics, err error) {
	v := url.Values{
		"timestamp": []string{at.Format(time.RFC3339)},
	}
	err = c.c.GET(context.Background(), "/metrics?"+v.Encode(), &metrics)
	return
}

// PeriodMetrics returns the metrics of the host for n periods starting at start.
func (c *Client) PeriodMetrics(start time.Time, n int, interval metrics.Interval) (periods []metrics.Metrics, err error) {
	v := url.Values{
		"start":   []string{start.Format(time.RFC3339)},
		"periods": []string{strconv.Itoa(n)},
	}
	err = c.c.GET(context.Background(), "/metrics/"+interval.String()+"?"+v.Encode(), &periods)
	return
}

// Contracts returns the contracts of the host matching the filter.
func (c *Client) Contracts(filter contracts.ContractFilter) ([]contracts.Contract, int, error) {
	var resp ContractsResponse
	err := c.c.POST(context.Background(), "/contracts", filter, &resp)
	return resp.Contracts, resp.Count, err
}

// Contract returns the contract with the specified ID.
func (c *Client) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	err = c.c.GET(context.Background(), "/contracts/"+id.String(), &contract)
	return
}

// V2Contracts returns the v2 contracts of the host matching the filter.
func (c *Client) V2Contracts(filter contracts.V2ContractFilter) ([]contracts.V2Contract, int, error) {
	var resp V2ContractsResponse
	err := c.c.POST(context.Background(), "/v2/contracts", filter, &resp)
	return resp.Contracts, resp.Count, err
}

// V2Contract returns the v2 contract with the specified ID.
func (c *Client) V2Contract(id types.FileContractID) (contract contracts.V2Contract, err error) {
	err = c.c.GET(context.Background(), "/v2/contracts/"+id.String(), &contract)
	return
}

// StartIntegrityCheck scans the volume with the specified ID for consistency errors.
func (c *Client) StartIntegrityCheck(id types.FileContractID) error {
	return c.c.PUT(context.Background(), fmt.Sprintf("/contracts/%v/integrity", id), nil)
}

// IntegrityCheckProgress returns the progress of the integrity check for the
// specified contract.
func (c *Client) IntegrityCheckProgress(id types.FileContractID) (IntegrityCheckResult, error) {
	var result IntegrityCheckResult
	err := c.c.GET(context.Background(), fmt.Sprintf("/contracts/%v/integrity", id), &result)
	return result, err
}

// DeleteIntegrityCheck deletes the integrity check for the specified contract.
func (c *Client) DeleteIntegrityCheck(id types.FileContractID) error {
	return c.c.DELETE(context.Background(), fmt.Sprintf("/contracts/%v/integrity", id))
}

// DeleteSector deletes the sector with the specified root. This can cause
// contract failures if the sector is still in use.
func (c *Client) DeleteSector(root types.Hash256) error {
	return c.c.DELETE(context.Background(), fmt.Sprintf("/sectors/%s", root))
}

// Volumes returns the volumes of the host.
func (c *Client) Volumes() (volumes []VolumeMeta, err error) {
	err = c.c.GET(context.Background(), "/volumes", &volumes)
	return
}

// Volume returns the volume with the specified ID.
func (c *Client) Volume(id int) (volume VolumeMeta, err error) {
	err = c.c.GET(context.Background(), fmt.Sprintf("/volumes/%d", id), &volume)
	return
}

// AddVolume adds a new volume to the host
func (c *Client) AddVolume(localPath string, sectors uint64) (vol storage.Volume, err error) {
	req := AddVolumeRequest{
		LocalPath:  localPath,
		MaxSectors: sectors,
	}
	err = c.c.POST(context.Background(), "/volumes", req, &vol)
	return
}

// UpdateVolume updates the volume with the specified ID.
func (c *Client) UpdateVolume(id int, req UpdateVolumeRequest) error {
	return c.c.PUT(context.Background(), fmt.Sprintf("/volumes/%v", id), req)
}

// DeleteVolume deletes the volume with the specified ID.
func (c *Client) DeleteVolume(id int) error {
	return c.c.DELETE(context.Background(), fmt.Sprintf("/volumes/%v", id))
}

// ResizeVolume resizes the volume with the specified ID to a new size.
func (c *Client) ResizeVolume(id int, sectors uint64) error {
	req := ResizeVolumeRequest{
		MaxSectors: sectors,
	}
	return c.c.PUT(context.Background(), fmt.Sprintf("/volumes/%v/resize", id), req)
}

// Wallet returns the state of the host's wallet.
func (c *Client) Wallet() (resp WalletResponse, err error) {
	err = c.c.GET(context.Background(), "/wallet", &resp)
	return
}

// Events returns the transactions of the host's wallet.
func (c *Client) Events(limit, offset int) (transactions []wallet.Event, err error) {
	err = c.c.GET(context.Background(), fmt.Sprintf("/wallet/events?limit=%d&offset=%d", limit, offset), &transactions)
	return
}

// PendingEvents returns transactions that are not yet confirmed.
func (c *Client) PendingEvents() (events []wallet.Event, err error) {
	err = c.c.GET(context.Background(), "/wallet/pending", &events)
	return
}

// SendSiacoins sends siacoins to the specified address. If subtractFee is true,
// the miner fee is subtracted from the amount.
func (c *Client) SendSiacoins(address types.Address, amount types.Currency, subtractFee bool) (id types.TransactionID, err error) {
	req := WalletSendSiacoinsRequest{
		Address:          address,
		Amount:           amount,
		SubtractMinerFee: subtractFee,
	}
	err = c.c.POST(context.Background(), "/wallet/send", req, &id)
	return
}

// LocalDir returns the contents of the specified directory on the host.
func (c *Client) LocalDir(path string) (resp SystemDirResponse, err error) {
	v := url.Values{
		"path": []string{path},
	}
	err = c.c.GET(context.Background(), "/system/dir?"+v.Encode(), &resp)
	return
}

// BackupSQLite3 creates a backup of the SQLite3 database at the specified path
// on the local filesystem.
func (c *Client) BackupSQLite3(destPath string) error {
	return c.c.POST(context.Background(), "/system/sqlite3/backup", BackupRequest{destPath}, nil)
}

// MkDir creates a new directory on the host.
func (c *Client) MkDir(path string) error {
	req := CreateDirRequest{
		Path: path,
	}
	return c.c.PUT(context.Background(), "/system/dir", req)
}

// RegisterWebHook registers a new Webhook.
func (c *Client) RegisterWebHook(callbackURL string, scopes []string) (hook webhooks.Webhook, err error) {
	req := RegisterWebHookRequest{
		CallbackURL: callbackURL,
		Scopes:      scopes,
	}
	err = c.c.POST(context.Background(), "/webhooks", req, &hook)
	return
}

// UpdateWebHook updates the Webhook with the specified ID.
func (c *Client) UpdateWebHook(id int64, callbackURL string, scopes []string) (hook webhooks.Webhook, err error) {
	req := RegisterWebHookRequest{
		CallbackURL: callbackURL,
		Scopes:      scopes,
	}
	err = c.c.PUT(context.Background(), fmt.Sprintf("/webhooks/%d", id), req)
	return
}

// DeleteWebHook deletes the Webhook with the specified ID.
func (c *Client) DeleteWebHook(id int64) error {
	return c.c.DELETE(context.Background(), fmt.Sprintf("/webhooks/%d", id))
}

// WebHooks returns all registered WebHooks.
func (c *Client) WebHooks() (hooks []webhooks.Webhook, err error) {
	err = c.c.GET(context.Background(), "/webhooks", &hooks)
	return
}

// TestConnection starts an external connection test to the host. This is used to
// test the host's connectivity to the network and its ability to accept
// connections from renters.
func (c *Client) TestConnection() (res explorer.TestResult, err error) {
	err = c.c.PUT(context.Background(), "/system/connect/test", &res)
	return
}

// NewClient creates a new hostd API client.
func NewClient(baseURL, password string) *Client {
	return &Client{
		c: jape.Client{
			BaseURL:  baseURL,
			Password: password,
		},
	}
}
