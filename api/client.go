package api

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/jape"
)

// A Client is a client for the hostd API.
type Client struct {
	c jape.Client
}

// Host returns the current state of the host
func (c *Client) Host() (resp HostState, err error) {
	err = c.c.GET("/state/host", &resp)
	return
}

// Consensus returns the current consensus state.
func (c *Client) Consensus() (resp ConsensusState, err error) {
	err = c.c.GET("/state/consensus", &resp)
	return
}

// SyncerAddress returns the address of the syncer.
func (c *Client) SyncerAddress() (addr string, err error) {
	err = c.c.GET("/syncer/address", &addr)
	return
}

// SyncerPeers returns the currently connected peers of the syncer.
func (c *Client) SyncerPeers() (peers []Peer, err error) {
	err = c.c.GET("/syncer/peers", &peers)
	return
}

// SyncerConnect connects to a peer.
func (c *Client) SyncerConnect(address string) error {
	return c.c.PUT("/syncer/peers", SyncerConnectRequest{address})
}

// SyncerDisconnect disconnects from a peer.
func (c *Client) SyncerDisconnect(address string) error {
	return c.c.DELETE(fmt.Sprintf("/syncer/peers/%s", address))
}

// Announce announces the host to the network. The announced address is
// determined by the host's current settings.
func (c *Client) Announce() error {
	return c.c.POST("/settings/announce", nil, nil)
}

// Settings returns the current settings of the host.
func (c *Client) Settings() (settings settings.Settings, err error) {
	err = c.c.GET("/settings", &settings)
	return
}

// UpdateSettings updates the host's settings.
func (c *Client) UpdateSettings(updated ...Setting) (settings settings.Settings, err error) {
	values := make(map[string]any)
	for _, s := range updated {
		s(values)
	}
	err = c.c.PATCH("/settings", values, &settings)
	return
}

// TestDDNS tests the dynamic DNS settings of the host.
func (c *Client) TestDDNS() error {
	return c.c.PUT("/settings/ddns/update", nil)
}

// Metrics returns the metrics of the host at the specified time.
func (c *Client) Metrics(at time.Time) (metrics metrics.Metrics, err error) {
	v := url.Values{
		"timestamp": []string{at.Format(time.RFC3339)},
	}
	err = c.c.GET("/metrics?"+v.Encode(), &metrics)
	return
}

// PeriodMetrics returns the metrics of the host for n periods starting at start.
func (c *Client) PeriodMetrics(start time.Time, n int, interval metrics.Interval) (periods []metrics.Metrics, err error) {
	v := url.Values{
		"start":   []string{start.Format(time.RFC3339)},
		"periods": []string{strconv.Itoa(n)},
	}
	err = c.c.GET("/metrics/"+interval.String()+"?"+v.Encode(), &periods)
	return
}

// Contracts returns the contracts of the host matching the filter.
func (c *Client) Contracts(filter contracts.ContractFilter) ([]contracts.Contract, int, error) {
	var resp ContractsResponse
	err := c.c.POST("/contracts", filter, &resp)
	return resp.Contracts, resp.Count, err
}

// Contract returns the contract with the specified ID.
func (c *Client) Contract(id types.FileContractID) (contract contracts.Contract, err error) {
	err = c.c.GET("/contracts/"+id.String(), &contract)
	return
}

// StartIntegrityCheck scans the volume with the specified ID for consistency errors.
func (c *Client) StartIntegrityCheck(id types.FileContractID) error {
	return c.c.PUT(fmt.Sprintf("/contracts/%v/integrity", id), nil)
}

// IntegrityCheckProgress returns the progress of the integrity check for the
// specified contract.
func (c *Client) IntegrityCheckProgress(id types.FileContractID) (IntegrityCheckResult, error) {
	var result IntegrityCheckResult
	err := c.c.GET(fmt.Sprintf("/contracts/%v/integrity", id), &result)
	return result, err
}

// DeleteIntegrityCheck deletes the integrity check for the specified contract.
func (c *Client) DeleteIntegrityCheck(id types.FileContractID) error {
	return c.c.DELETE(fmt.Sprintf("/contracts/%v/integrity", id))
}

// DeleteSector deletes the sector with the specified root. This can cause
// contract failures if the sector is still in use.
func (c *Client) DeleteSector(root types.Hash256) error {
	return c.c.DELETE(fmt.Sprintf("/sectors/%s", root))
}

// Volumes returns the volumes of the host.
func (c *Client) Volumes() (volumes []VolumeMeta, err error) {
	err = c.c.GET("/volumes", &volumes)
	return
}

// Volume returns the volume with the specified ID.
func (c *Client) Volume(id int) (volume VolumeMeta, err error) {
	err = c.c.GET(fmt.Sprintf("/volumes/%d", id), &volume)
	return
}

// AddVolume adds a new volume to the host
func (c *Client) AddVolume(localPath string, sectors uint64) (vol storage.Volume, err error) {
	req := AddVolumeRequest{
		LocalPath:  localPath,
		MaxSectors: sectors,
	}
	err = c.c.POST("/volumes", req, &vol)
	return
}

// UpdateVolume updates the volume with the specified ID.
func (c *Client) UpdateVolume(id int, req UpdateVolumeRequest) error {
	return c.c.PUT(fmt.Sprintf("/volumes/%v", id), req)
}

// DeleteVolume deletes the volume with the specified ID.
func (c *Client) DeleteVolume(id int) error {
	return c.c.DELETE(fmt.Sprintf("/volumes/%v", id))
}

// ResizeVolume resizes the volume with the specified ID to a new size.
func (c *Client) ResizeVolume(id int, sectors uint64) error {
	req := ResizeVolumeRequest{
		MaxSectors: sectors,
	}
	return c.c.PUT(fmt.Sprintf("/volumes/%v/resize", id), req)
}

// Wallet returns the state of the host's wallet.
func (c *Client) Wallet() (resp WalletResponse, err error) {
	err = c.c.GET("/wallet", &resp)
	return
}

// Transactions returns the transactions of the host's wallet.
func (c *Client) Transactions(limit, offset int) (transactions []wallet.Transaction, err error) {
	err = c.c.GET(fmt.Sprintf("/wallet/transactions?limit=%d&offset=%d", limit, offset), &transactions)
	return
}

// PendingTransactions returns transactions that are not yet confirmed.
func (c *Client) PendingTransactions() (transactions []wallet.Transaction, err error) {
	err = c.c.GET("/wallet/pending", &transactions)
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
	err = c.c.POST("/wallet/send", req, &id)
	return
}

// LocalDir returns the contents of the specified directory on the host.
func (c *Client) LocalDir(path string) (resp SystemDirResponse, err error) {
	v := url.Values{
		"path": []string{path},
	}
	err = c.c.GET("/system/dir?"+v.Encode(), &resp)
	return
}

// MkDir creates a new directory on the host.
func (c *Client) MkDir(path string) error {
	req := CreateDirRequest{
		Path: path,
	}
	return c.c.PUT("/system/dir", req)
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
