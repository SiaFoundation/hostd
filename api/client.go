package api

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/wallet"
	"go.sia.tech/jape"
)

// A Client is a client for the hostd API.
type Client struct {
	c jape.Client
}

// State returns the current state of the host
func (c *Client) State() (resp StateResponse, err error) {
	err = c.c.GET("/state", &resp)
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
	return c.c.PUT(fmt.Sprintf("/syncer/peers/%s", address), nil)
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
func (c *Client) UpdateSettings(patch UpdateSettingsRequest) (settings settings.Settings, err error) {
	err = c.c.POST("/settings", patch, &settings)
	return
}

// Financials returns the financial metrics of the host for the specified period
func (c *Client) Financials(period string) (periods []financials.Revenue, err error) {
	err = c.c.GET(fmt.Sprintf("/financials/%s", period), &periods)
	return
}

// Contracts returns the contracts of the host.
func (c *Client) Contracts(filter contracts.ContractFilter) (contracts []contracts.Contract, err error) {
	err = c.c.POST("/contracts", filter, &contracts)
	return
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
func (c *Client) Volumes() (volumes []storage.VolumeMeta, err error) {
	err = c.c.GET("/volumes", &volumes)
	return
}

// Volume returns the volume with the specified ID.
func (c *Client) Volume(id int) (volume storage.VolumeMeta, err error) {
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

// SendSiacoins sends siacoins to the specified address.
func (c *Client) SendSiacoins(address types.Address, amount types.Currency) error {
	req := WalletSendSiacoinsRequest{
		Address: address,
		Amount:  amount,
	}
	return c.c.POST("/wallet/send", req, nil)
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
