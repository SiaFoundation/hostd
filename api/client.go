package api

import (
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/financials"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/jape"
)

type Client struct {
	c jape.Client
}

func (c *Client) State() error {
	return nil
}
func (c *Client) Syncer() error {
	return nil
}
func (c *Client) SyncerPeers() ([]string, error) {
	return nil, nil
}
func (c *Client) SyncerConnect(address string) error {
	return nil
}
func (c *Client) SyncerDisconnect(address string) error {
	return nil
}
func (c *Client) Announce() error {
	return nil
}
func (c *Client) Settings() (settings.Settings, error) {
	return settings.Settings{}, nil
}
func (c *Client) UpdateSettings(settings settings.Settings) error {
	return nil
}
func (c *Client) Financials(period string) ([]financials.Revenue, error) {
	return nil, nil
}
func (c *Client) Contracts(limit, offset int) error {
	return nil
}
func (c *Client) GetContract() error {
	return nil
}
func (c *Client) DeleteSector() error {
	return nil
}

func (c *Client) Volumes() error {
	return nil
}

func (c *Client) AddVolume() error {
	return nil
}

func (c *Client) Volume() error {
	return nil
}

func (c *Client) UpdateVolume(UpdateVolumeRequest) error {
	return nil
}

func (c *Client) DeleteVolume() error {
	return nil
}

func (c *Client) ResizeVolume() error {
	return nil
}

func (c *Client) CheckVolume() error {
	return nil
}

func (c *Client) Wallet() error {
	return nil
}
func (c *Client) Transactions(limit, offset int) error {
	return nil
}
func (c *Client) SendSiacoins(address types.Address, amount types.Currency) error {
	return nil
}

func NewClient(baseURL, password string) *Client {
	return &Client{
		c: jape.Client{
			BaseURL:  baseURL,
			Password: password,
		},
	}
}
