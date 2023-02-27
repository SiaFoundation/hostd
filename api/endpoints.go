package api

import (
	"errors"
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/jape"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

// checkServerError conditionally writes an error to the response if err is not
// nil.
func (a *API) checkServerError(c jape.Context, context string, err error) bool {
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *API) handleGetState(c jape.Context) {
	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetSyncerAddr(c jape.Context) {
	c.Encode(a.syncer.Address())
}

func (a *API) handleGetSyncerPeers(c jape.Context) {
	c.Encode(a.syncer.Peers())
}

func (a *API) handlePutSyncerPeer(c jape.Context) {
	var addr modules.NetAddress
	if err := c.DecodeParam("address", &addr); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}
	err := a.syncer.Connect(addr)
	a.checkServerError(c, "failed to connect to peer", err)
}

func (a *API) handleDeleteSyncerPeer(c jape.Context) {
	var addr modules.NetAddress
	if err := c.DecodeParam("address", &addr); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}
	err := a.syncer.Disconnect(addr)
	a.checkServerError(c, "failed to disconnect from peer", err)
}

func (a *API) handlePostAnnounce(c jape.Context) {
	err := a.settings.Announce()
	a.checkServerError(c, "failed to announce", err)
}

func (a *API) handleGetSettings(c jape.Context) {
	c.Encode(a.settings.Settings())
}

func (a *API) handlePutSettings(c jape.Context) {
	var updated settings.Settings
	if err := c.Decode(&updated); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}
	err := a.settings.UpdateSettings(updated)
	if !a.checkServerError(c, "failed to update settings", err) {
		return
	}
	c.Encode(updated)
}

func (a *API) handleGetFinancials(c jape.Context) {
	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetContracts(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)
	contracts, err := a.contracts.Contracts(limit, offset)
	if !a.checkServerError(c, "failed to get contracts", err) {
		return
	}
	c.Encode(contracts)
}

func (a *API) handleGetContract(c jape.Context) {
	var id types.FileContractID
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}
	contract, err := a.contracts.Contract(id)
	if errors.Is(err, contracts.ErrNotFound) {
		c.Error(err, http.StatusNotFound)
		return
	} else if !a.checkServerError(c, "failed to get contract", err) {
		return
	}
	c.Encode(contract)
}

func (a *API) handleGetVolume(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	volume, err := a.volumes.Volume(id)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		c.Error(err, http.StatusNotFound)
		return
	} else if !a.checkServerError(c, "failed to get volume", err) {
		return
	}
	c.Encode(volume)
}

func (a *API) handlePutVolume(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req UpdateVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	err := a.volumes.SetReadOnly(id, req.ReadOnly)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		c.Error(err, http.StatusNotFound)
		return
	}
	a.checkServerError(c, "failed to update volume", err)
}

func (a *API) handleDeleteSector(c jape.Context) {
	var root types.Hash256
	if err := c.DecodeParam("root", &root); err != nil {
		c.Error(err, http.StatusBadRequest)
	}
	err := a.volumes.RemoveSector(root)
	a.checkServerError(c, "failed to remove sector", err)
}

func (a *API) handleGetVolumes(c jape.Context) {
	volumes, err := a.volumes.Volumes()
	if !a.checkServerError(c, "failed to get volumes", err) {
		return
	}
	c.Encode(volumes)
}

func (a *API) handlePostVolume(c jape.Context) {
	var req AddVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	} else if len(req.LocalPath) == 0 {
		c.Error(errors.New("local path is required"), http.StatusBadRequest)
		return
	} else if req.MaxSectors == 0 {
		c.Error(errors.New("max sectors is required"), http.StatusBadRequest)
		return
	}

	volume, err := a.volumes.AddVolume(req.LocalPath, req.MaxSectors)
	if !a.checkServerError(c, "failed to add volume", err) {
		return
	}
	c.Encode(volume)
}

func (a *API) handleDeleteVolume(c jape.Context) {
	var id int
	var force bool
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	} else if err := c.DecodeForm("force", &force); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}
	err := a.volumes.RemoveVolume(id, force)
	a.checkServerError(c, "failed to remove volume", err)
}

func (a *API) handlePutVolumeResize(c jape.Context) {
	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handlePutVolumeCheck(c jape.Context) {
	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetWalletAddress(c jape.Context) {
	c.Encode(a.wallet.Address())
}

func (a *API) handleGetWallet(c jape.Context) {
	spendable, confirmed, err := a.wallet.Balance()
	if !a.checkServerError(c, "failed to get wallet", err) {
		return
	}
	c.Encode(WalletResponse{
		ScanHeight: a.wallet.ScanHeight(),
		Spendable:  spendable,
		Confirmed:  confirmed,
	})
}

func (a *API) handleGetWalletTransactions(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)

	transactions, err := a.wallet.Transactions(limit, offset)
	if !a.checkServerError(c, "failed to get wallet transactions", err) {
		return
	}
	c.Encode(transactions)
}

func (a *API) handlePostWalletSend(c jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := c.Decode(&req); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func parseLimitParams(c jape.Context, defaultLimit, maxLimit int) (limit, offset int) {
	c.DecodeForm("limit", &limit)
	c.DecodeForm("offset", &offset)
	if limit > maxLimit {
		limit = maxLimit
	} else if limit <= 0 {
		limit = defaultLimit
	}

	if offset < 0 {
		offset = 0
	}
	return
}
