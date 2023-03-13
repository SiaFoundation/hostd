package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/financials"
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

func (a *API) handleGETState(c jape.Context) {
	if err := c.Error(errors.New("not implemented"), http.StatusInternalServerError); err != nil {
		return
	}
	c.Encode(struct{}{})
}

func (a *API) handleGETSyncerAddr(c jape.Context) {
	c.Encode(string(a.syncer.Address()))
}

func (a *API) handleGETSyncerPeers(c jape.Context) {
	p := a.syncer.Peers()
	peers := make([]Peer, len(p))
	for i, peer := range p {
		peers[i] = Peer{
			Address: string(peer.NetAddress),
			Version: peer.Version,
		}
	}
	c.Encode(peers)
}

func (a *API) handlePUTSyncerPeer(c jape.Context) {
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

func (a *API) handlePOSTAnnounce(c jape.Context) {
	err := a.settings.Announce()
	a.checkServerError(c, "failed to announce", err)
}

func (a *API) handleGETSettings(c jape.Context) {
	c.Encode(a.settings.Settings())
}

func (a *API) handlePOSTSettings(c jape.Context) {
	buf, err := json.Marshal(a.settings.Settings())
	if !a.checkServerError(c, "failed to marshal existing settings", err) {
		return
	}
	var current map[string]any
	err = json.Unmarshal(buf, &current)
	if !a.checkServerError(c, "failed to unmarshal existing settings", err) {
		return
	}

	var req UpdateSettingsRequest
	if err := c.Decode(&req); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	merged, err := patchSettings(current, req)
	if !a.checkServerError(c, "failed to patch settings", err) {
		return
	}

	buf, err = json.Marshal(merged)
	if !a.checkServerError(c, "failed to marshal patched settings", err) {
		return
	}

	var settings settings.Settings
	if err := json.Unmarshal(buf, &settings); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	err = a.settings.UpdateSettings(settings)
	if !a.checkServerError(c, "failed to update settings", err) {
		return
	}
	c.Encode(settings)
}

func (a *API) handleGETFinancials(c jape.Context) {
	if err := c.Error(errors.New("not implemented"), http.StatusInternalServerError); err != nil {
		return
	}
	c.Encode([]financials.Revenue{})
}

func (a *API) handleGETContracts(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)
	contracts, err := a.contracts.Contracts(limit, offset)
	if !a.checkServerError(c, "failed to get contracts", err) {
		return
	}
	c.Encode(contracts)
}

func (a *API) handleGETContract(c jape.Context) {
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

func (a *API) handleGETVolume(c jape.Context) {
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

func (a *API) handlePUTVolume(c jape.Context) {
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

func (a *API) handleGETVolumes(c jape.Context) {
	volumes, err := a.volumes.Volumes()
	if !a.checkServerError(c, "failed to get volumes", err) {
		return
	}
	c.Encode(volumes)
}

func (a *API) handlePOSTVolume(c jape.Context) {
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

func (a *API) handlePUTVolumeResize(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req ResizeVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	err := a.volumes.ResizeVolume(id, req.MaxSectors)
	a.checkServerError(c, "failed to resize volume", err)
}

func (a *API) handlePOSTVolumeCheck(c jape.Context) {
	c.Error(errors.New("not implemented"), http.StatusInternalServerError)
	c.Encode([]types.Hash256{})
}

func (a *API) handleGETWallet(c jape.Context) {
	spendable, confirmed, unconfirmed, err := a.wallet.Balance()
	if !a.checkServerError(c, "failed to get wallet", err) {
		return
	}
	c.Encode(WalletResponse{
		ScanHeight:  a.wallet.ScanHeight(),
		Address:     a.wallet.Address(),
		Spendable:   spendable,
		Confirmed:   confirmed,
		Unconfirmed: unconfirmed,
	})
}

func (a *API) handleGETWalletTransactions(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)

	transactions, err := a.wallet.Transactions(limit, offset)
	if !a.checkServerError(c, "failed to get wallet transactions", err) {
		return
	}
	c.Encode(transactions)
}

func (a *API) handleGETWalletPending(c jape.Context) {
	pending, err := a.wallet.UnconfirmedTransactions()
	if !a.checkServerError(c, "failed to get wallet pending", err) {
		return
	}
	c.Encode(pending)
}

func (a *API) handlePOSTWalletSend(c jape.Context) {
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
