package api

import (
	"errors"
	"net/http"

	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func (a *API) handleGetState(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetSyncer(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetSyncerPeers(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handlePutSyncerPeer(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleDeleteSyncerPeer(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handlePostAnnounce(ctx jape.Context) {
	if err := a.settings.Announce(); err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to announce", zap.Error(err))
	}
}

func (a *API) handleGetSettings(ctx jape.Context) {
	settings, err := a.settings.Settings()
	if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to get settings", zap.Error(err))
		return
	}
	ctx.Encode(settings)
}

func (a *API) handlePutSettings(ctx jape.Context) {
	var updated settings.Settings
	if err := ctx.Decode(&updated); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	}
	if err := a.settings.UpdateSettings(updated); err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to update settings", zap.Error(err))
		return
	}
	ctx.Encode(updated)
}

func (a *API) handleGetFinancials(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetContracts(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetContract(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleDeleteSector(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetVolumes(ctx jape.Context) {
	volumes, err := a.volumes.Volumes()
	if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to get volumes", zap.Error(err))
		return
	}
	ctx.Encode(volumes)
}

func (a *API) handlePostVolume(ctx jape.Context) {
	var req AddVolumeRequest
	if err := ctx.Decode(&req); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	} else if len(req.LocalPath) == 0 {
		ctx.Error(errors.New("local path is required"), http.StatusBadRequest)
		return
	} else if req.MaxSectors == 0 {
		ctx.Error(errors.New("max sectors is required"), http.StatusBadRequest)
		return
	}

	volume, err := a.volumes.AddVolume(req.LocalPath, req.MaxSectors)
	if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to add volume", zap.Error(err))
		return
	}
	ctx.Encode(volume)
}

func (a *API) handleGetVolume(ctx jape.Context) {
	var id int
	if err := ctx.DecodeParam("id", &id); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		ctx.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	volume, err := a.volumes.Volume(id)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		ctx.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to get volume", zap.Error(err))
		return
	}
	ctx.Encode(volume)
}

func (a *API) handlePutVolume(ctx jape.Context) {
	var id int
	if err := ctx.DecodeParam("id", &id); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		ctx.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req UpdateVolumeRequest
	if err := ctx.Decode(&req); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	}

	if err := a.volumes.SetReadOnly(id, req.ReadOnly); err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to set volume read-only", zap.Error(err))
		return
	}
}

func (a *API) handleDeleteVolume(ctx jape.Context) {
	var id int
	var force bool
	if err := ctx.DecodeParam("id", &id); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	} else if id < 0 {
		ctx.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	} else if err := ctx.DecodeForm("force", &force); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	}

	if err := a.volumes.RemoveVolume(id, force); err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to remove volume", zap.Error(err))
		return
	}
}

func (a *API) handlePutVolumeResize(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handlePutVolumeCheck(ctx jape.Context) {
	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func (a *API) handleGetWalletAddress(ctx jape.Context) {
	ctx.Encode(a.wallet.Address())
}

func (a *API) handleGetWallet(ctx jape.Context) {
	spendable, confirmed, err := a.wallet.Balance()
	if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to get wallet balance", zap.Error(err))
		return
	}
	ctx.Encode(WalletResponse{
		ScanHeight: a.wallet.ScanHeight(),
		Spendable:  spendable,
		Confirmed:  confirmed,
	})
}

func (a *API) handleGetWalletTransactions(ctx jape.Context) {
	limit, offset := parseLimitParams(ctx, 100, 500)

	transactions, err := a.wallet.Transactions(limit, offset)
	if err != nil {
		ctx.Error(err, http.StatusInternalServerError)
		a.log.Warn("failed to get wallet transactions", zap.Error(err))
		return
	}
	ctx.Encode(transactions)
}

func (a *API) handlePostWalletSend(ctx jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := ctx.Decode(&req); err != nil {
		ctx.Error(err, http.StatusBadRequest)
		return
	}

	ctx.Error(errors.New("not implemented"), http.StatusInternalServerError)
}

func parseLimitParams(ctx jape.Context, defaultLimit, maxLimit int) (limit, offset int) {
	ctx.DecodeForm("limit", &limit)
	ctx.DecodeForm("offset", &offset)
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
