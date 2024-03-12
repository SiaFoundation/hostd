package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/settings/pin"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/disk"
	"go.sia.tech/hostd/internal/prometheus"
	"go.sia.tech/hostd/webhooks"
	"go.sia.tech/jape"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

const stdTxnSize = 1200 // bytes

var startTime = time.Now()

// checkServerError conditionally writes an error to the response if err is not
// nil.
func (a *api) checkServerError(c jape.Context, context string, err error) bool {
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *api) writeResponse(c jape.Context, resp any) {
	var responseFormat string
	if err := c.DecodeForm("response", &responseFormat); err != nil {
		return
	}

	if resp != nil {
		switch responseFormat {
		case "prometheus":
			v, ok := resp.(prometheus.Marshaller)
			if !ok {
				err := fmt.Errorf("response does not implement prometheus.Marshaller %T", resp)
				c.Error(err, http.StatusInternalServerError)
				a.log.Error("response does not implement prometheus.Marshaller", zap.Stack("stack"), zap.Error(err))
				return
			}

			enc := prometheus.NewEncoder(c.ResponseWriter)
			if err := enc.Append(v); err != nil {
				a.log.Error("failed to marshal prometheus response", zap.Error(err))
				return
			}
		default:
			c.Encode(resp)
		}
	}
}

func (a *api) handleGETHostState(c jape.Context) {
	announcement, err := a.settings.LastAnnouncement()
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}

	a.writeResponse(c, HostState{
		Name:             a.name,
		PublicKey:        a.hostKey,
		WalletAddress:    a.wallet.Address(),
		StartTime:        startTime,
		LastAnnouncement: announcement,
		BuildState: BuildState{
			Network:   build.NetworkName(),
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: build.Time(),
		},
	})
}

func (a *api) handleGETConsensusState(c jape.Context) {
	a.writeResponse(c, ConsensusState{
		Synced:     a.chain.Synced(),
		ChainIndex: a.chain.TipState().Index,
	})
}

func (a *api) handleGETSyncerAddr(c jape.Context) {
	a.writeResponse(c, SyncerAddrResp(a.syncer.Address()))
}

func (a *api) handleGETSyncerPeers(c jape.Context) {
	p := a.syncer.Peers()
	peers := make([]Peer, len(p))
	for i, peer := range p {
		peers[i] = Peer{
			Address: string(peer.NetAddress),
			Version: peer.Version,
		}
	}
	a.writeResponse(c, PeerResp(peers))
}

func (a *api) handlePUTSyncerPeer(c jape.Context) {
	var req SyncerConnectRequest
	if err := c.Decode(&req); err != nil {
		return
	}
	err := a.syncer.Connect(modules.NetAddress(req.Address))
	a.checkServerError(c, "failed to connect to peer", err)
}

func (a *api) handleDeleteSyncerPeer(c jape.Context) {
	var addr modules.NetAddress
	if err := c.DecodeParam("address", &addr); err != nil {
		return
	}
	err := a.syncer.Disconnect(addr)
	a.checkServerError(c, "failed to disconnect from peer", err)
}

func (a *api) handleGETAlerts(c jape.Context) {
	a.writeResponse(c, AlertResp(a.alerts.Active()))
}

func (a *api) handlePOSTAlertsDismiss(c jape.Context) {
	var ids []types.Hash256
	if err := c.Decode(&ids); err != nil {
		return
	} else if len(ids) == 0 {
		c.Error(errors.New("no alerts to dismiss"), http.StatusBadRequest)
		return
	}
	a.alerts.Dismiss(ids...)
}

func (a *api) handlePOSTAnnounce(c jape.Context) {
	err := a.settings.Announce()
	a.checkServerError(c, "failed to announce", err)
}

func (a *api) handleGETSettings(c jape.Context) {
	hs := HostSettings(a.settings.Settings())
	a.writeResponse(c, hs)
}

func (a *api) handlePATCHSettings(c jape.Context) {
	buf, err := json.Marshal(a.settings.Settings())
	if !a.checkServerError(c, "failed to marshal existing settings", err) {
		return
	}
	var current map[string]any
	err = json.Unmarshal(buf, &current)
	if !a.checkServerError(c, "failed to unmarshal existing settings", err) {
		return
	}

	var req map[string]any
	if err := c.Decode(&req); err != nil {
		return
	}

	err = patchSettings(current, req)
	if !a.checkServerError(c, "failed to patch settings", err) {
		return
	}

	buf, err = json.Marshal(current)
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

	// Resize the cache based on the updated settings
	a.volumes.ResizeCache(settings.SectorCacheSize)

	c.Encode(a.settings.Settings())
}

func (a *api) handleGETPinnedSettings(c jape.Context) {
	if a.pinned == nil {
		c.Error(errors.New("pinned settings disabled"), http.StatusNotFound)
		return
	}
	c.Encode(a.pinned.Pinned(c.Request.Context()))
}

func (a *api) handlePUTPinnedSettings(c jape.Context) {
	if a.pinned == nil {
		c.Error(errors.New("pinned settings disabled"), http.StatusNotFound)
		return
	}

	var req pin.PinnedSettings
	if err := c.Decode(&req); err != nil {
		return
	}

	a.checkServerError(c, "failed to update pinned settings", a.pinned.Update(c.Request.Context(), req))
}

func (a *api) handlePUTDDNSUpdate(c jape.Context) {
	err := a.settings.UpdateDDNS(true)
	a.checkServerError(c, "failed to update dynamic DNS", err)
}

func (a *api) handleGETMetrics(c jape.Context) {
	var timestamp time.Time
	if err := c.DecodeForm("timestamp", &timestamp); err != nil {
		return
	} else if timestamp.IsZero() {
		timestamp = time.Now()
	}

	metrics, err := a.metrics.Metrics(timestamp)
	if !a.checkServerError(c, "failed to get metrics", err) {
		return
	}

	a.writeResponse(c, Metrics(metrics))
}

func (a *api) handleGETPeriodMetrics(c jape.Context) {
	var interval metrics.Interval
	if err := c.DecodeParam("period", &interval); err != nil {
		return
	}
	var start time.Time
	var periods int
	if err := c.DecodeForm("start", &start); err != nil {
		return
	} else if err := c.DecodeForm("periods", &periods); err != nil {
		return
	} else if start.IsZero() {
		c.Error(errors.New("start time cannot be zero"), http.StatusBadRequest)
		return
	} else if start.After(time.Now()) {
		c.Error(errors.New("start time cannot be in the future"), http.StatusBadRequest)
	}

	start, err := metrics.Normalize(start, interval)
	if err != nil {
		c.Error(err, http.StatusBadRequest)
		return
	}

	if periods == 0 {
		// if periods is 0 calculate the number of periods between start and now
		switch interval {
		case metrics.Interval5Minutes:
			periods = int(time.Now().Truncate(5*time.Minute).Sub(start)/(5*time.Minute)) + 1
		case metrics.Interval15Minutes:
			periods = int(time.Now().Truncate(15*time.Minute).Sub(start)/(15*time.Minute)) + 1
		case metrics.IntervalHourly:
			periods = int(time.Now().Truncate(time.Hour).Sub(start)/time.Hour) + 1
		case metrics.IntervalDaily:
			y, m, d := time.Now().Date()
			end := time.Date(y, m, d, 0, 0, 0, 0, start.Location())
			periods = int(end.Sub(start)/(24*time.Hour)) + 1
		case metrics.IntervalWeekly:
			end := time.Now()
			y, m, d := end.Date()
			end = time.Date(y, m, d+int(end.Weekday()), 0, 0, 0, 0, start.Location())
			periods = int(end.Sub(start)/(7*24*time.Hour)) + 1
		case metrics.IntervalMonthly:
			y, m, _ := start.Date()
			y1, m1, _ := time.Now().Date()
			periods = int((y1-y)*12+int(m1-m)) + 1
		case metrics.IntervalYearly:
			y, _, _ := start.Date()
			y1, _, _ := time.Now().Date()
			periods = int(y1-y) + 1
		}
	}

	period, err := a.metrics.PeriodMetrics(start, periods, interval)
	if !a.checkServerError(c, "failed to get metrics", err) {
		return
	}
	c.Encode(period)
}

func (a *api) handlePostContracts(c jape.Context) {
	var filter contracts.ContractFilter
	if err := c.Decode(&filter); err != nil {
		return
	}

	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 500
	}

	contracts, count, err := a.contracts.Contracts(filter)
	if !a.checkServerError(c, "failed to get contracts", err) {
		return
	}
	c.Encode(ContractsResponse{
		Contracts: contracts,
		Count:     count,
	})
}

func (a *api) handleGETContract(c jape.Context) {
	var id types.FileContractID
	if err := c.DecodeParam("id", &id); err != nil {
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

func (a *api) handleGETVolume(c jape.Context) {
	var id int64
	if err := c.DecodeParam("id", &id); err != nil {
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
	c.Encode(toJSONVolume(volume))
}

func (a *api) handlePUTVolume(c jape.Context) {
	var id int64
	if err := c.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req UpdateVolumeRequest
	if err := c.Decode(&req); err != nil {
		return
	}

	err := a.volumes.SetReadOnly(id, req.ReadOnly)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		c.Error(err, http.StatusNotFound)
		return
	}
	a.checkServerError(c, "failed to update volume", err)
}

func (a *api) handleDeleteSector(c jape.Context) {
	var root types.Hash256
	if err := c.DecodeParam("root", &root); err != nil {
		return
	}
	err := a.volumes.RemoveSector(root)
	a.checkServerError(c, "failed to remove sector", err)
}

func (a *api) handleGETWallet(c jape.Context) {
	spendable, confirmed, unconfirmed, err := a.wallet.Balance()
	if !a.checkServerError(c, "failed to get wallet", err) {
		return
	}
	a.writeResponse(c, WalletResponse{
		ScanHeight:  a.wallet.ScanHeight(),
		Address:     a.wallet.Address(),
		Spendable:   spendable,
		Confirmed:   confirmed,
		Unconfirmed: unconfirmed,
	})
}

func (a *api) handleGETWalletTransactions(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)

	transactions, err := a.wallet.Transactions(limit, offset)
	if !a.checkServerError(c, "failed to get wallet transactions", err) {
		return
	}

	a.writeResponse(c, WalletTransactionsResp(transactions))
}

func (a *api) handleGETWalletPending(c jape.Context) {
	pending, err := a.wallet.UnconfirmedTransactions()
	if !a.checkServerError(c, "failed to get wallet pending", err) {
		return
	}
	a.writeResponse(c, WalletPendingResp(pending))
}

func (a *api) handlePOSTWalletSend(c jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := c.Decode(&req); err != nil {
		return
	} else if req.Address == types.VoidAddress {
		c.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// estimate miner fee
	feePerByte := a.tpool.RecommendedFee()
	minerFee := feePerByte.Mul64(stdTxnSize)
	if req.SubtractMinerFee {
		var underflow bool
		req.Amount, underflow = req.Amount.SubWithUnderflow(minerFee)
		if underflow {
			c.Error(fmt.Errorf("amount must be greater than miner fee: %s", minerFee), http.StatusBadRequest)
			return
		}
	}

	// build transaction
	txn := types.Transaction{
		MinerFees: []types.Currency{minerFee},
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: req.Address, Value: req.Amount},
		},
	}
	// fund and sign transaction
	toSign, release, err := a.wallet.FundTransaction(&txn, req.Amount.Add(minerFee))
	if !a.checkServerError(c, "failed to fund transaction", err) {
		return
	}
	defer release()
	err = a.wallet.SignTransaction(a.chain.TipState(), &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if !a.checkServerError(c, "failed to sign transaction", err) {
		return
	}
	// broadcast transaction
	err = a.tpool.AcceptTransactionSet([]types.Transaction{txn})
	if !a.checkServerError(c, "failed to broadcast transaction", err) {
		return
	}
	c.Encode(txn.ID())
}

func (a *api) handleGETSystemDir(c jape.Context) {
	var path string
	if err := c.DecodeForm("path", &path); err != nil {
		return
	}

	if runtime.GOOS == "windows" {
		// special handling for / on Windows
		if path == `/` || path == `\` {
			drives, err := disk.Drives()
			if !a.checkServerError(c, "failed to get drives", err) {
				return
			}
			c.Encode(SystemDirResponse{
				Path:        path,
				Directories: drives,
			})
			return
		}

		// add a trailing slash to the root path
		if len(path) == 2 && path[1] == ':' && ('a' <= path[0] && path[0] <= 'z' || 'A' <= path[0] && path[0] <= 'Z') {
			path += string(filepath.Separator)
		}
	}

	switch path {
	case "~":
		var err error
		path, err = os.UserHomeDir()
		if err != nil || len(path) == 0 {
			a.log.Debug("failed to get home dir", zap.Error(err), zap.String("path", path))
			// try to get the working directory instead
			path, err = os.Getwd()
			if err != nil {
				c.Error(fmt.Errorf("failed to get home dir: %w", err), http.StatusInternalServerError)
				return
			}
		}
	case ".", "":
		var err error
		path, err = os.Getwd()
		if err != nil {
			c.Error(fmt.Errorf("failed to get working dir: %w", err), http.StatusInternalServerError)
			return
		}
	}

	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		c.Error(errors.New("path must be absolute"), http.StatusBadRequest)
		return
	}
	dir, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		c.Error(fmt.Errorf("path does not exist: %w", err), http.StatusNotFound)
		return
	} else if !a.checkServerError(c, "failed to read dir", err) {
		return
	}

	// get disk usage
	free, total, err := disk.Usage(path)
	if !a.checkServerError(c, "failed to get disk usage", err) {
		return
	}

	resp := SystemDirResponse{
		Path:       path,
		FreeBytes:  free,
		TotalBytes: total,
	}

	for _, entry := range dir {
		if entry.IsDir() {
			resp.Directories = append(resp.Directories, entry.Name())
		}
	}
	c.Encode(resp)
}

func (a *api) handlePUTSystemDir(c jape.Context) {
	var req CreateDirRequest
	if err := c.Decode(&req); err != nil {
		return
	}
	a.checkServerError(c, "failed to create dir", os.MkdirAll(req.Path, 0775))
}

func (a *api) handleGETTPoolFee(c jape.Context) {
	a.writeResponse(c, TPoolResp(a.tpool.RecommendedFee()))
}

func (a *api) handleGETAccounts(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)
	accounts, err := a.accounts.Accounts(limit, offset)
	if !a.checkServerError(c, "failed to get accounts", err) {
		return
	}
	c.Encode(accounts)
}

func (a *api) handleGETAccountFunding(c jape.Context) {
	var account rhp3.Account
	if err := c.DecodeParam("account", &account); err != nil {
		return
	}
	funding, err := a.accounts.AccountFunding(account)
	if !a.checkServerError(c, "failed to get account funding", err) {
		return
	}
	c.Encode(funding)
}

func (a *api) handleGETWebhooks(c jape.Context) {
	hooks, err := a.webhooks.WebHooks()
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	c.Encode(hooks)
}

func (a *api) handlePOSTWebhooks(c jape.Context) {
	var req RegisterWebHookRequest
	if err := c.Decode(&req); err != nil {
		return
	}

	hook, err := a.webhooks.RegisterWebHook(req.CallbackURL, req.Scopes)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
	c.Encode(hook)
}

func (a *api) handlePUTWebhooks(c jape.Context) {
	var id int64
	if err := c.DecodeParam("id", &id); err != nil {
		return
	}
	var req RegisterWebHookRequest
	if err := c.Decode(&req); err != nil {
		return
	}

	_, err := a.webhooks.UpdateWebHook(id, req.CallbackURL, req.Scopes)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
}

func (a *api) handlePOSTWebhooksTest(c jape.Context) {
	var id int64
	if err := c.DecodeParam("id", &id); err != nil {
		return
	}

	if err := a.webhooks.BroadcastToWebhook(id, "test", webhooks.ScopeTest, nil); err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
}

func (a *api) handleDELETEWebhooks(c jape.Context) {
	var id int64
	if err := c.DecodeParam("id", &id); err != nil {
		return
	}

	err := a.webhooks.RemoveWebHook(id)
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		return
	}
}

func parseLimitParams(c jape.Context, defaultLimit, maxLimit int) (limit, offset int) {
	if err := c.DecodeForm("limit", &limit); err != nil {
		return
	} else if err := c.DecodeForm("offset", &offset); err != nil {
		return
	}
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

func toJSONVolume(vol storage.VolumeMeta) VolumeMeta {
	jvm := VolumeMeta{
		VolumeMeta: vol,
	}
	for _, err := range vol.Errors {
		jvm.Errors = append(jvm.Errors, err)
	}
	return jvm
}
