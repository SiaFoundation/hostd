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
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/hostd/v2/build"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/hostd/v2/host/metrics"
	"go.sia.tech/hostd/v2/host/settings"
	"go.sia.tech/hostd/v2/host/settings/pin"
	"go.sia.tech/hostd/v2/host/storage"
	"go.sia.tech/hostd/v2/internal/disk"
	"go.sia.tech/hostd/v2/internal/prometheus"
	"go.sia.tech/hostd/v2/webhooks"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

const stdTxnSize = 1200 // bytes

var startTime = time.Now()

// checkServerError conditionally writes an error to the response if err is not
// nil.
func (a *api) checkServerError(jc jape.Context, context string, err error) bool {
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *api) writeResponse(jc jape.Context, resp any) {
	var responseFormat string
	if err := jc.DecodeForm("response", &responseFormat); err != nil {
		return
	}

	if resp != nil {
		switch responseFormat {
		case "prometheus":
			v, ok := resp.(prometheus.Marshaller)
			if !ok {
				err := fmt.Errorf("response does not implement prometheus.Marshaller %T", resp)
				jc.Error(err, http.StatusInternalServerError)
				a.log.Error("response does not implement prometheus.Marshaller", zap.Stack("stack"), zap.Error(err))
				return
			}

			enc := prometheus.NewEncoder(jc.ResponseWriter)
			if err := enc.Append(v); err != nil {
				a.log.Error("failed to marshal prometheus response", zap.Error(err))
				return
			}
		default:
			jc.Encode(resp)
		}
	}
}

func (a *api) handleGETState(jc jape.Context) {
	announcement, err := a.settings.LastAnnouncement()
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	var baseURL string
	if a.explorer != nil {
		baseURL = a.explorer.BaseURL()
	}

	a.writeResponse(jc, State{
		Name:             a.name,
		PublicKey:        a.hostKey,
		StartTime:        startTime,
		LastAnnouncement: announcement,
		Explorer: ExplorerState{
			Enabled: !a.explorerDisabled,
			URL:     baseURL,
		},
		BuildState: BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: build.Time(),
		},
	})
}

func (a *api) handleGETConsensusTip(jc jape.Context) {
	a.writeResponse(jc, ConsensusIndexResp(a.chain.Tip()))
}
func (a *api) handleGETConsensusTipState(jc jape.Context) {
	a.writeResponse(jc, ConsensusStateResp(a.chain.TipState()))
}
func (a *api) handleGETConsensusNetwork(jc jape.Context) {
	jc.Encode(a.chain.TipState().Network)
}

func (a *api) handleGETSyncerAddr(jc jape.Context) {
	a.writeResponse(jc, SyncerAddrResp(a.syncer.Addr()))
}

func (a *api) handleGETSyncerPeers(jc jape.Context) {
	var peers []Peer
	for _, p := range a.syncer.Peers() {
		// create peer response with known fields
		peer := Peer{
			Address: p.Addr(),
			Inbound: p.Inbound,
			Version: p.Version(),
		}
		//  add more info if available
		info, err := a.syncer.PeerInfo(p.Addr())
		if errors.Is(err, syncer.ErrPeerNotFound) {
			continue
		} else if err != nil {
			jc.Error(err, http.StatusInternalServerError)
			return
		}
		peer.FirstSeen = info.FirstSeen
		peer.ConnectedSince = info.LastConnect
		peer.SyncedBlocks = info.SyncedBlocks
		peer.SyncDuration = info.SyncDuration
		peers = append(peers, peer)
	}
	a.writeResponse(jc, PeerResp(peers))
}

func (a *api) handlePUTSyncerPeer(jc jape.Context) {
	var req SyncerConnectRequest
	if err := jc.Decode(&req); err != nil {
		return
	}
	_, err := a.syncer.Connect(jc.Request.Context(), req.Address)
	a.checkServerError(jc, "failed to connect to peer", err)
}

func (a *api) handleGETIndexTip(jc jape.Context) {
	a.writeResponse(jc, IndexTipResp(a.index.Tip()))
}

func (a *api) handleGETAlerts(jc jape.Context) {
	a.writeResponse(jc, AlertResp(a.alerts.Active()))
}

func (a *api) handlePOSTAlertsDismiss(jc jape.Context) {
	var ids []types.Hash256
	if err := jc.Decode(&ids); err != nil {
		return
	} else if len(ids) == 0 {
		jc.Error(errors.New("no alerts to dismiss"), http.StatusBadRequest)
		return
	}
	a.alerts.Dismiss(ids...)
}

func (a *api) handlePOSTAnnounce(jc jape.Context) {
	err := a.settings.Announce()
	a.checkServerError(jc, "failed to announce", err)
}

func (a *api) handleGETSettings(jc jape.Context) {
	a.writeResponse(jc, HostSettings(a.settings.Settings()))
}

func (a *api) handlePATCHSettings(jc jape.Context) {
	buf, err := json.Marshal(a.settings.Settings())
	if !a.checkServerError(jc, "failed to marshal existing settings", err) {
		return
	}
	var current map[string]any
	err = json.Unmarshal(buf, &current)
	if !a.checkServerError(jc, "failed to unmarshal existing settings", err) {
		return
	}

	var req map[string]any
	if err := jc.Decode(&req); err != nil {
		return
	}

	err = patchSettings(current, req)
	if !a.checkServerError(jc, "failed to patch settings", err) {
		return
	}

	buf, err = json.Marshal(current)
	if !a.checkServerError(jc, "failed to marshal patched settings", err) {
		return
	}

	var settings settings.Settings
	if err := json.Unmarshal(buf, &settings); err != nil {
		jc.Error(err, http.StatusBadRequest)
		return
	}

	err = a.settings.UpdateSettings(settings)
	if !a.checkServerError(jc, "failed to update settings", err) {
		return
	}

	// Resize the cache based on the updated settings
	a.volumes.ResizeCache(settings.SectorCacheSize)

	jc.Encode(a.settings.Settings())
}

func (a *api) handleGETPinnedSettings(jc jape.Context) {
	jc.Encode(a.pinned.Pinned(jc.Request.Context()))
}

func (a *api) handlePUTPinnedSettings(jc jape.Context) {
	var req pin.PinnedSettings
	if err := jc.Decode(&req); err != nil {
		return
	}

	a.checkServerError(jc, "failed to update pinned settings", a.pinned.Update(jc.Request.Context(), req))
}

func (a *api) handlePUTDDNSUpdate(jc jape.Context) {
	err := a.settings.UpdateDDNS(true)
	a.checkServerError(jc, "failed to update dynamic DNS", err)
}

func (a *api) handleGETMetrics(jc jape.Context) {
	var timestamp time.Time
	if err := jc.DecodeForm("timestamp", &timestamp); err != nil {
		return
	} else if timestamp.IsZero() {
		timestamp = time.Now()
	}

	metrics, err := a.metrics.Metrics(timestamp)
	if !a.checkServerError(jc, "failed to get metrics", err) {
		return
	}

	a.writeResponse(jc, Metrics(metrics))
}

func (a *api) handleGETPeriodMetrics(jc jape.Context) {
	var interval metrics.Interval
	if err := jc.DecodeParam("period", &interval); err != nil {
		return
	}
	var start time.Time
	var periods int
	if err := jc.DecodeForm("start", &start); err != nil {
		return
	} else if err := jc.DecodeForm("periods", &periods); err != nil {
		return
	} else if start.IsZero() {
		jc.Error(errors.New("start time cannot be zero"), http.StatusBadRequest)
		return
	} else if start.After(time.Now()) {
		jc.Error(errors.New("start time cannot be in the future"), http.StatusBadRequest)
		return
	}

	start, err := metrics.Normalize(start, interval)
	if err != nil {
		jc.Error(err, http.StatusBadRequest)
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
	if !a.checkServerError(jc, "failed to get metrics", err) {
		return
	}
	jc.Encode(period)
}

func (a *api) handlePostContracts(jc jape.Context) {
	var filter contracts.ContractFilter
	if err := jc.Decode(&filter); err != nil {
		return
	}

	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 500
	}

	contracts, count, err := a.contracts.Contracts(filter)
	if !a.checkServerError(jc, "failed to get contracts", err) {
		return
	}
	jc.Encode(ContractsResponse{
		Contracts: contracts,
		Count:     count,
	})
}

func (a *api) handleGETContract(jc jape.Context) {
	var id types.FileContractID
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	}
	contract, err := a.contracts.Contract(id)
	if errors.Is(err, contracts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if !a.checkServerError(jc, "failed to get contract", err) {
		return
	}
	jc.Encode(contract)
}

func (a *api) handlePOSTV2Contracts(jc jape.Context) {
	var filter contracts.V2ContractFilter
	if err := jc.Decode(&filter); err != nil {
		return
	}

	if filter.Limit <= 0 || filter.Limit > 500 {
		filter.Limit = 500
	}

	contracts, count, err := a.contracts.V2Contracts(filter)
	if !a.checkServerError(jc, "failed to get contracts", err) {
		return
	}
	jc.Encode(V2ContractsResponse{
		Contracts: contracts,
		Count:     count,
	})
}

func (a *api) handleGETV2Contract(jc jape.Context) {
	var id types.FileContractID
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	}
	contract, err := a.contracts.V2Contract(id)
	if errors.Is(err, contracts.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if !a.checkServerError(jc, "failed to get contract", err) {
		return
	}
	jc.Encode(contract)
}

func (a *api) handleGETVolume(jc jape.Context) {
	var id int64
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		jc.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	volume, err := a.volumes.Volume(id)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if !a.checkServerError(jc, "failed to get volume", err) {
		return
	}
	jc.Encode(toJSONVolume(volume))
}

func (a *api) handlePUTVolume(jc jape.Context) {
	var id int64
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	} else if id < 0 {
		jc.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req UpdateVolumeRequest
	if err := jc.Decode(&req); err != nil {
		return
	}

	err := a.volumes.SetReadOnly(id, req.ReadOnly)
	if errors.Is(err, storage.ErrVolumeNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	a.checkServerError(jc, "failed to update volume", err)
}

func (a *api) handleDeleteSector(jc jape.Context) {
	var root types.Hash256
	if err := jc.DecodeParam("root", &root); err != nil {
		return
	}
	err := a.volumes.RemoveSector(root)
	a.checkServerError(jc, "failed to remove sector", err)
}

func (a *api) handleGETWallet(jc jape.Context) {
	balance, err := a.wallet.Balance()
	if !a.checkServerError(jc, "failed to get wallet", err) {
		return
	}
	a.writeResponse(jc, WalletResponse{
		Balance: balance,
		Address: a.wallet.Address(),
	})
}

func (a *api) handleGETWalletEvents(jc jape.Context) {
	limit, offset := parseLimitParams(jc, 100, 500)

	transactions, err := a.wallet.Events(offset, limit)
	if !a.checkServerError(jc, "failed to get events", err) {
		return
	}

	a.writeResponse(jc, WalletTransactionsResp(transactions))
}

func (a *api) handleGETWalletPending(jc jape.Context) {
	pending, err := a.wallet.UnconfirmedEvents()
	if !a.checkServerError(jc, "failed to get wallet pending", err) {
		return
	}
	a.writeResponse(jc, WalletPendingResp(pending))
}

func (a *api) handlePOSTWalletSend(jc jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := jc.Decode(&req); err != nil {
		return
	} else if req.Address == types.VoidAddress {
		jc.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// estimate miner fee
	feePerByte := a.wallet.RecommendedFee()
	minerFee := feePerByte.Mul64(stdTxnSize)
	if req.SubtractMinerFee {
		var underflow bool
		req.Amount, underflow = req.Amount.SubWithUnderflow(minerFee)
		if underflow {
			jc.Error(fmt.Errorf("amount must be greater than miner fee: %s", minerFee), http.StatusBadRequest)
			return
		}
	}

	state := a.chain.TipState()
	// if the current height is below the v2 hardfork height, send a v1
	// transaction
	if state.Index.Height < state.Network.HardforkV2.AllowHeight {
		// build transaction
		txn := types.Transaction{
			MinerFees: []types.Currency{minerFee},
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: req.Address, Value: req.Amount},
			},
		}
		toSign, err := a.wallet.FundTransaction(&txn, req.Amount.Add(minerFee), false)
		if !a.checkServerError(jc, "failed to fund transaction", err) {
			return
		}
		a.wallet.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
		// shouldn't be necessary to get parents since the transaction is
		// not using unconfirmed outputs, but good practice
		txnset := append(a.chain.UnconfirmedParents(txn), txn)
		if err := a.wallet.BroadcastTransactionSet(txnset); !a.checkServerError(jc, "failed to broadcast transaction set", err) {
			a.wallet.ReleaseInputs([]types.Transaction{txn}, nil)
			return
		}
		jc.Encode(txn.ID())
	} else {
		txn := types.V2Transaction{
			MinerFee: minerFee,
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: req.Address, Value: req.Amount},
			},
		}
		// fund and sign transaction
		basis, toSign, err := a.wallet.FundV2Transaction(&txn, req.Amount.Add(minerFee), false)
		if !a.checkServerError(jc, "failed to fund transaction", err) {
			return
		}
		a.wallet.SignV2Inputs(&txn, toSign)
		basis, txnset, err := a.chain.V2TransactionSet(basis, txn)
		if !a.checkServerError(jc, "failed to create transaction set", err) {
			a.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			return
		}
		// verify the transaction and add it to the transaction pool
		if err := a.wallet.BroadcastV2TransactionSet(basis, txnset); err != nil {
			a.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
			return
		}
		jc.Encode(txn.ID())
	}
}

func (a *api) handleGETSystemDir(jc jape.Context) {
	var path string
	if err := jc.DecodeForm("path", &path); err != nil {
		return
	}

	if runtime.GOOS == "windows" {
		// special handling for / on Windows
		if path == `/` || path == `\` {
			drives, err := disk.Drives()
			if !a.checkServerError(jc, "failed to get drives", err) {
				return
			}
			jc.Encode(SystemDirResponse{
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
		if err != nil || path == "" {
			a.log.Debug("failed to get home dir", zap.Error(err), zap.String("path", path))
			// try to get the working directory instead
			path, err = os.Getwd()
			if err != nil {
				jc.Error(fmt.Errorf("failed to get home dir: %w", err), http.StatusInternalServerError)
				return
			}
		}
	case ".", "":
		var err error
		path, err = os.Getwd()
		if err != nil {
			jc.Error(fmt.Errorf("failed to get working dir: %w", err), http.StatusInternalServerError)
			return
		}
	}

	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		jc.Error(errors.New("path must be absolute"), http.StatusBadRequest)
		return
	}
	dir, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		jc.Error(fmt.Errorf("path does not exist: %w", err), http.StatusNotFound)
		return
	} else if !a.checkServerError(jc, "failed to read dir", err) {
		return
	}

	// get disk usage
	free, total, err := disk.Usage(path)
	if !a.checkServerError(jc, "failed to get disk usage", err) {
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
	jc.Encode(resp)
}

func (a *api) handlePUTSystemDir(jc jape.Context) {
	var req CreateDirRequest
	if err := jc.Decode(&req); err != nil {
		return
	}
	a.checkServerError(jc, "failed to create dir", os.MkdirAll(req.Path, 0775))
}

func (a *api) handlePOSTSystemSQLite3Backup(jc jape.Context) {
	if a.sqlite3Store == nil {
		jc.Error(errors.New("sqlite3 store not available"), http.StatusNotFound)
		return
	}

	var req BackupRequest
	if err := jc.Decode(&req); err != nil {
		return
	}
	a.checkServerError(jc, "failed to backup", a.sqlite3Store.Backup(jc.Request.Context(), req.Path))
}

func (a *api) handleGETTPoolFee(jc jape.Context) {
	a.writeResponse(jc, TPoolResp(a.wallet.RecommendedFee()))
}

func (a *api) handleGETAccounts(jc jape.Context) {
	limit, offset := parseLimitParams(jc, 100, 500)
	accounts, err := a.accounts.Accounts(limit, offset)
	if !a.checkServerError(jc, "failed to get accounts", err) {
		return
	}
	jc.Encode(accounts)
}

func (a *api) handleGETAccountFunding(jc jape.Context) {
	var account rhp3.Account
	if err := jc.DecodeParam("account", &account); err != nil {
		return
	}
	funding, err := a.accounts.AccountFunding(account)
	if !a.checkServerError(jc, "failed to get account funding", err) {
		return
	}
	jc.Encode(funding)
}

func (a *api) handleGETWebhooks(jc jape.Context) {
	hooks, err := a.webhooks.Webhooks()
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(hooks)
}

func (a *api) handlePOSTWebhooks(jc jape.Context) {
	var req RegisterWebHookRequest
	if err := jc.Decode(&req); err != nil {
		return
	}

	hook, err := a.webhooks.RegisterWebhook(req.CallbackURL, req.Scopes)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(hook)
}

func (a *api) handlePUTWebhooks(jc jape.Context) {
	var id int64
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	}
	var req RegisterWebHookRequest
	if err := jc.Decode(&req); err != nil {
		return
	}

	_, err := a.webhooks.UpdateWebhook(id, req.CallbackURL, req.Scopes)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (a *api) handlePOSTWebhooksTest(jc jape.Context) {
	var id int64
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	}

	if err := a.webhooks.BroadcastToWebhook(id, "test", webhooks.ScopeTest, nil); err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (a *api) handleDELETEWebhooks(jc jape.Context) {
	var id int64
	if err := jc.DecodeParam("id", &id); err != nil {
		return
	}

	err := a.webhooks.RemoveWebhook(id)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
}

func (a *api) handlePUTSystemConnectTest(jc jape.Context) {
	result, _, err := a.settings.TestConnection(jc.Request.Context())
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(result)
}

func parseLimitParams(jc jape.Context, defaultLimit, maxLimit int) (limit, offset int) {
	if err := jc.DecodeForm("limit", &limit); err != nil {
		return
	} else if err := jc.DecodeForm("offset", &offset); err != nil {
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
