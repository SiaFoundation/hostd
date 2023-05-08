package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/hostd/host/metrics"
	"go.sia.tech/hostd/host/settings"
	"go.sia.tech/hostd/host/storage"
	"go.sia.tech/hostd/internal/disk"
	"go.sia.tech/hostd/logging"
	"go.sia.tech/jape"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

const stdTxnSize = 1200

// checkServerError conditionally writes an error to the response if err is not
// nil.
func (a *api) checkServerError(c jape.Context, context string, err error) bool {
	if err != nil {
		c.Error(err, http.StatusInternalServerError)
		a.log.Warn(context, zap.Error(err))
	}
	return err == nil
}

func (a *api) handleGETHostState(c jape.Context) {
	c.Encode(HostState{
		PublicKey:     a.hostKey,
		WalletAddress: a.wallet.Address(),
		BuildState: BuildState{
			Network:   build.NetworkName(),
			Version:   build.Version(),
			Commit:    build.Commit(),
			BuildTime: build.BuildTime(),
		},
	})
}
func (a *api) handleGETConsensusState(c jape.Context) {
	c.Encode(ConsensusState{
		Synced:     a.chain.Synced(),
		ChainIndex: a.chain.TipState().Index,
	})
}

func (a *api) handleGETSyncerAddr(c jape.Context) {
	c.Encode(string(a.syncer.Address()))
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
	c.Encode(peers)
}

func (a *api) handlePUTSyncerPeer(c jape.Context) {
	var req SyncerConnectRequest
	if err := c.Decode(&req); err != nil {
		c.Error(fmt.Errorf("failed to parse peer address: %w", err), http.StatusBadRequest)
		return
	}
	err := a.syncer.Connect(modules.NetAddress(req.Address))
	a.checkServerError(c, "failed to connect to peer", err)
}

func (a *api) handleDeleteSyncerPeer(c jape.Context) {
	var addr modules.NetAddress
	if err := c.DecodeParam("address", &addr); err != nil {
		c.Error(fmt.Errorf("failed to parse peer address: %w", err), http.StatusBadRequest)
		return
	}
	err := a.syncer.Disconnect(addr)
	a.checkServerError(c, "failed to disconnect from peer", err)
}

func (a *api) handlePOSTAnnounce(c jape.Context) {
	err := a.settings.Announce()
	a.checkServerError(c, "failed to announce", err)
}

func (a *api) handleGETSettings(c jape.Context) {
	c.Encode(a.settings.Settings())
}

func (a *api) handlePOSTSettings(c jape.Context) {
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
		c.Error(fmt.Errorf("failed to parse update request: %w", err), http.StatusBadRequest)
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

func (a *api) handlePUTDynDNSUpdate(c jape.Context) {
	err := a.settings.UpdateDynDNS(true)
	a.checkServerError(c, "failed to update dynamic DNS", err)
}

func (a *api) handleGETMetrics(c jape.Context) {
	var timestamp time.Time
	c.DecodeForm("timestamp", &timestamp)
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	metrics, err := a.metrics.Metrics(timestamp)
	if !a.checkServerError(c, "failed to get metrics", err) {
		return
	}
	c.Encode(metrics)
}

func (a *api) handleGETPeriodMetrics(c jape.Context) {
	var interval metrics.Interval
	if err := c.DecodeParam("period", &interval); err != nil {
		c.Error(fmt.Errorf("failed to parse period: %w", err), http.StatusBadRequest)
	}
	var start time.Time
	var periods int
	if err := c.DecodeForm("start", &start); err != nil {
		c.Error(fmt.Errorf("failed to parse start time: %w", err), http.StatusBadRequest)
		return
	} else if err := c.DecodeForm("periods", &periods); err != nil {
		c.Error(fmt.Errorf("failed to parse end time: %w", err), http.StatusBadRequest)
		return
	} else if start.IsZero() {
		c.Error(errors.New("start time cannot be zero"), http.StatusBadRequest)
		return
	} else if periods <= 0 {
		c.Error(errors.New("periods must be greater than zero"), http.StatusBadRequest)
		return
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
		c.Error(fmt.Errorf("failed to parse filter: %w", err), http.StatusBadRequest)
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
		c.Error(fmt.Errorf("failed to parse contract ID: %w", err), http.StatusBadRequest)
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
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(fmt.Errorf("failed to parse volume id: %w", err), http.StatusBadRequest)
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
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(fmt.Errorf("failed to parse volume id: %w", err), http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req UpdateVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(fmt.Errorf("failed to parse volume request: %w", err), http.StatusBadRequest)
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
		c.Error(fmt.Errorf("failed to parse sector root: %w", err), http.StatusBadRequest)
	}
	err := a.volumes.RemoveSector(root)
	a.checkServerError(c, "failed to remove sector", err)
}

func (a *api) handleGETVolumes(c jape.Context) {
	volumes, err := a.volumes.Volumes()
	if !a.checkServerError(c, "failed to get volumes", err) {
		return
	}
	var jsonVolumes []VolumeMeta
	for _, volume := range volumes {
		jsonVolumes = append(jsonVolumes, toJSONVolume(volume))
	}
	c.Encode(jsonVolumes)
}

func (a *api) handlePOSTVolume(c jape.Context) {
	var req AddVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(fmt.Errorf("failed to parse add volume request: %w", err), http.StatusBadRequest)
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

func (a *api) handleDeleteVolume(c jape.Context) {
	var id int
	var force bool
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(fmt.Errorf("failed to parse volume id: %w", err), http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	} else if err := c.DecodeForm("force", &force); err != nil {
		c.Error(fmt.Errorf("failed to parse force: %w", err), http.StatusBadRequest)
		return
	}
	err := a.volumes.RemoveVolume(id, force)
	a.checkServerError(c, "failed to remove volume", err)
}

func (a *api) handlePUTVolumeResize(c jape.Context) {
	var id int
	if err := c.DecodeParam("id", &id); err != nil {
		c.Error(fmt.Errorf("failed to parse volume id: %w", err), http.StatusBadRequest)
		return
	} else if id < 0 {
		c.Error(errors.New("invalid volume id"), http.StatusBadRequest)
		return
	}

	var req ResizeVolumeRequest
	if err := c.Decode(&req); err != nil {
		c.Error(fmt.Errorf("failed to parse resize volume request: %w", err), http.StatusBadRequest)
		return
	}

	err := a.volumes.ResizeVolume(id, req.MaxSectors)
	a.checkServerError(c, "failed to resize volume", err)
}

func (a *api) handleGETWallet(c jape.Context) {
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

func (a *api) handleGETWalletTransactions(c jape.Context) {
	limit, offset := parseLimitParams(c, 100, 500)

	transactions, err := a.wallet.Transactions(limit, offset)
	if !a.checkServerError(c, "failed to get wallet transactions", err) {
		return
	}
	c.Encode(transactions)
}

func (a *api) handleGETWalletPending(c jape.Context) {
	pending, err := a.wallet.UnconfirmedTransactions()
	if !a.checkServerError(c, "failed to get wallet pending", err) {
		return
	}
	c.Encode(pending)
}

func (a *api) handlePOSTWalletSend(c jape.Context) {
	var req WalletSendSiacoinsRequest
	if err := c.Decode(&req); err != nil {
		c.Error(fmt.Errorf("failed to parse send siacoins request: %w", err), http.StatusBadRequest)
		return
	} else if req.Address == types.VoidAddress {
		c.Error(errors.New("cannot send to void address"), http.StatusBadRequest)
		return
	}

	// estimate miner fee
	feePerByte := a.tpool.RecommendedFee()
	minerFee := feePerByte.Mul64(stdTxnSize)
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
	if err := c.DecodeParam("path", &path); err != nil {
		c.Error(fmt.Errorf("failed to parse path: %w", err), http.StatusBadRequest)
	} else if path == "/~" { // handle user home directory.
		path, _ = os.UserHomeDir()
	} else if len(path) == 0 { // handle empty path for root directory.
		path = "/"
	}
	path, _ = filepath.Abs(path)
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

func (a *api) handlePOSTLogEntries(c jape.Context) {
	var filter logging.Filter
	if err := c.Decode(&filter); err != nil {
		c.Error(fmt.Errorf("failed to parse log filter: %w", err), http.StatusBadRequest)
		return
	}

	entries, err := a.logs.LogEntries(filter)
	if !a.checkServerError(c, "failed to get log entries", err) {
		return
	}
	c.Encode(entries)
}

func (a *api) handleDELETELogPrune(c jape.Context) {
	var timestamp time.Time
	if err := c.DecodeForm("before", &timestamp); err != nil {
		c.Error(fmt.Errorf("failed to parse log filter: %w", err), http.StatusBadRequest)
		return
	}
	err := a.logs.Prune(timestamp)
	a.checkServerError(c, "failed to prune logs", err)
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

func toJSONVolume(vol storage.VolumeMeta) VolumeMeta {
	jvm := VolumeMeta{
		VolumeMeta: vol,
	}
	for _, err := range vol.Errors {
		jvm.Errors = append(jvm.Errors, err)
	}
	return jvm
}
