package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	"go.sia.tech/jape"
)

type (
	// IntegrityCheckResult tracks the result of an integrity check.
	IntegrityCheckResult struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`

		CheckedSectors uint64                      `json:"checkedSectors"`
		TotalSectors   uint64                      `json:"totalSectors"`
		BadSectors     []contracts.IntegrityResult `json:"badSectors"`
	}

	integrityCheckJob struct {
		cancelFn context.CancelFunc
		IntegrityCheckResult
	}

	// integrityChecks tracks the result of all integrity checks.
	integrityCheckJobs struct {
		contracts ContractManager

		mu     sync.Mutex // protects checks
		checks map[types.FileContractID]integrityCheckJob
	}
)

// Cancel cancels the integrity check for the specified contract. If no check is
// running, false is returned.
func (ic *integrityCheckJobs) Cancel(contractID types.FileContractID) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	job, exists := ic.checks[contractID]
	if !exists {
		return false
	}
	job.cancelFn()
	delete(ic.checks, contractID)
	return true
}

// Results returns the result of the integrity check for the specified contract.
func (ic *integrityCheckJobs) Results(contractID types.FileContractID) (IntegrityCheckResult, bool) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	check, exists := ic.checks[contractID]
	return check.IntegrityCheckResult, exists
}

// CheckContract starts an integrity check for the specified contract. If a
// check is already running, an error is returned.
func (ic *integrityCheckJobs) CheckContract(contractID types.FileContractID) (uint64, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	job, exists := ic.checks[contractID]
	if exists && job.End.IsZero() { // if a check is still running, return an error
		return 0, fmt.Errorf("integrity check already running for contract %v", contractID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	results, roots, err := ic.contracts.CheckIntegrity(ctx, contractID)
	if err != nil {
		cancel()
		return 0, fmt.Errorf("failed to check contract integrity: %w", err)
	}
	ic.checks[contractID] = integrityCheckJob{
		cancelFn: cancel,
		IntegrityCheckResult: IntegrityCheckResult{
			Start:        time.Now(),
			TotalSectors: roots,
		},
	}

	go func() {
		defer cancel()

		for result := range results {
			ic.mu.Lock()
			check := ic.checks[contractID]
			check.CheckedSectors++
			if result.Error != nil {
				check.BadSectors = append(check.BadSectors, result)
			}
			ic.checks[contractID] = check
			ic.mu.Unlock()
		}
		ic.mu.Lock()
		check := ic.checks[contractID]
		check.End = time.Now()
		ic.checks[contractID] = check
		ic.mu.Unlock()
	}()
	return roots, nil
}

func (a *api) handleGETContractCheck(c jape.Context) {
	var contractID types.FileContractID
	if err := c.DecodeParam("id", &contractID); err != nil {
		return
	}

	result, ok := a.checks.Results(contractID)
	if !ok {
		c.Error(fmt.Errorf("no integrity check found for contract %v", contractID), http.StatusNotFound)
		return
	}
	c.Encode(result)
}

func (a *api) handleDeleteContractCheck(c jape.Context) {
	var contractID types.FileContractID
	if err := c.DecodeParam("id", &contractID); err != nil {
		return
	}

	if !a.checks.Cancel(contractID) {
		c.Error(fmt.Errorf("no integrity check found for contract %q", contractID), http.StatusNotFound)
	}
}

func (a *api) handlePUTContractCheck(c jape.Context) {
	var contractID types.FileContractID
	if err := c.DecodeParam("id", &contractID); err != nil {
		return
	}

	_, err := a.checks.CheckContract(contractID)
	a.checkServerError(c, "failed to check contract integrity", err)
}
