package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/contracts"
	"go.sia.tech/jape"
)

type (
	// IntegrityCheckResult tracks the result of an integrity check.
	IntegrityCheckResult struct {
		// Start is the time the integrity check started.
		Start time.Time `json:"start"`
		// End is the time the integrity check completed. Zero if the
		// check is still running.
		End time.Time `json:"end"`

		// CheckedSectors is the number of sectors that have been
		// checked so far.
		CheckedSectors uint64 `json:"checkedSectors"`
		// TotalSectors is the total number of sectors to check in the
		// contract.
		TotalSectors uint64 `json:"totalSectors"`
		// BadSectors is a list of sectors that failed the integrity
		// check.
		BadSectors []contracts.IntegrityResult `json:"badSectors"`
	}

	// integrityChecks tracks the result of all integrity checks.
	integrityCheckJobs struct {
		contracts ContractManager

		mu     sync.Mutex // protects checks
		checks map[types.FileContractID]IntegrityCheckResult
	}
)

// ClearResult clears the result of the integrity check for the specified contract.
func (ic *integrityCheckJobs) ClearResult(contractID types.FileContractID) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	_, exists := ic.checks[contractID]
	if !exists {
		return false
	}
	delete(ic.checks, contractID)
	return true
}

// Results returns the result of the integrity check for the specified contract.
func (ic *integrityCheckJobs) Results(contractID types.FileContractID) (IntegrityCheckResult, bool) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	check, exists := ic.checks[contractID]
	return check, exists
}

// CheckContract starts an integrity check for the specified contract. If a
// check is already running, an error is returned.
func (ic *integrityCheckJobs) CheckContract(contractID types.FileContractID) (uint64, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	check, exists := ic.checks[contractID]
	if exists && check.End.IsZero() { // if a check is still running, return an error
		return 0, fmt.Errorf("integrity check already running for contract %v", contractID)
	}

	results, roots, err := ic.contracts.CheckIntegrity(context.Background(), contractID)
	if err != nil {
		return 0, fmt.Errorf("failed to check contract integrity: %w", err)
	}

	check = IntegrityCheckResult{
		Start:        time.Now(),
		TotalSectors: roots,
	}
	ic.checks[contractID] = check

	go func() {
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

// handleGETContractCheck handles the [GET] /contracts/:id/integrity endpoint.
// It returns the current or most recent integrity check result for the
// specified contract.
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

// handleDeleteContractCheck handles the [DELETE] /contracts/:id/integrity
// endpoint. It clears the stored integrity check result for the specified
// contract.
func (a *api) handleDeleteContractCheck(c jape.Context) {
	var contractID types.FileContractID
	if err := c.DecodeParam("id", &contractID); err != nil {
		return
	}

	if !a.checks.ClearResult(contractID) {
		c.Error(fmt.Errorf("no integrity check found for contract %v", contractID), http.StatusNotFound)
	}
}

// handlePUTContractCheck handles the [PUT] /contracts/:id/integrity endpoint.
// It starts a background integrity check that reads and verifies every sector
// in the specified contract.
func (a *api) handlePUTContractCheck(c jape.Context) {
	var contractID types.FileContractID
	if err := c.DecodeParam("id", &contractID); err != nil {
		return
	}

	_, err := a.checks.CheckContract(contractID)
	a.checkServerError(c, "failed to check contract integrity", err)
}
