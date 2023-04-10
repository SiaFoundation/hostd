package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/metrics"
)

const (
	// contracts
	metricPendingContracts    = "pendingContracts"
	metricActiveContracts     = "activeContracts"
	metricRejectedContracts   = "rejectedContracts"
	metricSuccessfulContracts = "successfulContracts"
	metricFailedContracts     = "failedContracts"

	// storage
	metricPhysicalSectors = "physicalSectors"
	metricContractSectors = "contractSectors"
	metricTempSectors     = "tempSectors"

	// bandwidth
	metricRHP2Ingress = "rhp2Ingress"
	metricRHP2Egress  = "rhp2Egress"
	metricRHP3Ingress = "rhp3Ingress"
	metricRHP3Egress  = "rhp3Egress"

	statInterval = 5 * time.Minute
)

// CurrentMetrics returns the current metrics for the host.
func (s *Store) CurrentMetrics() (m metrics.Metrics, err error) {
	const query = `WITH summary AS (
SELECT 
	stat, stat_value, 
	ROW_NUMBER() OVER (PARTITION BY stat ORDER BY date_created DESC) AS rank 
FROM host_stats s)
SELECT stat, stat_value FROM summary WHERE rank=1;
`
	rows, err := s.query(query)
	if err != nil {
		return metrics.Metrics{}, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var stat string
		var value []byte

		if err := rows.Scan(&stat, &value); err != nil {
			return metrics.Metrics{}, fmt.Errorf("failed to scan row: %w", err)
		}

		switch stat {
		case metricPendingContracts:
			m.Contracts.Pending = mustScanUint64(value)
		case metricActiveContracts:
			m.Contracts.Active = mustScanUint64(value)
		case metricRejectedContracts:
			m.Contracts.Rejected = mustScanUint64(value)
		case metricSuccessfulContracts:
			m.Contracts.Successful = mustScanUint64(value)
		case metricFailedContracts:
			m.Contracts.Failed = mustScanUint64(value)
		case metricPhysicalSectors:
			m.Storage.PhysicalSectors = mustScanUint64(value)
		case metricContractSectors:
			m.Storage.ContractSectors = mustScanUint64(value)
		case metricTempSectors:
			m.Storage.TempSectors = mustScanUint64(value)
		}
	}
	return
}

func mustScanCurrency(b []byte) types.Currency {
	var c sqlCurrency
	if err := c.Scan(b); err != nil {
		panic(err)
	}
	return types.Currency(c)
}

func mustScanUint64(b []byte) uint64 {
	var u sqlUint64
	if err := u.Scan(b); err != nil {
		panic(err)
	}
	return uint64(u)
}

// trackNumericStat tracks a numeric stat, incrementing the current value by
// delta. If the resulting value is negative, the function panics.
func trackNumericStat(tx txn, stat string, delta int) error {
	timestamp := time.Now().Truncate(statInterval)
	var current uint64
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, sqlTime(timestamp)).Scan((*sqlUint64)(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	}
	var value uint64
	if delta < 0 {
		if current < uint64(-delta) {
			panic(fmt.Errorf("negative stat value: %v %v%v", stat, current, delta))
		}
		value = current - uint64(-delta)
	} else {
		value = current + uint64(delta)
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, sqlUint64(value), sqlTime(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

// trackCurrencyStat tracks a currency stat. If negative is false, the current
// value is incremented by delta. Otherwise, the value is decremented. If the
// resulting value would be negative, the function panics.
func trackCurrencyStat(tx txn, stat string, delta types.Currency, negative bool) error {
	timestamp := time.Now().Truncate(statInterval)
	var current types.Currency
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, metricContractSectors, sqlTime(timestamp)).Scan((*sqlCurrency)(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	}
	value := current
	if negative {
		value = value.Sub(delta)
	} else {
		value = value.Add(delta)
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, metricContractSectors, sqlCurrency(value), sqlTime(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}
