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
	metricTotalSectors    = "totalSectors"
	metricPhysicalSectors = "physicalSectors"
	metricContractSectors = "contractSectors"
	metricTempSectors     = "tempSectors"
	metricRegistryEntries = "registryEntries"

	// bandwidth
	metricRHP2Ingress = "rhp2Ingress"
	metricRHP2Egress  = "rhp2Egress"
	metricRHP3Ingress = "rhp3Ingress"
	metricRHP3Egress  = "rhp3Egress"

	// pricing
	metricContractPrice     = "contractPrice"
	metricIngressPrice      = "ingressPrice"
	metricEgressPrice       = "egressPrice"
	metricBaseRPCPrice      = "baseRPCPrice"
	metricSectorAccessPrice = "sectorAccessPrice"
	metricStoragePrice      = "storagePrice"
	metricCollateral        = "collateral"

	// wallet
	metricWalletBalance = "walletBalance"

	statInterval = 5 * time.Minute
)

// PeriodMetrics returns aggregated metrics for the period between start and end.
func (s *Store) PeriodMetrics(start, end time.Time, interval metrics.Interval) (period []metrics.Metrics, err error) {
	if start.After(end) {
		return nil, errors.New("start time must be before end time")
	}

	current := start
	switch interval {
	case metrics.Interval15Minutes:
		current = current.Truncate(15 * time.Minute)
		end = end.Add(15 * time.Minute).Truncate(15 * time.Minute)
	case metrics.IntervalHourly:
		current = current.Truncate(time.Hour)
		end = end.Add(time.Hour).Truncate(time.Hour)
	case metrics.IntervalDaily:
		y, m, d := current.Date()
		current = time.Date(y, m, d, 0, 0, 0, 0, current.Location())
		// set end to the last second of the day
		y, m, d = end.Date()
		end = time.Date(y, m, d, 23, 59, 59, 999, current.Location())
	case metrics.IntervalWeekly:
		y, m, d := current.Date()
		d -= int(current.Weekday())
		current = time.Date(y, m, d, 0, 0, 0, 0, current.Location())
		// set end to the last second of the following Sunday
		y, m, d = end.Date()
		d += 7 - int(end.Weekday())
		end = time.Date(y, m, d, 23, 59, 59, 999, current.Location())
	case metrics.IntervalMonthly:
		y, m, _ := current.Date()
		current = time.Date(y, m, 1, 0, 0, 0, 0, current.Location())
		// set end to the last second of the last day of the month
		y, m, _ = end.Date()
		end = time.Date(y, m+1, 0, 23, 59, 59, 999, current.Location())
	case metrics.IntervalYearly:
		y, _, _ := current.Date()
		current = time.Date(y, 1, 1, 0, 0, 0, 0, current.Location())
		// set end to the last second of the last day of the year
		y, _, _ = end.Date()
		end = time.Date(y, 12, 31, 23, 59, 59, 999, current.Location())
	default:
		return nil, fmt.Errorf("invalid interval: %v", interval)
	}

	err = s.transaction(func(tx txn) error {
		// TODO: this would be more performant in a single query, then parsing
		// the results, but this is simple and performant enough short-term.
		for current.Before(end) {
			m, err := aggregateMetrics(tx, current)
			if err != nil {
				return fmt.Errorf("failed to get metrics: %w", err)
			}
			period = append(period, m)

			switch interval {
			case metrics.Interval15Minutes:
				current = current.Add(15 * time.Minute)
			case metrics.IntervalHourly:
				current = current.Add(time.Hour)
			case metrics.IntervalDaily:
				current = current.AddDate(0, 0, 1)
			case metrics.IntervalWeekly:
				current = current.AddDate(0, 0, 7)
			case metrics.IntervalMonthly:
				current = current.AddDate(0, 1, 0)
			case metrics.IntervalYearly:
				current = current.AddDate(1, 0, 0)
			}
		}
		return nil
	})
	return
}

// Metrics returns aggregate metrics for the host as of the timestamp.
func (s *Store) Metrics(timestamp time.Time) (m metrics.Metrics, err error) {
	return aggregateMetrics(&dbTxn{s}, timestamp)
}

// IncrementRHP2DataUsage increments the RHP2 ingress and egress metrics.
func (s *Store) IncrementRHP2DataUsage(ingress, egress uint64) error {
	return s.transaction(func(tx txn) error {
		if ingress > 0 {
			if err := incrementNumericStat(tx, metricRHP2Ingress, int(ingress), time.Now()); err != nil {
				return fmt.Errorf("failed to track ingress: %w", err)
			}
		}
		if egress > 0 {
			if err := incrementNumericStat(tx, metricRHP2Egress, int(egress), time.Now()); err != nil {
				return fmt.Errorf("failed to track egress: %w", err)
			}
		}
		return nil
	})
}

// IncrementRHP3DataUsage increments the RHP3 ingress and egress metrics.
func (s *Store) IncrementRHP3DataUsage(ingress, egress uint64) error {
	return s.transaction(func(tx txn) error {
		if ingress > 0 {
			if err := incrementNumericStat(tx, metricRHP3Ingress, int(ingress), time.Now()); err != nil {
				return fmt.Errorf("failed to track ingress: %w", err)
			}
		}
		if egress > 0 {
			if err := incrementNumericStat(tx, metricRHP3Egress, int(egress), time.Now()); err != nil {
				return fmt.Errorf("failed to track egress: %w", err)
			}
		}
		return nil
	})
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

func aggregateMetrics(tx txn, timestamp time.Time) (m metrics.Metrics, err error) {
	const query = `WITH summary AS (
SELECT 
stat, stat_value, 
ROW_NUMBER() OVER (PARTITION BY stat ORDER BY date_created DESC) AS rank 
FROM host_stats s
WHERE s.date_created<=$1)
SELECT stat, stat_value FROM summary WHERE rank=1;`
	rows, err := tx.Query(query, sqlTime(timestamp))
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
		case metricContractPrice:
			m.Pricing.ContractPrice = mustScanCurrency(value)
		case metricIngressPrice:
			m.Pricing.IngressPrice = mustScanCurrency(value)
		case metricEgressPrice:
			m.Pricing.EgressPrice = mustScanCurrency(value)
		case metricBaseRPCPrice:
			m.Pricing.BaseRPCPrice = mustScanCurrency(value)
		case metricSectorAccessPrice:
			m.Pricing.SectorAccessPrice = mustScanCurrency(value)
		case metricStoragePrice:
			m.Pricing.StoragePrice = mustScanCurrency(value)
		case metricCollateral:
			m.Pricing.Collateral = mustScanCurrency(value)
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
		case metricTotalSectors:
			m.Storage.TotalSectors = mustScanUint64(value)
		case metricPhysicalSectors:
			m.Storage.PhysicalSectors = mustScanUint64(value)
		case metricContractSectors:
			m.Storage.ContractSectors = mustScanUint64(value)
		case metricTempSectors:
			m.Storage.TempSectors = mustScanUint64(value)
		case metricRegistryEntries:
			m.Storage.RegistryEntries = mustScanUint64(value)
		case metricRHP2Ingress:
			m.Data.RHP2.Ingress = mustScanUint64(value)
		case metricRHP2Egress:
			m.Data.RHP2.Egress = mustScanUint64(value)
		case metricRHP3Ingress:
			m.Data.RHP3.Ingress = mustScanUint64(value)
		case metricRHP3Egress:
			m.Data.RHP3.Egress = mustScanUint64(value)
		case metricWalletBalance:
			m.Balance = mustScanCurrency(value)
		}
	}
	m.Timestamp = timestamp
	return
}

// incrementNumericStat tracks a numeric stat, incrementing the current value by
// delta. If the resulting value is negative, the function panics.
func incrementNumericStat(tx txn, stat string, delta int, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
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

// incrementCurrencyStat tracks a currency stat. If negative is false, the current
// value is incremented by delta. Otherwise, the value is decremented. If the
// resulting value would be negative, the function panics.
func incrementCurrencyStat(tx txn, stat string, delta types.Currency, negative bool, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	var current types.Currency
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, sqlTime(timestamp)).Scan((*sqlCurrency)(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	}
	value := current
	if negative {
		value = value.Sub(delta)
	} else {
		value = value.Add(delta)
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, sqlCurrency(value), sqlTime(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

func setCurrencyStat(tx txn, stat string, value types.Currency, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	var current types.Currency
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, sqlTime(timestamp)).Scan((*sqlCurrency)(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	} else if value.Equals(current) {
		return nil
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, sqlCurrency(value), sqlTime(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

// reflowCurrencyStat updates all currency stats after the given timestamp. If
// negative is false, the current value is incremented by delta. Otherwise, the
// value is decremented. If the resulting value would be negative, the function
// panics.
func reflowCurrencyStat(tx txn, stat string, startTime time.Time, value types.Currency, negative bool) error {
	startTime = startTime.Truncate(statInterval)
	rows, err := tx.Query(`SELECT stat_value, date_created FROM host_stats WHERE stat=$1 AND date_created > $2 ORDER BY date_created ASC`, stat, sqlTime(startTime))
	if err != nil {
		return fmt.Errorf("failed to query existing value: %w", err)
	}
	defer rows.Close()
	var values []types.Currency
	var timestamps []time.Time
	for rows.Next() {
		var v types.Currency
		var timestamp time.Time
		if err := rows.Scan((*sqlCurrency)(&v), (*sqlTime)(&timestamp)); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if negative {
			v = v.Sub(value)
		} else {
			v = v.Add(value)
		}
		values = append(values, v)
		timestamps = append(timestamps, timestamp)
	}

	stmt, err := tx.Prepare(`UPDATE host_stats SET stat_value=$1 WHERE stat=$2 AND date_created=$3 RETURNING date_created`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement: %w", err)
	}
	defer stmt.Close()
	for i := range values {
		var dbTime time.Time
		err := stmt.QueryRow(sqlCurrency(values[i]), stat, sqlTime(timestamps[i])).Scan((*sqlTime)(&dbTime))
		if err != nil {
			return fmt.Errorf("failed to update stat: %w", err)
		}
	}
	return nil
}
