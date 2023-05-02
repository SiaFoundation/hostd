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
	metricSectorReads     = "sectorReads"
	metricSectorWrites    = "sectorWrites"

	// registry
	metricMaxRegistryEntries = "maxRegistryEntries"
	metricRegistryEntries    = "registryEntries"
	metricRegistryReads      = "registryReads"
	metricRegistryWrites     = "registryWrites"

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

// PeriodMetrics returns aggregate metrics for n periods starting at start
func (s *Store) PeriodMetrics(start time.Time, n int, interval metrics.Interval) ([]metrics.Metrics, error) {
	if n <= 0 {
		return nil, errors.New("n periods must be greater than 0")
	}

	var end time.Time
	switch interval {
	case metrics.Interval15Minutes:
		start = start.Truncate(15 * time.Minute)
		end = start.Add(15 * time.Minute * time.Duration(n))
	case metrics.IntervalHourly:
		start = start.Truncate(time.Hour)
		end = start.Add(time.Hour * time.Duration(n))
	case metrics.IntervalDaily:
		y, m, d := start.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, start.Location())
		end = start.AddDate(0, 0, n)
	case metrics.IntervalWeekly:
		y, m, d := start.Date()
		d -= int(start.Weekday())
		// set start to the first day of the week
		start = time.Date(y, m, d, 0, 0, 0, 0, start.Location())
		end = start.AddDate(0, 0, 7*n) // add n weeks
	case metrics.IntervalMonthly:
		y, m, _ := start.Date()
		// set start to the first day of the month
		start = time.Date(y, m, 1, 0, 0, 0, 0, start.Location())
		end = start.AddDate(0, n, 0) // add n months
	case metrics.IntervalYearly:
		start = time.Date(start.Year(), 1, 1, 0, 0, 0, 0, start.Location())
		end = start.AddDate(n, 0, 0) // add n years
	default:
		return nil, fmt.Errorf("invalid interval: %v", interval)
	}

	// get metrics as of the start time to backfill any missing periods
	initial, err := s.Metrics(start)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial metrics: %w", err)
	}

	const query = `SELECT stat, stat_value, date_created FROM host_stats WHERE date_created BETWEEN $1 AND $2 ORDER BY date_created ASC`
	rows, err := s.db.Query(query, sqlTime(start), sqlTime(end))
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	stats := []metrics.Metrics{
		// add the initial metric so that the first period is not empty
		initial,
	}
	for rows.Next() {
		var stat string
		var value []byte
		var timestamp time.Time

		if err := rows.Scan(&stat, &value, (*sqlTime)(&timestamp)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// normalize the stored timestamp to the locale and interval
		timestamp = timestamp.In(start.Location())
		switch interval {
		case metrics.Interval15Minutes:
			timestamp = timestamp.Truncate(15 * time.Minute)
		case metrics.IntervalHourly:
			timestamp = timestamp.Truncate(time.Hour)
		case metrics.IntervalDaily:
			y, m, d := timestamp.Date()
			timestamp = time.Date(y, m, d, 0, 0, 0, 0, timestamp.Location())
		case metrics.IntervalWeekly:
			y, m, d := timestamp.Date()
			d -= int(timestamp.Weekday())
			timestamp = time.Date(y, m, d, 0, 0, 0, 0, timestamp.Location())
		case metrics.IntervalMonthly:
			y, m, _ := timestamp.Date()
			timestamp = time.Date(y, m, 1, 0, 0, 0, 0, timestamp.Location())
		case metrics.IntervalYearly:
			timestamp = time.Date(timestamp.Year(), 1, 1, 0, 0, 0, 0, timestamp.Location())
		}

		// if the timestamp is not the same as the last period, add a new period
		if stats[len(stats)-1].Timestamp != timestamp {
			m := stats[len(stats)-1]
			m.Timestamp = timestamp
			stats = append(stats, m)
		}
		// overwrite the metric value for the current period
		mustParseMetricValue(stat, value, &stats[len(stats)-1])
	}

	// fill in any missing periods
	periods := []metrics.Metrics{}
	current := start
	for i := 0; i < n; i++ {
		// stats will always be non-empty because of the initial values
		if stats[0].Timestamp.Equal(current) {
			periods = append(periods, stats[0])
			stats = stats[1:]
		} else {
			// if there is not a metric for the current period, copy previous
			// period and overwrite the timestamp
			periods = append(periods, periods[len(periods)-1])
			periods[len(periods)-1].Timestamp = current
		}

		// increment the current time by the interval
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
	return periods, nil
}

// Metrics returns aggregate metrics for the host as of the timestamp.
func (s *Store) Metrics(timestamp time.Time) (m metrics.Metrics, err error) {
	const query = `WITH summary AS (
		SELECT 
		stat, stat_value, 
		ROW_NUMBER() OVER (PARTITION BY stat ORDER BY date_created DESC) AS rank 
		FROM host_stats s
		WHERE s.date_created<=$1)
		SELECT stat, stat_value FROM summary WHERE rank=1;`
	rows, err := s.query(query, sqlTime(timestamp))
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
		mustParseMetricValue(stat, value, &m)
	}
	m.Timestamp = timestamp
	return
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

// IncrementSectorAccess increments the sector read and write metrics.
func (s *Store) IncrementSectorAccess(reads, writes uint64) error {
	return s.transaction(func(tx txn) error {
		if reads > 0 {
			if err := incrementNumericStat(tx, metricSectorReads, int(reads), time.Now()); err != nil {
				return fmt.Errorf("failed to track reads: %w", err)
			}
		}
		if writes > 0 {
			if err := incrementNumericStat(tx, metricSectorWrites, int(writes), time.Now()); err != nil {
				return fmt.Errorf("failed to track writes: %w", err)
			}
		}
		return nil
	})
}

// IncrementRegistryAccess increments the registry read and write metrics.
func (s *Store) IncrementRegistryAccess(read, write uint64) error {
	return s.transaction(func(tx txn) error {
		if read > 0 {
			if err := incrementNumericStat(tx, metricRegistryReads, int(read), time.Now()); err != nil {
				return fmt.Errorf("failed to track reads: %w", err)
			}
		}
		if write > 0 {
			if err := incrementNumericStat(tx, metricRegistryWrites, int(write), time.Now()); err != nil {
				return fmt.Errorf("failed to track writes: %w", err)
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

// mustParseMetricValue parses the value of a metric from the database.
// If the metric fails to parse, it will panic.
func mustParseMetricValue(stat string, buf []byte, m *metrics.Metrics) {
	switch stat {
	// pricing
	case metricContractPrice:
		m.Pricing.ContractPrice = mustScanCurrency(buf)
	case metricIngressPrice:
		m.Pricing.IngressPrice = mustScanCurrency(buf)
	case metricEgressPrice:
		m.Pricing.EgressPrice = mustScanCurrency(buf)
	case metricBaseRPCPrice:
		m.Pricing.BaseRPCPrice = mustScanCurrency(buf)
	case metricSectorAccessPrice:
		m.Pricing.SectorAccessPrice = mustScanCurrency(buf)
	case metricStoragePrice:
		m.Pricing.StoragePrice = mustScanCurrency(buf)
	case metricCollateral:
		m.Pricing.Collateral = mustScanCurrency(buf)
	// contracts
	case metricPendingContracts:
		m.Contracts.Pending = mustScanUint64(buf)
	case metricActiveContracts:
		m.Contracts.Active = mustScanUint64(buf)
	case metricRejectedContracts:
		m.Contracts.Rejected = mustScanUint64(buf)
	case metricSuccessfulContracts:
		m.Contracts.Successful = mustScanUint64(buf)
	case metricFailedContracts:
		m.Contracts.Failed = mustScanUint64(buf)
	// storage
	case metricTotalSectors:
		m.Storage.TotalSectors = mustScanUint64(buf)
	case metricPhysicalSectors:
		m.Storage.PhysicalSectors = mustScanUint64(buf)
	case metricContractSectors:
		m.Storage.ContractSectors = mustScanUint64(buf)
	case metricTempSectors:
		m.Storage.TempSectors = mustScanUint64(buf)
	case metricSectorReads:
		m.Storage.Reads = mustScanUint64(buf)
	case metricSectorWrites:
		m.Storage.Writes = mustScanUint64(buf)
	// registry
	case metricRegistryEntries:
		m.Registry.Entries = mustScanUint64(buf)
	case metricMaxRegistryEntries:
		m.Registry.MaxEntries = mustScanUint64(buf)
	case metricRegistryReads:
		m.Registry.Reads = mustScanUint64(buf)
	case metricRegistryWrites:
		m.Registry.Writes = mustScanUint64(buf)
	// bandwidth
	case metricRHP2Ingress:
		m.Data.RHP2.Ingress = mustScanUint64(buf)
	case metricRHP2Egress:
		m.Data.RHP2.Egress = mustScanUint64(buf)
	case metricRHP3Ingress:
		m.Data.RHP3.Ingress = mustScanUint64(buf)
	case metricRHP3Egress:
		m.Data.RHP3.Egress = mustScanUint64(buf)
	// wallet
	case metricWalletBalance:
		m.Balance = mustScanCurrency(buf)
	default:
		panic(fmt.Sprintf("unknown metric: %v", stat))
	}
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

func setNumericStat(tx txn, stat string, value uint64, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	var current uint64
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, sqlTime(timestamp)).Scan((*sqlUint64)(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	} else if value == current {
		return nil
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, sqlUint64(value), sqlTime(timestamp))
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
