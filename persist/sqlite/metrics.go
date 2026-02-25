package sqlite

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/v2/host/metrics"
	"go.sia.tech/hostd/v2/host/storage"
)

const (
	// contracts
	metricActiveContracts     = "activeContracts"
	metricRejectedContracts   = "rejectedContracts"
	metricSuccessfulContracts = "successfulContracts"
	metricFailedContracts     = "failedContracts"

	// v2
	metricRenewedContracts = "renewedContracts"

	metricLockedCollateral = "lockedCollateral"
	metricRiskedCollateral = "riskedCollateral"

	// accounts
	metricActiveAccounts = "activeAccounts"
	metricAccountBalance = "accountBalance"

	// storage
	metricTotalSectors     = "totalSectors"
	metricPhysicalSectors  = "physicalSectors"
	metricLostSectors      = "lostSectors"
	metricContractSectors  = "contractSectors"
	metricTempSectors      = "tempSectors"
	metricSectorReads      = "sectorReads"
	metricSectorWrites     = "sectorWrites"
	metricSectorReadBytes  = "sectorReadBytes"
	metricSectorWriteBytes = "sectorWriteBytes"
	metricSectorCacheHit   = "sectorCacheHit"
	metricSectorCacheMiss  = "sectorCacheMiss"

	// registry
	metricMaxRegistryEntries = "maxRegistryEntries"
	metricRegistryEntries    = "registryEntries"
	metricRegistryReads      = "registryReads"
	metricRegistryWrites     = "registryWrites"

	// bandwidth
	metricDataRHPIngress    = "dataIngress"
	metricDataRHPEgress     = "dataEgress"
	metricDataSyncerIngress = "syncerIngress"
	metricDataSyncerEgress  = "syncerEgress"

	// metricRHP2Ingress
	//
	// Deprecated: combined into metricDataRHPIngress
	metricRHP2Ingress = "rhp2Ingress"
	// metricRHP2Egress
	//
	// Deprecated: combined into metricDataRHPEgress
	metricRHP2Egress = "rhp2Egress"
	// metricRHP3Ingress
	//
	// Deprecated: combined into metricDataRHPIngress
	metricRHP3Ingress = "rhp3Ingress"
	// metricRHP3Egress
	//
	// Deprecated: combined into metricDataRHPEgress
	metricRHP3Egress = "rhp3Egress"

	// pricing
	metricContractPrice        = "contractPrice"
	metricIngressPrice         = "ingressPrice"
	metricEgressPrice          = "egressPrice"
	metricBaseRPCPrice         = "baseRPCPrice"
	metricSectorAccessPrice    = "sectorAccessPrice"
	metricStoragePrice         = "storagePrice"
	metricCollateralMultiplier = "collateralMultiplier"

	// wallet
	metricWalletBalance         = "walletBalance"
	metricWalletImmatureBalance = "walletImmatureBalance"

	// potential revenue
	metricPotentialRPCRevenue           = "potentialRPCRevenue"
	metricPotentialStorageRevenue       = "potentialStorageRevenue"
	metricPotentialIngressRevenue       = "potentialIngressRevenue"
	metricPotentialEgressRevenue        = "potentialEgressRevenue"
	metricPotentialRegistryReadRevenue  = "potentialRegistryReadRevenue"
	metricPotentialRegistryWriteRevenue = "potentialRegistryWriteRevenue"

	// earned revenue
	metricEarnedRPCRevenue           = "earnedRPCRevenue"
	metricEarnedStorageRevenue       = "earnedStorageRevenue"
	metricEarnedIngressRevenue       = "earnedIngressRevenue"
	metricEarnedEgressRevenue        = "earnedEgressRevenue"
	metricEarnedRegistryReadRevenue  = "earnedRegistryReadRevenue"
	metricEarnedRegistryWriteRevenue = "earnedRegistryWriteRevenue"

	statInterval = 5 * time.Minute
)

// PeriodMetrics returns aggregate metrics for n periods starting at start
func (s *Store) PeriodMetrics(start time.Time, n int, interval metrics.Interval) ([]metrics.Metrics, error) {
	if n <= 0 {
		return nil, errors.New("n periods must be greater than 0")
	}

	var end time.Time
	switch interval {
	case metrics.Interval5Minutes:
		end = start.Add(5 * time.Minute * time.Duration(n))
	case metrics.Interval15Minutes:
		end = start.Add(15 * time.Minute * time.Duration(n))
	case metrics.IntervalHourly:
		end = start.Add(time.Hour * time.Duration(n))
	case metrics.IntervalDaily:
		end = start.AddDate(0, 0, n)
	case metrics.IntervalWeekly:
		end = start.AddDate(0, 0, 7*n) // add n weeks
	case metrics.IntervalMonthly:
		end = start.AddDate(0, n, 0) // add n months
	case metrics.IntervalYearly:
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
	rows, err := s.db.Query(query, encode(start), encode(end))
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

		if err := rows.Scan(&stat, &value, decode(&timestamp)); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// normalize the stored timestamp to the locale and interval
		timestamp = timestamp.In(start.Location())
		switch interval {
		case metrics.Interval5Minutes:
			timestamp = timestamp.Truncate(5 * time.Minute)
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
	for range n {
		if len(stats) != 0 && stats[0].Timestamp.Equal(current) {
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
		case metrics.Interval5Minutes:
			current = current.Add(5 * time.Minute)
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
	err = s.transaction(func(tx *txn) error {
		const query = `SELECT s.stat, s.stat_value
FROM host_stats s
JOIN (
    SELECT stat, MAX(date_created) AS most_recent
    FROM host_stats
    WHERE date_created <= $1
    GROUP BY stat
) AS sub ON s.stat = sub.stat AND s.date_created = sub.most_recent;`
		rows, err := tx.Query(query, encode(timestamp))
		if err != nil {
			return fmt.Errorf("failed to query metrics: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var stat string
			var value []byte

			if err := rows.Scan(&stat, &value); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			mustParseMetricValue(stat, value, &m)
		}
		m.Timestamp = timestamp
		return nil
	})

	return
}

// IncrementRHPDataUsage increments the RHP ingress and egress metrics.
func (s *Store) IncrementRHPDataUsage(ingress, egress uint64) error {
	return s.incrementDataUsage(metricDataRHPIngress, metricDataRHPEgress, ingress, egress)
}

// IncrementSyncerDataUsage increments the syncer ingress and egress metrics.
func (s *Store) IncrementSyncerDataUsage(ingress, egress uint64) error {
	return s.incrementDataUsage(metricDataSyncerIngress, metricDataSyncerEgress, ingress, egress)
}

func (s *Store) incrementDataUsage(ingressStat, egressStat string, ingress, egress uint64) error {
	return s.transaction(func(tx *txn) error {
		if ingress > 0 {
			if err := incrementNumericStat(tx, ingressStat, int(ingress), time.Now()); err != nil {
				return fmt.Errorf("failed to track ingress stat %q: %w", ingressStat, err)
			}
		}
		if egress > 0 {
			if err := incrementNumericStat(tx, egressStat, int(egress), time.Now()); err != nil {
				return fmt.Errorf("failed to track egress stat %q: %w", egressStat, err)
			}
		}
		return nil
	})
}

// IncrementSectorMetrics increments sector access metrics.
func (s *Store) IncrementSectorMetrics(metrics storage.SectorMetrics) error {
	return s.transaction(func(tx *txn) error {
		incrementStmt, done, err := incrementNumericStatStmt(tx)
		if err != nil {
			return fmt.Errorf("failed to prepare increment stmt: %w", err)
		}
		defer done()

		increment := func(stat string, delta int) error {
			if delta == 0 {
				return nil
			}

			return incrementStmt(stat, int64(delta), time.Now())
		}

		if err := increment(metricSectorReads, int(metrics.ReadCount)); err != nil {
			return fmt.Errorf("failed to track reads: %w", err)
		} else if err := increment(metricSectorWrites, int(metrics.WriteCount)); err != nil {
			return fmt.Errorf("failed to track writes: %w", err)
		} else if err := increment(metricSectorCacheHit, int(metrics.CacheHit)); err != nil {
			return fmt.Errorf("failed to track cache hits: %w", err)
		} else if err := increment(metricSectorCacheMiss, int(metrics.CacheMiss)); err != nil {
			return fmt.Errorf("failed to track cache misses: %w", err)
		} else if err := increment(metricSectorReadBytes, int(metrics.ReadBytes)); err != nil {
			return fmt.Errorf("failed to track read bytes: %w", err)
		} else if err := increment(metricSectorWriteBytes, int(metrics.WriteBytes)); err != nil {
			return fmt.Errorf("failed to track write bytes: %w", err)
		}
		return nil
	})
}

// IncrementRegistryAccess increments the registry read and write metrics.
func (s *Store) IncrementRegistryAccess(read, write uint64) error {
	return s.transaction(func(tx *txn) error {
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

func mustScanCurrency(src []byte) (c types.Currency) {
	if len(src) != 16 {
		panic(fmt.Sprintf("cannot scan %d bytes into Currency", len(src)))
	}
	c.Lo = binary.LittleEndian.Uint64(src[:8])
	c.Hi = binary.LittleEndian.Uint64(src[8:])
	return
}

func mustScanUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
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
	case metricCollateralMultiplier:
		value := mustScanUint64(buf)
		m.Pricing.CollateralMultiplier = math.Float64frombits(value)
	// contracts
	case metricActiveContracts:
		m.Contracts.Active = mustScanUint64(buf)
	case metricRejectedContracts:
		m.Contracts.Rejected = mustScanUint64(buf)
	case metricSuccessfulContracts:
		m.Contracts.Successful = mustScanUint64(buf)
	case metricFailedContracts:
		m.Contracts.Failed = mustScanUint64(buf)
	case metricRenewedContracts:
		m.Contracts.Renewed = mustScanUint64(buf)
	case metricLockedCollateral:
		m.Contracts.LockedCollateral = mustScanCurrency(buf)
	case metricRiskedCollateral:
		m.Contracts.RiskedCollateral = mustScanCurrency(buf)
	// accounts
	case metricActiveAccounts:
		m.Accounts.Active = mustScanUint64(buf)
	case metricAccountBalance:
		m.Accounts.Balance = mustScanCurrency(buf)
	// storage
	case metricTotalSectors:
		m.Storage.TotalSectors = mustScanUint64(buf)
	case metricPhysicalSectors:
		m.Storage.PhysicalSectors = mustScanUint64(buf)
	case metricLostSectors:
		m.Storage.LostSectors = mustScanUint64(buf)
	case metricContractSectors:
		m.Storage.ContractSectors = mustScanUint64(buf)
	case metricTempSectors:
		m.Storage.TempSectors = mustScanUint64(buf)
	case metricSectorReads:
		m.Storage.Reads = mustScanUint64(buf)
	case metricSectorWrites:
		m.Storage.Writes = mustScanUint64(buf)
	case metricSectorReadBytes:
		m.Storage.ReadBytes = mustScanUint64(buf)
	case metricSectorWriteBytes:
		m.Storage.WriteBytes = mustScanUint64(buf)
	case metricSectorCacheHit:
		m.Storage.SectorCacheHits = mustScanUint64(buf)
	case metricSectorCacheMiss:
		m.Storage.SectorCacheMisses = mustScanUint64(buf)
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
	case metricDataRHPIngress:
		m.Data.RHP.Ingress = mustScanUint64(buf)
	case metricDataRHPEgress:
		m.Data.RHP.Egress = mustScanUint64(buf)
	case metricDataSyncerEgress:
		m.Data.Syncer.Egress = mustScanUint64(buf)
	case metricDataSyncerIngress:
		m.Data.Syncer.Ingress = mustScanUint64(buf)
	// potential revenue
	case metricPotentialRPCRevenue:
		m.Revenue.Potential.RPC = mustScanCurrency(buf)
	case metricPotentialStorageRevenue:
		m.Revenue.Potential.Storage = mustScanCurrency(buf)
	case metricPotentialIngressRevenue:
		m.Revenue.Potential.Ingress = mustScanCurrency(buf)
	case metricPotentialEgressRevenue:
		m.Revenue.Potential.Egress = mustScanCurrency(buf)
	case metricPotentialRegistryReadRevenue:
		m.Revenue.Potential.RegistryRead = mustScanCurrency(buf)
	case metricPotentialRegistryWriteRevenue:
		m.Revenue.Potential.RegistryWrite = mustScanCurrency(buf)
	// earnedRevenue
	case metricEarnedRPCRevenue:
		m.Revenue.Earned.RPC = mustScanCurrency(buf)
	case metricEarnedStorageRevenue:
		m.Revenue.Earned.Storage = mustScanCurrency(buf)
	case metricEarnedIngressRevenue:
		m.Revenue.Earned.Ingress = mustScanCurrency(buf)
	case metricEarnedEgressRevenue:
		m.Revenue.Earned.Egress = mustScanCurrency(buf)
	case metricEarnedRegistryReadRevenue:
		m.Revenue.Earned.RegistryRead = mustScanCurrency(buf)
	case metricEarnedRegistryWriteRevenue:
		m.Revenue.Earned.RegistryWrite = mustScanCurrency(buf)
	// wallet
	case metricWalletBalance:
		m.Wallet.Balance = mustScanCurrency(buf)
	case metricWalletImmatureBalance:
		m.Wallet.ImmatureBalance = mustScanCurrency(buf)
	default:
		panic(fmt.Sprintf("unknown metric: %v", stat))
	}
}

// incrementNumericStatStmt tracks a numeric stat, incrementing the current value by
// delta. If the resulting value is negative, the function panics. This function
// should be used when lots of stats need to be batched together.
func incrementNumericStatStmt(tx *txn) (func(stat string, delta int64, timestamp time.Time) error, func() error, error) {
	getStatStmt, err := tx.Prepare(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare get stat statement: %w", err)
	}

	insertStatStmt, err := tx.Prepare(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare insert stat statement: %w", err)
	}

	return func(stat string, delta int64, timestamp time.Time) error {
			if delta == 0 {
				return nil
			}
			timestamp = timestamp.Truncate(statInterval)
			var current int64
			if err := getStatStmt.QueryRow(stat, encode(timestamp)).Scan(decode(&current)); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to query existing value: %w", err)
			}

			if current+delta < 0 {
				panic(fmt.Errorf("negative stat value: %v %v%v", stat, current, delta))
			}
			value := current + delta
			_, err = insertStatStmt.Exec(stat, encode(value), encode(timestamp))
			return err
		}, func() error {
			getStatStmt.Close()
			insertStatStmt.Close()
			return nil
		}, nil
}

// incrementCurrencyStatStmt increments a currency stat. If negative is false, the current
// value is incremented by delta. Otherwise, the value is decremented. If the
// resulting value would be negative, the function panics. This function should
// be used when lots of stats need to be batched together.
func incrementCurrencyStatStmt(tx *txn) (func(stat string, delta types.Currency, negative bool, timestamp time.Time) error, func() error, error) {
	getStatStmt, err := tx.Prepare(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare get stat statement: %w", err)
	}

	insertStatStmt, err := tx.Prepare(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare insert stat statement: %w", err)
	}

	return func(stat string, delta types.Currency, negative bool, timestamp time.Time) error {
			if delta.IsZero() {
				return nil
			}
			timestamp = timestamp.Truncate(statInterval)
			var current types.Currency
			if err := getStatStmt.QueryRow(stat, encode(timestamp)).Scan(decode(&current)); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("failed to query existing value: %w", err)
			}

			var value types.Currency
			if negative {
				if current.Cmp(delta) < 0 {
					panic(fmt.Errorf("negative stat value: %v %v-%v", stat, current, delta))
				}
				value = current.Sub(delta)
			} else {
				value = current.Add(delta)
			}

			_, err = insertStatStmt.Exec(stat, encode(value), encode(timestamp))
			return err
		}, func() error {
			getStatStmt.Close()
			insertStatStmt.Close()
			return nil
		}, nil
}

// incrementNumericStat tracks a numeric stat, incrementing the current value by
// delta. If the resulting value is negative, the function panics.
func incrementNumericStat(tx *txn, stat string, delta int, timestamp time.Time) error {
	if delta == 0 {
		return nil
	}
	timestamp = timestamp.Truncate(statInterval)
	var current uint64
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, encode(timestamp)).Scan(decode(&current))
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
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, encode(value), encode(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

// incrementCurrencyStat tracks a currency stat. If negative is false, the current
// value is incremented by delta. Otherwise, the value is decremented. If the
// resulting value would be negative, the function panics.
func incrementCurrencyStat(tx *txn, stat string, delta types.Currency, negative bool, timestamp time.Time) error {
	if delta.IsZero() {
		return nil
	}
	timestamp = timestamp.Truncate(statInterval)
	var current types.Currency
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, encode(timestamp)).Scan(decode(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	}
	value := current
	if negative {
		value = value.Sub(delta)
	} else {
		value = value.Add(delta)
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, encode(value), encode(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

func setCurrencyStat(tx *txn, stat string, value types.Currency, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	var current types.Currency
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, encode(timestamp)).Scan(decode(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	} else if value.Equals(current) {
		return nil
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, encode(value), encode(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

func setNumericStat(tx *txn, stat string, value uint64, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	var current uint64
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, encode(timestamp)).Scan(decode(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	} else if value == current {
		return nil
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, encode(value), encode(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}

func setFloat64Stat(tx *txn, stat string, f float64, timestamp time.Time) error {
	timestamp = timestamp.Truncate(statInterval)
	value := math.Float64bits(f)
	var current uint64
	err := tx.QueryRow(`SELECT stat_value FROM host_stats WHERE stat=$1 AND date_created<=$2 ORDER BY date_created DESC LIMIT 1`, stat, encode(timestamp)).Scan(decode(&current))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query existing value: %w", err)
	} else if value == current {
		return nil
	}
	_, err = tx.Exec(`INSERT INTO host_stats (stat, stat_value, date_created) VALUES ($1, $2, $3) ON CONFLICT (stat, date_created) DO UPDATE SET stat_value=EXCLUDED.stat_value`, stat, encode(value), encode(timestamp))
	if err != nil {
		return fmt.Errorf("failed to insert stat: %w", err)
	}
	return nil
}
