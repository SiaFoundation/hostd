package sqlite

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"go.sia.tech/siad/types"
)

type (
	sqlUint64   uint64 // sqlite does not support uint64, this will marshal it as a string for when we need to store the high bits
	sqlCurrency types.Currency
	sqlHash     [32]byte
	sqlTime     time.Time
)

func (su *sqlUint64) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		i, err := strconv.ParseUint(src, 10, 64)
		if err != nil {
			return err
		}
		*su = sqlUint64(i)
	case int64:
		if src < 0 {
			return fmt.Errorf("cannot scan %v to uint64", src)
		}
		*su = sqlUint64(src)
	default:
		return fmt.Errorf("cannot scan %T to uint64", src)
	}
	return nil
}

func (su *sqlUint64) Value() (driver.Value, error) {
	return strconv.FormatUint(uint64(*su), 10), nil
}

// Scan implements the sql.Scanner interface.
func (sc *sqlCurrency) Scan(src interface{}) error {
	var i big.Int
	var ok bool
	switch src := src.(type) {
	case []byte:
		_, ok = i.SetString(string(src), 10)
	case string:
		_, ok = i.SetString(src, 10)
	default:
		return fmt.Errorf("cannot scan %T to Currency", src)
	}
	if !ok {
		return fmt.Errorf("failed to scan %v to Currency", src)
	}
	*sc = (sqlCurrency)(types.NewCurrency(&i))
	return nil
}

// Value implements the driver.Valuer interface.
func (sc sqlCurrency) Value() (driver.Value, error) {
	return types.Currency(sc).String(), nil
}

// Scan implements the sql.Scanner interface.
func (sh *sqlHash) Scan(src interface{}) error {
	n := copy(sh[:], src.([]byte))
	if n != len(sh) {
		return fmt.Errorf("expected %d bytes, got %d", len(sh), n)
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (sh sqlHash) Value() (driver.Value, error) {
	return sh[:], nil
}

func (st *sqlTime) Scan(src interface{}) error {
	switch src := src.(type) {
	case int64:
		*st = sqlTime(time.Unix(src, 0))
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Time", src)
	}
}

func (st sqlTime) Value() (driver.Value, error) {
	return time.Time(st).Unix(), nil
}

func scanCurrency(c *types.Currency) *sqlCurrency {
	return (*sqlCurrency)(c)
}

func valueCurrency(c types.Currency) sqlCurrency {
	return (sqlCurrency)(c)
}

func scanHash(h *[32]byte) *sqlHash {
	return (*sqlHash)(h)
}

func valueHash(h [32]byte) sqlHash {
	return (sqlHash)(h)
}

func scanTime(t *time.Time) *sqlTime {
	return (*sqlTime)(t)
}

func valueTime(t time.Time) sqlTime {
	return (sqlTime)(t)
}

func valueUint64(u uint64) sqlUint64 {
	return (sqlUint64)(u)
}
