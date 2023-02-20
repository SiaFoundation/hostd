package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"go.sia.tech/core/types"
)

type (
	sqlUint64   uint64 // sqlite does not support uint64, this will marshal it as a BLOB for when we need to store the high bits
	sqlCurrency types.Currency
	sqlHash256  [32]byte
	sqlHash512  [64]byte
	sqlTime     time.Time

	sqlNullable[T sql.Scanner] struct {
		Value T
		Valid bool
	}
)

func (sn *sqlNullable[T]) Scan(src interface{}) error {
	if src == nil {
		sn.Valid = false
		return nil
	} else if err := sn.Value.Scan(src); err != nil {
		return err
	}
	sn.Valid = true
	return nil
}

func (sh *sqlHash256) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		hex.Decode(sh[:], []byte(src))
	case []byte:
		copy(sh[:], src)
	default:
		return fmt.Errorf("cannot scan %T to Hash256", src)
	}
	return nil
}

func (sh sqlHash256) Value() (driver.Value, error) {
	return sh[:], nil
}

func (sh *sqlHash512) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		hex.Decode(sh[:], []byte(src))
	case []byte:
		copy(sh[:], src)
	default:
		return fmt.Errorf("cannot scan %T to Hash256", src)
	}
	return nil
}

func (sh sqlHash512) Value() (driver.Value, error) {
	return sh[:], nil
}

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
	case []byte:
		*su = sqlUint64(binary.LittleEndian.Uint64(src))
	default:
		return fmt.Errorf("cannot scan %T to uint64", src)
	}
	return nil
}

func (su sqlUint64) Value() (driver.Value, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(su))
	return buf, nil
}

// Scan implements the sql.Scanner interface.
func (sc *sqlCurrency) Scan(src interface{}) error {
	buf, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T to Currency", src)
	} else if len(buf) != 16 {
		return fmt.Errorf("cannot scan %d bytes to Currency", len(buf))
	}

	sc.Lo = binary.LittleEndian.Uint64(buf[:8])
	sc.Hi = binary.LittleEndian.Uint64(buf[8:])
	return nil
}

// Value implements the driver.Valuer interface.
func (sc sqlCurrency) Value() (driver.Value, error) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], sc.Lo)
	binary.LittleEndian.PutUint64(buf[8:], sc.Hi)
	return buf, nil
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

func nullable[T sql.Scanner](v T) *sqlNullable[T] {
	return &sqlNullable[T]{Value: v}
}
