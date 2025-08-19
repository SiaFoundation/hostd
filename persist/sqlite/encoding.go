package sqlite

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	rhp3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

func encode(obj any) any {
	switch obj := obj.(type) {
	case types.Currency:
		// Currency is encoded as two 64-bit LE integers
		// TODO: migrate to big-endian for sorting
		buf := make([]byte, 16)
		binary.LittleEndian.PutUint64(buf[:8], obj.Lo)
		binary.LittleEndian.PutUint64(buf[8:], obj.Hi)
		return buf
	case rhp3.Account:
		// rhp3 accounts are encoded as [32]byte
		return obj[:]
	case []types.Hash256:
		var buf bytes.Buffer
		e := types.NewEncoder(&buf)
		types.EncodeSlice(e, obj)
		e.Flush()
		return buf.Bytes()
	case types.EncoderTo:
		var buf bytes.Buffer
		e := types.NewEncoder(&buf)
		obj.EncodeTo(e)
		e.Flush()
		return buf.Bytes()
	case int: // special case for encoding contracts
		if obj < 0 {
			panic(fmt.Sprintf("dbEncode: cannot encode negative int %d", obj))
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(obj))
		return b
	case int64: // special case for encoding metrics
		if obj < 0 {
			panic(fmt.Sprintf("dbEncode: cannot encode negative int64 %d", obj))
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(obj))
		return b
	case uint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, obj)
		return b
	case time.Time:
		return obj.Unix()
	default:
		panic(fmt.Sprintf("dbEncode: unsupported type %T", obj))
	}
}

type decodable struct {
	v any
}

type nullDecodable[T any] struct {
	v     *T
	valid bool
}

func (nd *nullDecodable[T]) Scan(src any) error {
	nd.valid = false
	if src == nil {
		return nil
	} else if err := decode(nd.v).Scan(src); err != nil {
		return err
	}
	nd.valid = true
	return nil
}

func decodeNullable[T any](v *T) *nullDecodable[T] {
	return &nullDecodable[T]{v: v}
}

// Scan implements the sql.Scanner interface.
func (d *decodable) Scan(src any) error {
	if src == nil {
		return errors.New("cannot scan nil into decodable")
	}

	switch src := src.(type) {
	case []byte:
		switch v := d.v.(type) {
		case *types.Currency:
			if len(src) != 16 {
				return fmt.Errorf("cannot scan %d bytes into Currency", len(src))
			}
			v.Lo = binary.LittleEndian.Uint64(src[:8])
			v.Hi = binary.LittleEndian.Uint64(src[8:])
		case *rhp3.Account:
			// rhp3 accounts are encoded as [32]byte
			if len(src) != len(rhp3.ZeroAccount) {
				return fmt.Errorf("cannot scan %d bytes into rhp3.Account", len(src))
			}
			copy(v[:], src)
		case types.DecoderFrom:
			dec := types.NewBufDecoder(src)
			v.DecodeFrom(dec)
			return dec.Err()
		case *int64: // special case for decoding metrics
			*v = int64(binary.LittleEndian.Uint64(src))
		case *uint64:
			*v = binary.LittleEndian.Uint64(src)
		case *[]types.Hash256:
			dec := types.NewBufDecoder(src)
			types.DecodeSlice(dec, v)
		default:
			return fmt.Errorf("cannot scan %T to %T", src, d.v)
		}
		return nil
	case int64:
		switch v := d.v.(type) {
		case *int64:
			*v = src
		case *uint64:
			*v = uint64(src)
		case *time.Time:
			*v = time.Unix(src, 0).UTC()
		default:
			return fmt.Errorf("cannot scan %T to %T", src, d.v)
		}
		return nil
	default:
		return fmt.Errorf("cannot scan %T to %T", src, d.v)
	}
}

func encodeSlice[T types.EncoderTo](v []T) []byte {
	buf := bytes.NewBuffer(nil)
	e := types.NewEncoder(buf)
	types.EncodeSlice(e, v)
	if err := e.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decode(obj any) sql.Scanner {
	return &decodable{obj}
}
