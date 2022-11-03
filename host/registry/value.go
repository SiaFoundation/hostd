package registry

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

const (
	// EntryTypeArbitrary is a registry value where all data is arbitrary.
	EntryTypeArbitrary ValueType = iota + 1
	// EntryTypePubKey is a registry value where the first 20 bytes of data
	// corresponds to the hash of a host's public key.
	EntryTypePubKey
)

const (
	// MaxValueDataSize is the maximum size of a Value's Data
	// field.
	MaxValueDataSize = 113
)

type (
	// Work represents the "work" of a registry value.
	Work [32]byte

	// A ValueType identifies the structure of a value's data.
	ValueType uint8

	// A Value is stored in the host registry.
	Value struct {
		Tweak    crypto.Hash
		Data     []byte
		Revision uint64
		Type     ValueType

		PublicKey types.SiaPublicKey
		Signature []byte
	}
)

// Cmp compares to Work values.
func (w Work) Cmp(b Work) int {
	return bytes.Compare(w[:], b[:])
}

// Key returns the key for the registry value.
func (r *Value) Key() crypto.Hash {
	return Key(r.PublicKey.Key, r.Tweak)
}

// Hash returns the hash of the Value used for signing
// the entry.
func (r *Value) Hash() (hash crypto.Hash) {
	h, _ := blake2b.New256(nil)
	h.Write(r.Tweak[:])
	binary.Write(h, binary.LittleEndian, uint64(len(r.Data)))
	h.Write(r.Data)
	binary.Write(h, binary.LittleEndian, r.Revision)
	binary.Write(h, binary.LittleEndian, uint64(r.Type))
	h.Sum(hash[:0])
	return
}

// Work returns the work of a Value.
func (r *Value) Work() (work Work) {
	var data []byte
	switch r.Type {
	case EntryTypePubKey:
		// for public key entries the first 20 bytes represent the
		// public key of the host, ignore it for work calculations.
		data = r.Data[20:]
	default:
		data = r.Data
	}

	h, _ := blake2b.New256(nil)
	h.Write(r.Tweak[:])
	binary.Write(h, binary.LittleEndian, uint64(len(data)))
	h.Write(data)
	binary.Write(h, binary.LittleEndian, r.Revision)
	h.Sum(work[:0])
	return
}

// validateValue validates the fields of a registry entry.
func validateValue(value Value) (err error) {
	switch value.Type {
	case EntryTypeArbitrary:
		// no extra validation required
	case EntryTypePubKey:
		// pub key entries have the first 20 bytes of the host's pub key hash
		// prefixed to the data.
		if len(value.Data) < 20 {
			return errors.New("expected host public key hash")
		}
	default:
		return fmt.Errorf("invalid registry value type: %d", value.Type)
	}

	sigHash := value.Hash()
	switch {
	case !ed25519.Verify(value.PublicKey.Key, sigHash[:], value.Signature):
		return errors.New("registry value signature invalid")
	case len(value.Data) > MaxValueDataSize:
		return fmt.Errorf("registry value too large: %d", len(value.Data))
	}
	return nil
}

// validateUpdate validates a registry update against the current entry.
// An updated registry entry must have a greater revision number, more work, or
// be replacing a non-primary registry entry.
func validateUpdate(old, update Value, hostID crypto.Hash) error {
	// if the new revision is greater than the current revision, the update is
	// valid.
	if update.Revision > old.Revision {
		return nil
	} else if update.Revision < old.Revision {
		return errors.New("update revision must be greater than current revision")
	}

	// if the revision number is the same, but the work is greater, the update
	// is valid.
	if w := update.Work().Cmp(old.Work()); w > 0 {
		return nil
	} else if w < 0 {
		return errors.New("update must have greater work or greater revision number than current entry")
	}

	// if the updated entry is an arbitrary value entry, the update is invalid.
	if update.Type == EntryTypeArbitrary {
		return errors.New("update must be a primary entry or have a greater revision number")
	}

	// if the updated entry is not a primary entry, it is invalid.
	if !bytes.Equal(update.Data[:20], hostID[:20]) {
		return errors.New("update must be a primary entry or have a greater revision number")
	}

	// if the update and current entry are both primary, the update is invalid
	if old.Type == EntryTypePubKey && bytes.Equal(old.Data[:20], hostID[:20]) {
		return errors.New("update revision must be greater than current revision")
	}

	return nil
}

// Key is the unique key for a Value.
func Key(pub ed25519.PublicKey, tweak crypto.Hash) crypto.Hash {
	// v1 compat registry key
	// ed25519 specifier + LE uint64 pub key length + public key + tweak
	buf := make([]byte, 16+8+32+32)
	copy(buf, "ed25519")
	binary.LittleEndian.PutUint64(buf[16:], 32)
	copy(buf[24:], pub)
	copy(buf[56:], tweak[:])
	return blake2b.Sum256(buf)
}

// HostID returns the ID hash of the host for primary registry entries.
func HostID(pub ed25519.PublicKey) crypto.Hash {
	// v1 compat host public key hash
	// ed25519 specifier + LE uint64 pub key length + public key
	buf := make([]byte, 16+8+32)
	copy(buf, "ed25519")
	binary.LittleEndian.PutUint64(buf[16:], 32)
	copy(buf[24:], pub)
	return blake2b.Sum256(buf)
}
