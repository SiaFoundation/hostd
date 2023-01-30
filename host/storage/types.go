package storage

import "fmt"

type (
	// A SectorRoot is the Merkle root of a sector with 64 byte leaves.
	SectorRoot [32]byte
)

func (r SectorRoot) String() string {
	return fmt.Sprintf("sector:%x", r[:])
}
