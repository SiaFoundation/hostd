package storage

import (
	"fmt"

	"go.sia.tech/core/types"
)

type (
	// A SectorRoot is the Merkle root of a sector with 64 byte leaves.
	SectorRoot types.Hash256
)

func (r SectorRoot) String() string {
	return fmt.Sprintf("sector:%x", r[:])
}
