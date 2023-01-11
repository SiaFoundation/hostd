package storage

import "fmt"

type (
	// A VolumeID is a unique identifier for a storage volume.
	VolumeID [32]byte
	// A SectorRoot is the Merkle root of a sector with 64 byte leaves.
	SectorRoot [32]byte
)

func (v VolumeID) String() string {
	return fmt.Sprintf("volume:%x", v[:])
}

func (r SectorRoot) String() string {
	return fmt.Sprintf("sector:%x", r[:])
}
