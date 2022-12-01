package rhp

import (
	"math/bits"
	"sort"

	"go.sia.tech/hostd/internal/merkle"
	"go.sia.tech/siad/crypto"
)

// nextSubtreeSize returns the size of the subtree adjacent to start that does
// not overlap end.
func nextSubtreeSize(start, end uint64) uint64 {
	ideal := bits.TrailingZeros64(start)
	max := bits.Len64(end-start) - 1
	if ideal > max {
		return 1 << max
	}
	return 1 << ideal
}

// sectorsChanged returns the indices of the sectors that were changed by the
// specified actions.
func sectorsChanged(actions []rpcWriteAction, numSectors uint64) (sectorIndices []uint64) {
	newNumSectors := numSectors
	sectorsChanged := make(map[uint64]struct{})
	for _, action := range actions {
		switch action.Type {
		case rpcWriteActionAppend:
			sectorsChanged[newNumSectors] = struct{}{}
			newNumSectors++
		case rpcWriteActionTrim:
			for i := 0; i < int(action.A); i++ {
				newNumSectors--
				sectorsChanged[newNumSectors] = struct{}{}
			}
		case rpcWriteActionSwap:
			sectorsChanged[action.A] = struct{}{}
			sectorsChanged[action.B] = struct{}{}

		default:
			panic("unknown or unsupported action type: " + action.Type.String())
		}
	}

	for index := range sectorsChanged {
		if index < numSectors {
			sectorIndices = append(sectorIndices, index)
		}
	}
	sort.Slice(sectorIndices, func(i, j int) bool { return sectorIndices[i] < sectorIndices[j] })
	return sectorIndices
}

// buildDiffProof constructs a diff proof for the specified actions.
// ActionUpdate is not supported.
func buildDiffProof(actions []rpcWriteAction, sectorRoots []crypto.Hash) (treeHashes, leafHashes []crypto.Hash) {
	indices := sectorsChanged(actions, uint64(len(sectorRoots)))
	leafHashes = make([]crypto.Hash, len(indices))
	for i, j := range indices {
		leafHashes[i] = sectorRoots[j]
	}

	treeHashes = make([]crypto.Hash, 0, 128)
	buildRange := func(i, j uint64) {
		for i < j {
			subtreeSize := nextSubtreeSize(i, j)
			treeHashes = append(treeHashes, merkle.MetaRoot(sectorRoots[i:][:subtreeSize]))
			i += subtreeSize
		}
	}

	var start uint64
	for _, end := range indices {
		buildRange(start, end)
		start = end + 1
	}
	buildRange(start, uint64(len(sectorRoots)))
	return
}
