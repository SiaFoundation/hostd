package merkle

import (
	"io"
	"math"
	"math/bits"

	"go.sia.tech/siad/crypto"
)

// A RangeProofVerifier allows range proofs to be verified in streaming fashion.
type RangeProofVerifier struct {
	start, end uint64
	roots      []crypto.Hash
}

// ReadFrom implements io.ReaderFrom.
func (rpv *RangeProofVerifier) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	i, j := rpv.start, rpv.end
	for i < j {
		subtreeSize := nextSubtreeSize(i, j)
		n := int64(subtreeSize * LeafSize)
		root, err := ReaderRoot(io.LimitReader(r, n))
		if err != nil {
			return total, err
		}
		total += n
		rpv.roots = append(rpv.roots, root)
		i += subtreeSize
	}
	return total, nil
}

// Verify verifies the supplied proof, using the data ingested from ReadFrom.
func (rpv *RangeProofVerifier) Verify(proof []crypto.Hash, root crypto.Hash) bool {
	if uint64(len(proof)) != RangeProofSize(LeavesPerSector, rpv.start, rpv.end) {
		return false
	}
	var acc proofAccumulator
	consume := func(roots *[]crypto.Hash, i, j uint64) {
		for i < j && len(*roots) > 0 {
			subtreeSize := nextSubtreeSize(i, j)
			height := bits.TrailingZeros(uint(subtreeSize)) // log2
			acc.insertNode((*roots)[0], height)
			*roots = (*roots)[1:]
			i += subtreeSize
		}
	}
	consume(&proof, 0, rpv.start)
	consume(&rpv.roots, rpv.start, rpv.end)
	consume(&proof, rpv.end, LeavesPerSector)
	return acc.root() == root
}

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

// ProofSize returns the size of a Merkle proof for the leaf i within a tree
// containing n leaves.
func ProofSize(n, i uint64) uint64 {
	leftHashes := bits.OnesCount64(i)
	pathMask := 1<<bits.Len64(n-1) - 1
	rightHashes := bits.OnesCount64(^(n - 1)) & pathMask
	return uint64(leftHashes + rightHashes)
}

// RangeProofSize returns the size of a Merkle proof for the leaf range [start,
// end) within a tree containing n leaves.
func RangeProofSize(n, start, end uint64) uint64 {
	leftHashes := bits.OnesCount64(start)
	pathMask := 1<<bits.Len64((end-1)^(n-1)) - 1
	rightHashes := bits.OnesCount64(^(end - 1)) & pathMask
	return uint64(leftHashes + rightHashes)
}

// BuildProof constructs a proof for the segment range [start, end). If a non-
// nil precalc function is provided, it will be used to supply precalculated
// subtree Merkle roots. For example, if the root of the left half of the
// Merkle tree is precomputed, precalc should return it for i == 0 and j ==
// SegmentsPerSector/2. If a precalculated root is not available, precalc
// should return the zero hash.
func BuildProof(sector []byte, start, end uint64, precalc func(i, j uint64) crypto.Hash) []crypto.Hash {
	if end > LeavesPerSector || start > end || start == end {
		panic("BuildProof: illegal proof range")
	}
	if precalc == nil {
		precalc = func(i, j uint64) (h crypto.Hash) { return }
	}

	// define a helper function for later
	var s sectorAccumulator
	subtreeRoot := func(i, j uint64) crypto.Hash {
		s.reset()
		s.appendLeaves(sector[i*LeafSize : j*LeafSize])
		return s.root()
	}

	// we build the proof by recursively enumerating subtrees, left to right.
	// If a subtree is inside the segment range, we can skip it (because the
	// verifier has the segments); otherwise, we add its Merkle root to the
	// proof.
	//
	// NOTE: this operation might be a little tricky to understand because
	// it's a recursive function with side effects (appending to proof), but
	// this is the simplest way I was able to implement it. Namely, it has the
	// important advantage of being symmetrical to the Verify operation.
	proof := make([]crypto.Hash, 0, ProofSize(LeavesPerSector, start))
	var rec func(uint64, uint64)
	rec = func(i, j uint64) {
		if i >= start && j <= end {
			// this subtree contains only data segments; skip it
		} else if j <= start || i >= end {
			// this subtree does not contain any data segments; add its Merkle
			// root to the proof. If we have a precalculated root, use that;
			// otherwise, calculate it from scratch.
			if h := precalc(i, j); h != (crypto.Hash{}) {
				proof = append(proof, h)
			} else {
				proof = append(proof, subtreeRoot(i, j))
			}
		} else {
			// this subtree partially overlaps the data segments; split it
			// into two subtrees and recurse on each
			mid := (i + j) / 2
			rec(i, mid)
			rec(mid, j)
		}
	}
	rec(0, LeavesPerSector)
	return proof
}

// BuildSectorRangeProof constructs a proof for the sector range [start, end).
func BuildSectorRangeProof(sectorRoots []crypto.Hash, start, end uint64) []crypto.Hash {
	numLeaves := uint64(len(sectorRoots))
	if numLeaves == 0 {
		return nil
	} else if end > numLeaves || start > end || start == end {
		panic("BuildSectorRangeProof: illegal proof range")
	}

	proof := make([]crypto.Hash, 0, ProofSize(numLeaves, start))
	buildRange := func(i, j uint64) {
		for i < j && i < numLeaves {
			subtreeSize := nextSubtreeSize(i, j)
			if i+subtreeSize > numLeaves {
				subtreeSize = numLeaves - i
			}
			proof = append(proof, MetaRoot(sectorRoots[i:][:subtreeSize]))
			i += subtreeSize
		}
	}
	buildRange(0, start)
	buildRange(end, math.MaxInt32)
	return proof
}

// BuildDiffProof constructs a diff proof for the specified actions.
// ActionUpdate is not supported.
func BuildDiffProof(proofIndices []uint64, sectorRoots []crypto.Hash) (treeHashes, leafHashes []crypto.Hash) {
	leafHashes = make([]crypto.Hash, len(proofIndices))
	for i, j := range proofIndices {
		leafHashes[i] = sectorRoots[j]
	}

	treeHashes = make([]crypto.Hash, 0, 128)
	buildRange := func(i, j uint64) {
		for i < j {
			subtreeSize := nextSubtreeSize(i, j)
			treeHashes = append(treeHashes, MetaRoot(sectorRoots[i:][:subtreeSize]))
			i += subtreeSize
		}
	}

	var start uint64
	for _, end := range proofIndices {
		buildRange(start, end)
		start = end + 1
	}
	buildRange(start, uint64(len(sectorRoots)))
	return
}

// ConvertProofOrdering converts "left-to-right" proofs into the "leaf-to-root"
// ordering used in consensus storage proofs.
func ConvertProofOrdering(proof []crypto.Hash, index uint64) []crypto.Hash {
	// strategy: split proof into lefts and rights, then iterate over bits in
	// leaf-to-root order, selecting either a left or right hash as appropriate.
	lefts := proof[:bits.OnesCount(uint(index))]
	rights := proof[len(lefts):]
	reordered := make([]crypto.Hash, 0, len(proof))
	for i := 0; len(reordered) < len(proof); i++ {
		if index&(1<<i) != 0 {
			reordered = append(reordered, lefts[len(lefts)-1])
			lefts = lefts[:len(lefts)-1]
		} else if len(rights) > 0 {
			reordered = append(reordered, rights[0])
			rights = rights[1:]
		}
	}
	return reordered
}

// NewRangeProofVerifier returns a RangeProofVerifier for the sector range
// [start, end).
func NewRangeProofVerifier(start, end uint64) *RangeProofVerifier {
	return &RangeProofVerifier{
		start: start,
		end:   end,
	}
}

// VerifyAppendProof verifies a proof produced by BuildAppendProof.
func VerifyAppendProof(numLeaves uint64, treeHashes []crypto.Hash, sectorRoot, oldRoot, newRoot crypto.Hash) bool {
	acc := proofAccumulator{numLeaves: numLeaves}
	for i := range acc.trees {
		if acc.hasNodeAtHeight(i) && len(treeHashes) > 0 {
			acc.trees[i] = treeHashes[0]
			treeHashes = treeHashes[1:]
		}
	}
	if acc.root() != oldRoot {
		return false
	}
	acc.insertNode(sectorRoot, 0)
	return acc.root() == newRoot
}
