package merkle

import (
	"bytes"
	"math/bits"
	"reflect"
	"testing"

	"go.sia.tech/siad/crypto"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

func leafHash(seg []byte) crypto.Hash {
	return blake2b.Sum256(append([]byte{0}, seg...))
}

func nodeHash(left, right crypto.Hash) crypto.Hash {
	return blake2b.Sum256(append([]byte{1}, append(left[:], right[:]...)...))
}

func refSectorRoot(sector []byte) crypto.Hash {
	roots := make([]crypto.Hash, LeavesPerSector)
	for i := range roots {
		roots[i] = leafHash(sector[i*LeafSize:][:LeafSize])
	}
	return recNodeRoot(roots)
}

func recNodeRoot(roots []crypto.Hash) crypto.Hash {
	switch len(roots) {
	case 0:
		return crypto.Hash{}
	case 1:
		return roots[0]
	default:
		// split at largest power of two
		split := 1 << (bits.Len(uint(len(roots)-1)) - 1)
		return nodeHash(
			recNodeRoot(roots[:split]),
			recNodeRoot(roots[split:]),
		)
	}
}

func TestSectorRoot(t *testing.T) {
	// test some known roots
	sector := make([]byte, SectorSize)
	if SectorRoot(sector).String() != "h:50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("wrong Merkle root for empty sector")
	}
	sector[0] = 1
	if SectorRoot(sector).String() != "h:8c20a2c90a733a5139cc57e45755322e304451c3434b0c0a0aad87f2f89a44ab" {
		t.Error("wrong Merkle root for sector[0] = 1")
	}
	sector[0] = 0
	sector[SectorSize-1] = 1
	if SectorRoot(sector).String() != "h:d0ab6691d76750618452e920386e5f6f98fdd1219a70a06f06ef622ac6c6373c" {
		t.Error("wrong Merkle root for sector[SectorSize-1] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		frand.Read(sector[:])
		if SectorRoot(sector) != refSectorRoot(sector) {
			t.Error("SectorRoot does not match reference implementation")
		}
	}

	// SectorRoot should not allocate
	allocs := testing.AllocsPerRun(5, func() {
		_ = SectorRoot(sector)
	})
	if allocs > 0 {
		t.Error("expected SectorRoot to allocate 0 times, got", allocs)
	}
}

func BenchmarkSectorRoot(b *testing.B) {
	b.ReportAllocs()
	sector := make([]byte, SectorSize)
	b.SetBytes(SectorSize)
	for i := 0; i < b.N; i++ {
		_ = SectorRoot(sector)
	}
}

func TestMetaRoot(t *testing.T) {
	// test some known roots
	if MetaRoot(nil) != (crypto.Hash{}) {
		t.Error("wrong Merkle root for empty tree")
	}
	roots := make([]crypto.Hash, 1)
	roots[0] = frand.Entropy256()
	if MetaRoot(roots) != roots[0] {
		t.Error("wrong Merkle root for single root")
	}
	roots = make([]crypto.Hash, 32)
	if MetaRoot(roots).String() != "h:1c23727030051d1bba1c887273addac2054afbd6926daddef6740f4f8bf1fb7f" {
		t.Error("wrong Merkle root for 32 empty roots")
	}
	roots[0][0] = 1
	if MetaRoot(roots).String() != "h:c5da05749139505704ea18a5d92d46427f652ac79c5f5712e4aefb68e20dffb8" {
		t.Error("wrong Merkle root for roots[0][0] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		for j := range roots {
			roots[j] = frand.Entropy256()
		}
		if MetaRoot(roots) != recNodeRoot(roots) {
			t.Error("MetaRoot does not match reference implementation")
		}
	}
	// test some random tree sizes
	for i := 0; i < 10; i++ {
		roots := make([]crypto.Hash, frand.Intn(LeavesPerSector))
		if MetaRoot(roots) != recNodeRoot(roots) {
			t.Error("MetaRoot does not match reference implementation")
		}
	}

	roots = roots[:5]
	if MetaRoot(roots) != recNodeRoot(roots) {
		t.Error("MetaRoot does not match reference implementation")
	}

	allocs := testing.AllocsPerRun(10, func() {
		_ = MetaRoot(roots)
	})
	if allocs > 0 {
		t.Error("expected MetaRoot to allocate 0 times, got", allocs)
	}

	// test a massive number of roots, larger than a single stack can store
	const sectorsPerTerabyte = 262145
	roots = make([]crypto.Hash, sectorsPerTerabyte)
	if MetaRoot(roots) != recNodeRoot(roots) {
		t.Error("MetaRoot does not match reference implementation")
	}
}

func BenchmarkMetaRoot1TB(b *testing.B) {
	const sectorsPerTerabyte = 262144
	roots := make([]crypto.Hash, sectorsPerTerabyte)
	b.SetBytes(sectorsPerTerabyte * 32)
	for i := 0; i < b.N; i++ {
		_ = MetaRoot(roots)
	}
}

func TestProofAccumulator(t *testing.T) {
	var pa proofAccumulator

	// test some known roots
	if pa.root() != (crypto.Hash{}) {
		t.Error("wrong root for empty accumulator")
	}

	roots := make([]crypto.Hash, 32)
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	if pa.root().String() != "1c23727030051d1bba1c887273addac2054afbd6926daddef6740f4f8bf1fb7f" {
		t.Error("wrong root for 32 empty roots")
	}

	pa = proofAccumulator{}
	roots[0][0] = 1
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	if pa.root().String() != "c5da05749139505704ea18a5d92d46427f652ac79c5f5712e4aefb68e20dffb8" {
		t.Error("wrong root for roots[0][0] = 1")
	}

	// test some random roots against a reference implementation
	for i := 0; i < 5; i++ {
		var pa proofAccumulator
		for j := range roots {
			roots[j] = frand.Entropy256()
			pa.insertNode(roots[j], 0)
		}
		if pa.root() != recNodeRoot(roots) {
			t.Error("root does not match reference implementation")
		}
	}

	// test an odd number of roots
	pa = proofAccumulator{}
	roots = roots[:5]
	for _, root := range roots {
		pa.insertNode(root, 0)
	}
	refRoot := recNodeRoot([]crypto.Hash{recNodeRoot(roots[:4]), roots[4]})
	if pa.root() != refRoot {
		t.Error("root does not match reference implementation")
	}
}

func TestBuildProof(t *testing.T) {
	// test some known proofs
	var sector [SectorSize]byte
	frand.Read(sector[:])
	sectorRoot := SectorRoot(sector[:])
	segmentRoots := make([]crypto.Hash, LeavesPerSector)
	for i := range segmentRoots {
		segmentRoots[i] = leafHash(sector[i*LeafSize:][:LeafSize])
	}

	proof := BuildProof(sector[:], 0, LeavesPerSector, nil)
	if len(proof) != 0 {
		t.Error("BuildProof constructed an incorrect proof for the entire sector")
	}

	proof = BuildProof(sector[:], 0, 1, nil)
	hash := leafHash(sector[:64])
	for i := range proof {
		hash = nodeHash(hash, proof[i])
	}
	if hash != sectorRoot {
		t.Error("BuildProof constructed an incorrect proof for the first segment")
	}

	proof = BuildProof(sector[:], LeavesPerSector-1, LeavesPerSector, nil)
	hash = leafHash(sector[len(sector)-64:])
	for i := range proof {
		hash = nodeHash(proof[len(proof)-i-1], hash)
	}
	if hash != sectorRoot {
		t.Error("BuildProof constructed an incorrect proof for the last segment")
	}

	proof = BuildProof(sector[:], 10, 11, nil)
	hash = leafHash(sector[10*64:][:64])
	hash = nodeHash(hash, proof[2])
	hash = nodeHash(proof[1], hash)
	hash = nodeHash(hash, proof[3])
	hash = nodeHash(proof[0], hash)
	for i := 4; i < len(proof); i++ {
		hash = nodeHash(hash, proof[i])
	}
	if hash != sectorRoot {
		t.Error("BuildProof constructed an incorrect proof for a middle segment")
	}

	// this is the largest possible proof
	var midl, midr uint64 = LeavesPerSector/2 - 1, LeavesPerSector/2 + 1
	proof = BuildProof(sector[:], midl, midr, nil)
	left := leafHash(sector[midl*64:][:64])
	for i := 0; i < len(proof)/2; i++ {
		left = nodeHash(proof[len(proof)/2-i-1], left)
	}
	right := leafHash(sector[(midr-1)*64:][:64])
	for i := len(proof) / 2; i < len(proof); i++ {
		right = nodeHash(right, proof[i])
	}
	if nodeHash(left, right) != sectorRoot {
		t.Error("BuildProof constructed an incorrect proof for worst-case inputs")
	}

	// test a proof with precomputed inputs
	leftRoots := make([]crypto.Hash, LeavesPerSector/2)
	for i := range leftRoots {
		leftRoots[i] = leafHash(sector[i*LeafSize:][:LeafSize])
	}
	left = MetaRoot(leftRoots)
	precalc := func(i, j uint64) (h crypto.Hash) {
		if i == 0 && j == LeavesPerSector/2 {
			h = left
		}
		return
	}
	proof = BuildProof(sector[:], LeavesPerSector-1, LeavesPerSector, precalc)
	recalcProof := BuildProof(sector[:], LeavesPerSector-1, LeavesPerSector, nil)
	if !reflect.DeepEqual(proof, recalcProof) {
		t.Fatal("precalc failed")
	}
}

func TestBuildSectorRangeProof(t *testing.T) {
	// test some known proofs
	sectorRoots := make([]crypto.Hash, 16)
	for i := range sectorRoots {
		sectorRoots[i] = frand.Entropy256()
	}

	proof := BuildSectorRangeProof(sectorRoots, 0, uint64(len(sectorRoots)))
	if len(proof) != 0 {
		t.Error("BuildSectorRangeProof constructed an incorrect proof for the entire tree")
	}

	proof = BuildSectorRangeProof(sectorRoots[:2], 0, 1)
	hash := nodeHash(sectorRoots[0], proof[0])
	if hash != MetaRoot(sectorRoots[:2]) {
		t.Error("BuildSectorRangeProof constructed an incorrect proof for the first sector")
	}

	proof = BuildSectorRangeProof(sectorRoots[:4], 0, 2)
	hash = nodeHash(sectorRoots[0], sectorRoots[1])
	hash = nodeHash(hash, proof[0])
	if hash != MetaRoot(sectorRoots[:4]) {
		t.Error("BuildSectorRangeProof constructed an incorrect proof for the first two sectors")
	}

	proof = BuildSectorRangeProof(sectorRoots[:5], 0, 2)
	hash = nodeHash(sectorRoots[0], sectorRoots[1])
	hash = nodeHash(hash, proof[0])
	hash = nodeHash(hash, proof[1])
	if hash != MetaRoot(sectorRoots[:5]) {
		t.Error("BuildSectorRangeProof constructed an incorrect proof for the first two sectors")
	}

	// this is the largest possible proof
	proof = BuildSectorRangeProof(sectorRoots, 7, 9)
	left := sectorRoots[7]
	left = nodeHash(proof[2], left)
	left = nodeHash(proof[1], left)
	left = nodeHash(proof[0], left)
	right := sectorRoots[8]
	right = nodeHash(right, proof[3])
	right = nodeHash(right, proof[4])
	right = nodeHash(right, proof[5])
	hash = nodeHash(left, right)
	if hash != MetaRoot(sectorRoots) {
		t.Error("BuildProof constructed an incorrect proof for worst-case inputs")
	}
}

func TestReadSector(t *testing.T) {
	var expected [SectorSize]byte
	frand.Read(expected[:256])
	buf := bytes.NewBuffer(nil)
	buf.Write(expected[:])

	expectedRoot := refSectorRoot(expected[:])
	root, sector, err := ReadSector(buf)
	if err != nil {
		t.Fatal(err)
	} else if expectedRoot != root {
		t.Fatalf("incorrect root: expected %s, got %s", expected, root)
	} else if !bytes.Equal(sector[:], expected[:]) {
		t.Fatalf("incorrect data: expected %v, got %v", expected, sector)
	}

	buf.Reset()
	buf.Write(expected[:len(expected)-100])
	_, _, err = ReadSector(buf)
	if err == nil {
		t.Fatal("expected read error")
	}
}

func BenchmarkReadSector(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	buf.Grow(SectorSize)

	sector := make([]byte, SectorSize)
	frand.Read(sector[:256])

	b.SetBytes(SectorSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.Write(sector)
		_, _, err := ReadSector(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

const (
	normSectorSize     = 1 << 22 // 4 MiB
	normSectorsPerTiB  = (1 << 40) / normSectorSize
	normMetadataPerTiB = normSectorsPerTiB * 32

	midSectorSize     = 1 << 20 // 1 MiB
	midSectorsPerTiB  = (1 << 40) / midSectorSize
	midMetadataPerTiB = midSectorsPerTiB * 32

	smallSectorSize     = 1 << 16 // 64 KiB
	smallSectorsPerTiB  = (1 << 40) / smallSectorSize
	smallMetadataPerTiB = smallSectorsPerTiB * 32
)

func benchSectorSize(sectorSize, dataSize uint64) func(b *testing.B) {
	return func(b *testing.B) {
		sectors := dataSize / sectorSize

		randRoots := make([]crypto.Hash, sectors)
		for i := range randRoots {
			randRoots[i] = frand.Entropy256()
		}

		b.SetBytes(int64(dataSize))
		b.ReportMetric(float64(sectors), "sectors")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			MetaRoot(randRoots)
		}
	}
}

func BenchmarkSectorSize(b *testing.B) {
	const dataSize = 1 << 40 // 1 TiB
	b.Run("small", benchSectorSize(smallSectorSize, dataSize))
	b.Run("mid", benchSectorSize(midSectorSize, dataSize))
	b.Run("normal", benchSectorSize(normSectorSize, dataSize))
}
