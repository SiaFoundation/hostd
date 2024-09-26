package rhp_test

import (
	"testing"

	rhp2 "go.sia.tech/core/rhp/v2"
	librhp "go.sia.tech/hostd/internal/rhp"
	"lukechampine.com/frand"
)

func TestTest(t *testing.T) {
	var sector [librhp.SectorSize]byte
	frand.Read(sector[:])
	root := librhp.SectorRoot(&sector)
	expected := rhp2.SectorRoot(&sector)

	if root != expected {
		t.Fatalf("unexpected sector root: expected %v, got %v", expected, root)
	}
}

func BenchmarkSectorRoot(b *testing.B) {
	var sector [librhp.SectorSize]byte
	frand.Read(sector[:])

	expected := rhp2.SectorRoot(&sector)

	b.ReportAllocs()
	b.SetBytes(librhp.SectorSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		root := librhp.SectorRoot(&sector)
		if root != expected {
			b.Fatalf("unexpected sector root: expected %v, got %v", expected, root)
		}
	}
}
