package rhp

import (
	"testing"

	rhp2 "go.sia.tech/core/rhp/v2"
	"lukechampine.com/frand"
)

func BenchmarkReadSector(b *testing.B) {
	data := programData(frand.Bytes((rhp2.SectorSize) * 4))

	b.SetBytes(rhp2.SectorSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := data.Sector(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
