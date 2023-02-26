package rhp

import (
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"lukechampine.com/frand"
)

func BenchmarkReadSector(b *testing.B) {
	data := programData(frand.Bytes((rhpv2.SectorSize) * 4))

	b.SetBytes(rhpv2.SectorSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := data.Sector(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
