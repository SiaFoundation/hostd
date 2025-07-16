package rhp

import (
	"testing"

	proto4 "go.sia.tech/core/rhp/v4"
	"lukechampine.com/frand"
)

func BenchmarkReadSector(b *testing.B) {
	data := programData(frand.Bytes((proto4.SectorSize) * 4))

	b.SetBytes(proto4.SectorSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := data.Sector(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
