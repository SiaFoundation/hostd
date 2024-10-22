package sqlite

import (
	"path/filepath"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/storage"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func BenchmarkSectorLocation(b *testing.B) {
	log := zaptest.NewLogger(b)
	db, err := OpenDatabase(filepath.Join(b.TempDir(), "test.db"), log)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	volumeID, err := db.AddVolume("test", false)
	if err != nil {
		b.Fatal(err)
	}

	// grow the volume to b.N sectors
	if err := db.GrowVolume(volumeID, uint64(b.N)); err != nil {
		b.Fatal(err)
	} else if err := db.SetReadOnly(volumeID, false); err != nil {
		b.Fatal(err)
	} else if err := db.SetAvailable(volumeID, true); err != nil {
		b.Fatal(err)
	}

	roots := make([]types.Hash256, b.N)
	for i := 0; i < b.N; i++ {
		roots[i] = frand.Entropy256()
		_, err := db.StoreSector(roots[i], func(storage.SectorLocation, bool) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(b.N), "sectors")

	for i := 0; i < b.N; i++ {
		_, _, err := db.SectorLocation(roots[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}
