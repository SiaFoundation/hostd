package disk

import (
	"os"
	"testing"
)

const test = 4 * (1 << 40) / (1 << 22)

func TestUsage(t *testing.T) {
	dir, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	free, total, err := Usage(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s - %d/%d bytes", dir, free, total)
}
