package disk

import (
	"os"
	"testing"
)

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
