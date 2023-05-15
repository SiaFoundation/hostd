package disk

import (
	"testing"
)

func TestDrives(t *testing.T) {
	drives, err := Drives()
	if err != nil {
		t.Fatal(err)
	}

	// check that they are formatted properly
	for _, drive := range drives {
		switch {
		case len(drive) != 2: // C:
			t.Fatalf("expected drive to be 2 characters long: %q, %d", drive, len(drive))
		case drive[1] != ':':
			t.Fatalf(`expected drive to end with ":": %q`, drive)
		case drive[0] < 'A' || drive[0] > 'Z':
			t.Fatalf("expected drive to start with a capital letter: %q", drive)
		}
	}

	t.Log(drives)
}
