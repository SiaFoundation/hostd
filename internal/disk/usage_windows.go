//go:build windows

package disk

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// Usage returns the free and total bytes on the filesystem containing the
// specified path.
func Usage(p string) (free, total uint64, err error) {
	if _, err = os.Stat(p); err != nil {
		return 0, 0, fmt.Errorf("failed to stat %v: %v", p, err)
	}

	ptr, err := windows.UTF16PtrFromString(p)
	if err != nil {
		panic(err)
	}
	err = windows.GetDiskFreeSpaceEx(ptr, &free, &total, nil)
	return
}
