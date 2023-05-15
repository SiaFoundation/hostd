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

// Drives returns the paths for all Windows drives
func Drives() ([]string, error) {
	buf := make([]uint16, 254)
	n, err := windows.GetLogicalDriveStrings(uint32(len(buf)), &buf[0])
	if err != nil {
		return nil, err
	}
	var drives []string
	for _, v := range buf[:n] {
		if v < 'A' || v > 'Z' {
			continue
		}
		path := string(rune(v)) + ":"
		pathPtr, _ := windows.UTF16PtrFromString(path)
		driveType := windows.GetDriveType(pathPtr)
		if driveType == 0 {
			return nil, fmt.Errorf("failed to get drive type %q: %w", path, windows.GetLastError())
		} else if driveType != windows.DRIVE_FIXED && driveType != windows.DRIVE_REMOTE {
			continue // only include fixed and remote drives
		}
		drives = append(drives, path)
	}
	return drives, nil
}
