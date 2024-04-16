//go:build openbsd

package disk

import "golang.org/x/sys/unix"

// Usage returns the free and total bytes on the filesystem containing the
// specified path.
func Usage(p string) (free, total uint64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(p, &stat); err != nil {
		return 0, 0, err
	}
	return stat.F_bfree * uint64(stat.F_bsize), stat.F_blocks * uint64(stat.F_bsize), nil
}

// Drives returns the paths of all drives on Windows. It is a no-op on other systems
func Drives() ([]string, error) {
	return nil, nil
}
