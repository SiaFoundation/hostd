//go:build !windows && !openbsd

package disk

import "golang.org/x/sys/unix"

// Usage returns the free and total bytes on the filesystem containing the
// specified path.
func Usage(p string) (free, total uint64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(p, &stat); err != nil {
		return 0, 0, err
	}
	// Linux-based systems and Darwin use bfree and blocks to represent the
	// number of free and total blocks in the filesystem. Multiplying by f_bsize
	// converts that to the number of free and total bytes of a filesystem.
	return stat.Bfree * uint64(stat.Bsize), stat.Blocks * uint64(stat.Bsize), nil
}

// Drives returns the paths of all drives on Windows. It is a no-op on other systems
func Drives() ([]string, error) {
	return nil, nil
}
