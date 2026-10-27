// Package directio provides unbuffered Windows file I/O with aligned buffers.
package directio

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

const bufferSize = 4 << 20

// https://github.com/golang/go/issues/8005#issuecomment-190753527
type noCopy struct{}

func (*noCopy) Lock() {}

// File provides unbuffered I/O. It must not be copied after first use.
type File struct {
	file      *os.File
	blockSize int
	alignment int
	buffers   [11]sync.Pool // 4 KiB through 4 MiB

	noCopy noCopy
}

func (f *File) invalid(op string) error {
	return &os.PathError{Op: op, Path: f.file.Name(), Err: os.ErrInvalid}
}

func (f *File) aligned(p []byte) bool {
	return uintptr(unsafe.Pointer(unsafe.SliceData(p)))%uintptr(f.alignment) == 0
}

func (f *File) buffer(size int) (*[]byte, *sync.Pool) {
	shift := bits.Len(uint(max(size, 4<<10) - 1))
	pool := &f.buffers[shift-12]
	if buf := pool.Get(); buf != nil {
		return buf.(*[]byte), pool
	}
	length := 1 << shift
	buf := make([]byte, length+f.alignment-1)
	offset := int((-uintptr(unsafe.Pointer(&buf[0]))) & uintptr(f.alignment-1))
	buf = buf[offset : offset+length]
	return &buf, pool
}

func (f *File) readAlignedAt(p []byte, offset int64) (int, error) {
	n, err := f.file.ReadAt(p, offset)
	// A short read at an unaligned EOF makes os.File.ReadAt retry with an
	// unaligned buffer and offset. Recover the EOF that prompted that retry.
	if n > 0 && n < len(p) && errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		if info, statErr := f.file.Stat(); statErr == nil && offset <= info.Size() && int64(n) == info.Size()-offset {
			err = io.EOF
		}
	}
	return n, err
}

// ReadAt reads at offset, using an aligned temporary buffer when necessary.
func (f *File) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || int64(len(p)) > math.MaxInt64-offset {
		return 0, f.invalid("read")
	} else if len(p) == 0 {
		return 0, nil
	} else if offset%int64(f.blockSize) == 0 && len(p)%f.blockSize == 0 && f.aligned(p) {
		return f.readAlignedAt(p, offset)
	}

	skip := int(offset % int64(f.blockSize))
	length := min(len(p), bufferSize-skip)
	padded := (skip + length + f.blockSize - 1) / f.blockSize * f.blockSize
	buf, pool := f.buffer(padded)
	defer pool.Put(buf)
	var total int
	for len(p) > 0 {
		base := offset - offset%int64(f.blockSize)
		skip := int(offset - base)
		length := min(len(p), len(*buf)-skip)
		padded := (skip + length + f.blockSize - 1) / f.blockSize * f.blockSize
		n, err := f.readAlignedAt((*buf)[:padded], base)
		copied := copy(p[:length], (*buf)[min(skip, n):n])
		total += copied
		p = p[copied:]
		offset += int64(copied)
		if err != nil {
			if errors.Is(err, io.EOF) && len(p) == 0 {
				return total, nil
			}
			return total, err
		} else if copied != length {
			return total, io.EOF
		}
	}
	return total, nil
}

// WriteAt writes at offset. The offset and length must be multiples of the
// disk's logical sector size. Call Sync to flush the hardware cache afterward.
func (f *File) WriteAt(p []byte, offset int64) (int, error) {
	if offset < 0 || int64(len(p)) > math.MaxInt64-offset {
		return 0, f.invalid("write")
	} else if len(p) == 0 {
		return 0, nil
	} else if offset%int64(f.blockSize) != 0 || len(p)%f.blockSize != 0 {
		return 0, f.invalid("write")
	} else if f.aligned(p) {
		return f.file.WriteAt(p, offset)
	}

	buf, pool := f.buffer(min(len(p), bufferSize))
	defer pool.Put(buf)
	var total int
	for len(p) > 0 {
		length := copy(*buf, p)
		n, err := f.file.WriteAt((*buf)[:length], offset)
		total += n
		if err != nil {
			return total, err
		} else if n != length {
			return total, io.ErrShortWrite
		}
		p = p[n:]
		offset += int64(n)
	}
	return total, nil
}

// Sync flushes the underlying file and hardware cache.
func (f *File) Sync() error { return f.file.Sync() }

// Truncate resizes the file.
func (f *File) Truncate(size int64) error { return f.file.Truncate(size) }

// Close closes the file.
func (f *File) Close() error { return f.file.Close() }

// OpenFile opens a file with FILE_FLAG_NO_BUFFERING and determines its alignment.
func OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	f, err := os.OpenFile(name, flag|int(windows.FILE_FLAG_NO_BUFFERING), perm)
	if err != nil {
		return nil, err
	}
	var info struct {
		LogicalBytesPerSector                                 uint32
		PhysicalBytesPerSectorForAtomicity                    uint32
		PhysicalBytesPerSectorForPerformance                  uint32
		FileSystemEffectivePhysicalBytesPerSectorForAtomicity uint32
		Flags                                                 uint32
		ByteOffsetForSectorAlignment                          uint32
		ByteOffsetForPartitionAlignment                       uint32
	}
	if err := windows.GetFileInformationByHandleEx(windows.Handle(f.Fd()), windows.FileStorageInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		f.Close()
		return nil, &os.PathError{Op: "query sector alignment", Path: name, Err: err}
	}
	validSize := func(size uint32) bool { return size != 0 && size <= bufferSize && size&(size-1) == 0 }
	if !validSize(info.LogicalBytesPerSector) {
		f.Close()
		return nil, fmt.Errorf("unsupported logical sector size %d for %q: %w", info.LogicalBytesPerSector, name, os.ErrInvalid)
	}
	alignment := info.LogicalBytesPerSector
	// Some drivers return invalid optional physical sizes; retain the logical
	// sector alignment in that case.
	for _, size := range []uint32{info.PhysicalBytesPerSectorForAtomicity, info.PhysicalBytesPerSectorForPerformance, info.FileSystemEffectivePhysicalBytesPerSectorForAtomicity} {
		if validSize(size) {
			alignment = max(alignment, size)
		}
	}
	return &File{file: f, blockSize: int(info.LogicalBytesPerSector), alignment: int(alignment)}, nil
}
