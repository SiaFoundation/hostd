// Package directio provides unbuffered Windows file I/O with aligned buffers.
package directio

import (
	"errors"
	"io"
	"math"
	"math/bits"
	"os"
	"sync"
	"unsafe"

	proto4 "go.sia.tech/core/rhp/v4"
	"golang.org/x/sys/windows"
)

const bufferSize = proto4.SectorSize

type fileStorageInfo struct {
	LogicalBytesPerSector                                 uint32
	PhysicalBytesPerSectorForAtomicity                    uint32
	PhysicalBytesPerSectorForPerformance                  uint32
	FileSystemEffectivePhysicalBytesPerSectorForAtomicity uint32
	Flags                                                 uint32
	ByteOffsetForSectorAlignment                          uint32
	ByteOffsetForPartitionAlignment                       uint32
}

// https://github.com/golang/go/issues/8005#issuecomment-190753527
type noCopy struct{}

func (*noCopy) Lock() {}

// File provides sector I/O. It must not be copied after first use.
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

// ReadSector reads at offset, using an aligned temporary buffer when necessary.
// The read must not cross a sector boundary.
func (f *File) ReadSector(p []byte, offset int64) (int, error) {
	if offset < 0 || len(p) > bufferSize-int(offset%bufferSize) || int64(len(p)) > math.MaxInt64-offset {
		return 0, f.invalid("read")
	} else if len(p) == 0 {
		return 0, nil
	} else if offset%int64(f.blockSize) == 0 && len(p)%f.blockSize == 0 && f.aligned(p) {
		return f.readAlignedAt(p, offset)
	}

	skip := int(offset % int64(f.blockSize))
	padded := (skip + len(p) + f.blockSize - 1) / f.blockSize * f.blockSize
	buf, pool := f.buffer(padded)
	defer pool.Put(buf)
	n, err := f.readAlignedAt((*buf)[:padded], offset-int64(skip))
	n = copy(p, (*buf)[min(skip, n):n])
	if errors.Is(err, io.EOF) && n == len(p) {
		err = nil
	} else if err == nil && n < len(p) {
		err = io.EOF
	}
	return n, err
}

// WriteSector writes a sector at offset. The offset must be a multiple of the
// sector size. Call Sync to flush the hardware cache afterward.
func (f *File) WriteSector(data *[proto4.SectorSize]byte, offset int64) (int, error) {
	if offset < 0 || offset%bufferSize != 0 || offset > math.MaxInt64-bufferSize {
		return 0, f.invalid("write")
	} else if f.aligned(data[:]) {
		return f.file.WriteAt(data[:], offset)
	}

	buf, pool := f.buffer(bufferSize)
	defer pool.Put(buf)
	copy(*buf, data[:])
	return f.file.WriteAt(*buf, offset)
}

// Sync flushes the underlying file and hardware cache.
func (f *File) Sync() error { return f.file.Sync() }

// Truncate resizes the file.
func (f *File) Truncate(size int64) error { return f.file.Truncate(size) }

// Close closes the file.
func (f *File) Close() error { return f.file.Close() }

// OpenFile uses unbuffered I/O when supported, falling back to buffered I/O
// when the file's alignment cannot be determined or unbuffered opening fails.
func OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	return openFileWithInfo(name, flag, perm, windows.GetFileInformationByHandleEx)
}

func openFileWithInfo(name string, flag int, perm os.FileMode, queryInfo func(windows.Handle, uint32, *byte, uint32) error) (*File, error) {
	flag &^= int(windows.FILE_FLAG_NO_BUFFERING)
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	var info fileStorageInfo
	validSize := func(size uint32) bool { return size != 0 && size <= bufferSize && size&(size-1) == 0 }
	if err := queryInfo(windows.Handle(f.Fd()), windows.FileStorageInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil || !validSize(info.LogicalBytesPerSector) {
		return &File{file: f, blockSize: 1, alignment: 1}, nil
	}
	// Apply creation and truncation only on the first open. Keep its handle
	// until the unbuffered open succeeds so unsupported files remain usable.
	direct, err := os.OpenFile(name, flag&^(os.O_CREATE|os.O_EXCL|os.O_TRUNC)|int(windows.FILE_FLAG_NO_BUFFERING), perm)
	if err != nil {
		return &File{file: f, blockSize: 1, alignment: 1}, nil
	} else if err := f.Close(); err != nil {
		direct.Close()
		return nil, err
	}
	alignment := info.LogicalBytesPerSector
	// Some drivers return invalid optional physical sizes; retain the logical
	// sector alignment in that case.
	for _, size := range []uint32{info.PhysicalBytesPerSectorForAtomicity, info.FileSystemEffectivePhysicalBytesPerSectorForAtomicity} {
		if validSize(size) {
			alignment = max(alignment, size)
		}
	}
	return &File{file: direct, blockSize: int(info.LogicalBytesPerSector), alignment: int(alignment)}, nil
}
