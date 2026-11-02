package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	proto4 "go.sia.tech/core/rhp/v4"
	"golang.org/x/sys/windows"
)

func scratchFile(t *testing.T) *volumeDataFile {
	t.Helper()
	f, err := openVolumeData(filepath.Join(t.TempDir(), "volume.dat"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f.(*volumeDataFile)
}

func testAlignedBuffer(size, alignment int) []byte {
	buf := make([]byte, size+alignment-1)
	offset := int((-uintptr(unsafe.Pointer(&buf[0]))) & uintptr(alignment-1))
	return buf[offset : offset+size]
}

func TestFileRoundTrip(t *testing.T) {
	f := scratchFile(t)
	const sectorSize = 4 << 20
	for sector, unaligned := range []bool{false, true} {
		data := testAlignedBuffer(sectorSize+1, f.alignment)
		if unaligned {
			data = data[1:]
		} else {
			data = data[:sectorSize]
		}
		for i := range data {
			data[i] = byte(i*31 + i/257 + sector*113)
		}
		offset := int64(sector * sectorSize)
		if n, err := f.WriteSector((*[proto4.SectorSize]byte)(data), offset); n != len(data) || err != nil {
			t.Fatalf("unaligned=%v write: n=%d err=%v", unaligned, n, err)
		} else if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		read := testAlignedBuffer(sectorSize+1, f.alignment)
		if unaligned {
			read = read[1:]
		} else {
			read = read[:sectorSize]
		}
		if n, err := f.ReadSector(read, offset); n != len(read) || err != nil || !bytes.Equal(read, data) {
			t.Fatalf("unaligned=%v read: n=%d err=%v equal=%v", unaligned, n, err, bytes.Equal(read, data))
		}
	}
	path := f.file.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	} else if len(actual) != 2*sectorSize {
		t.Fatalf("expected writes to extend file to %d bytes, got %d", 2*sectorSize, len(actual))
	}
	for i, v := range actual {
		j := i % sectorSize
		if v != byte(j*31+j/257+(i/sectorSize)*113) {
			t.Fatalf("contents after close differ at %d", i)
		}
	}
}

func TestFileRandomWrites(t *testing.T) {
	const sectorSize, fileSize = proto4.SectorSize, 24 << 20
	rng := rand.New(rand.NewSource(1))
	want := make([]byte, fileSize)
	rng.Read(want)
	path := filepath.Join(t.TempDir(), "volume.dat")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	d, err := openVolumeData(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f := d.(*volumeDataFile)
	t.Cleanup(func() { f.Close() })
	writeBuffer := testAlignedBuffer(sectorSize+1, f.alignment)
	readBuffer := testAlignedBuffer(sectorSize+1, f.alignment)
	var offset int
	for iteration := range 96 {
		const size = sectorSize
		// Leave guards at both ends and repeat locations to exercise overwrites.
		if iteration%4 != 1 {
			offset = sectorSize + rng.Intn(fileSize/sectorSize-2)*sectorSize
		}
		data := writeBuffer[iteration%2 : iteration%2+size]
		rng.Read(data)
		data[0] = byte(iteration + 1)
		copy(want[offset:offset+size], data)
		if n, err := f.WriteSector((*[proto4.SectorSize]byte)(data), int64(offset)); n != size || err != nil {
			t.Fatalf("iteration %d write offset=%d size=%d: n=%d err=%v", iteration, offset, size, n, err)
		} else if !bytes.Equal(data, want[offset:offset+size]) {
			t.Fatalf("iteration %d write modified its input", iteration)
		} else if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		read := readBuffer[(iteration/2)%2 : (iteration/2)%2+size]
		if n, err := f.ReadSector(read, int64(offset)); n != size || err != nil || !bytes.Equal(read, want[offset:offset+size]) {
			t.Fatalf("iteration %d readback offset=%d size=%d: n=%d err=%v", iteration, offset, size, n, err)
		}
		readOffset := rng.Intn(fileSize)
		readSize := 1 + rng.Intn(sectorSize-readOffset%sectorSize)
		read = readBuffer[1 : 1+readSize]
		if n, err := f.ReadSector(read, int64(readOffset)); n != readSize || err != nil || !bytes.Equal(read, want[readOffset:readOffset+readSize]) {
			t.Fatalf("iteration %d partial read offset=%d size=%d: n=%d err=%v", iteration, readOffset, readSize, n, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	} else if len(actual) != len(want) {
		t.Fatalf("file size changed: got %d, want %d", len(actual), len(want))
	}
	for i, v := range actual {
		if v != want[i] {
			t.Fatalf("contents after close differ at %d: got %02x, want %02x", i, v, want[i])
		}
	}
}

func TestFileReadSector(t *testing.T) {
	f := scratchFile(t)
	blockSize := max(f.blockSize, 512)
	data := testAlignedBuffer(proto4.SectorSize, f.alignment)
	for i := range data {
		data[i] = byte(i*17 + i/251)
	}
	if _, err := f.WriteSector((*[proto4.SectorSize]byte)(data), 0); err != nil {
		t.Fatal(err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	size := blockSize + 5
	if err := f.Truncate(int64(size)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		offset int
		length int
	}{
		{0, size},
		{1, 1},
		{blockSize - 3, 11},
		{blockSize, blockSize},
		{blockSize + 1, 3},
		{blockSize + 1, 4},
		{blockSize + 1, 5},
		{size - 2, 2},
		{size - 2, 7},
		{size, 1},
		{size + blockSize, 1},
		{0, 0},
		{size + 1, 0},
	} {
		t.Run(fmt.Sprintf("Offset%dLength%d", tc.offset, tc.length), func(t *testing.T) {
			guarded := bytes.Repeat([]byte{0xa5}, tc.length+2)
			read := guarded[1 : len(guarded)-1]
			wantN := min(tc.length, max(0, size-tc.offset))
			var wantErr error
			if wantN < tc.length {
				wantErr = io.EOF
			}
			n, err := f.ReadSector(read, int64(tc.offset))
			if n != wantN || !errors.Is(err, wantErr) {
				t.Fatalf("read: n=%d err=%v, want n=%d err=%v", n, err, wantN, wantErr)
			} else if n > 0 && !bytes.Equal(read[:n], data[tc.offset:tc.offset+n]) {
				t.Fatal("read contents differ")
			} else if guarded[0] != 0xa5 || guarded[len(guarded)-1] != 0xa5 {
				t.Fatal("read overwrote bytes outside the supplied slice")
			}
		})
	}
	aligned := testAlignedBuffer(blockSize, f.alignment)
	if n, err := f.ReadSector(aligned, int64(blockSize)); n != 5 || !errors.Is(err, io.EOF) {
		t.Fatalf("aligned read across EOF: n=%d err=%v", n, err)
	} else if !bytes.Equal(aligned[:5], data[blockSize:size]) {
		t.Fatal("aligned read at EOF returned incorrect contents")
	}
	for _, offset := range []int64{-1, math.MaxInt64 - 1, math.MaxInt64} {
		if n, err := f.ReadSector(make([]byte, 8), offset); n != 0 || !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("read offset %d: n=%d err=%v", offset, n, err)
		}
	}
	for _, tc := range []struct {
		offset int64
		length int
	}{{proto4.SectorSize - 1, 2}, {0, proto4.SectorSize + 1}} {
		if n, err := f.ReadSector(make([]byte, tc.length), tc.offset); n != 0 || !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("read across sector boundary: n=%d err=%v", n, err)
		}
	}
	if n, err := f.ReadSector(nil, math.MaxInt64); n != 0 || err != nil {
		t.Fatalf("empty read: n=%d err=%v", n, err)
	}
}

func TestFileLargeRead(t *testing.T) {
	f := scratchFile(t)
	const size = proto4.SectorSize
	data := testAlignedBuffer(size+1, f.alignment)[1:]
	for i := range data {
		data[i] = byte(i*13 + i/(4<<20))
	}
	if n, err := f.WriteSector((*[proto4.SectorSize]byte)(data), 0); n != len(data) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	read := testAlignedBuffer(size, f.alignment)[1:]
	if n, err := f.ReadSector(read, 1); n != len(read) || err != nil || !bytes.Equal(read, data[1:]) {
		t.Fatalf("large partial read: n=%d err=%v equal=%v", n, err, bytes.Equal(read, data[1:]))
	}
}

func TestFileInvalidWrite(t *testing.T) {
	f := scratchFile(t)
	data := new([proto4.SectorSize]byte)
	for _, offset := range []int64{-1, 1, proto4.SectorSize - 1, proto4.SectorSize + 1, math.MaxInt64 - math.MaxInt64%proto4.SectorSize} {
		if n, err := f.WriteSector(data, offset); n != 0 || !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("write offset=%d: n=%d err=%v", offset, n, err)
		}
	}
	if info, err := f.file.Stat(); err != nil {
		t.Fatal(err)
	} else if info.Size() != 0 {
		t.Fatal("invalid write changed file size")
	}
}

func TestFileLargeSparseOffsets(t *testing.T) {
	f := scratchFile(t)
	var returned uint32
	if err := windows.DeviceIoControl(windows.Handle(f.file.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &returned, nil); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x37}, proto4.SectorSize)
	const base = int64(1 << 32)
	offset := base + proto4.SectorSize
	if err := f.Truncate(offset + int64(len(data))); err != nil {
		t.Fatal(err)
	} else if n, err := f.WriteSector((*[proto4.SectorSize]byte)(data), offset); n != len(data) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, len(data))
	if n, err := f.ReadSector(read, offset); n != len(data) || err != nil {
		t.Fatalf("read at sparse offset: n=%d err=%v", n, err)
	} else if !bytes.Equal(read, data) {
		t.Fatal("contents at large offset differ")
	}
	if n, err := f.ReadSector(read[:2], offset-2); n != 2 || err != nil || !bytes.Equal(read[:2], []byte{0, 0}) {
		t.Fatalf("read before sparse data: n=%d err=%v", n, err)
	} else if n, err := f.ReadSector(read[:1], offset+int64(len(data))); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read after sparse data: n=%d err=%v", n, err)
	}
}

func TestFileConcurrentIO(t *testing.T) {
	f := scratchFile(t)
	const workers, iterations, sectorSize = 8, 4, 4 << 20
	stride := 2 * sectorSize
	expected := testAlignedBuffer(workers*stride+sectorSize, f.alignment)
	for i := range expected {
		expected[i] = 0xa5
	}
	for offset := 0; offset < len(expected); offset += sectorSize {
		if n, err := f.WriteSector((*[proto4.SectorSize]byte)(expected[offset:offset+sectorSize]), int64(offset)); n != sectorSize || err != nil {
			t.Fatalf("initialize guarded file: n=%d err=%v", n, err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	results := make(chan error, workers)
	for worker := range workers {
		go func() {
			results <- func() error {
				offset := int64(sectorSize + worker*stride)
				data := testAlignedBuffer(sectorSize+1, f.alignment)
				read := testAlignedBuffer(sectorSize+1, f.alignment)
				if worker%2 == 0 {
					data, read = data[:sectorSize], read[1:]
				} else {
					data, read = data[1:], read[:sectorSize]
				}
				ready <- struct{}{}
				<-start
				for iteration := range iterations {
					for i := range data {
						data[i] = byte(worker*31 + iteration*17 + i*7 + i/251 + i/65521)
					}
					if n, err := f.WriteSector((*[proto4.SectorSize]byte)(data), offset); n != len(data) || err != nil {
						return fmt.Errorf("worker %d iteration %d write: n=%d err=%v", worker, iteration, n, err)
					} else if err := f.Sync(); err != nil {
						return err
					} else if n, err := f.ReadSector(read, offset); n != len(read) || err != nil || !bytes.Equal(read, data) {
						return fmt.Errorf("worker %d iteration %d read: n=%d err=%v equal=%v", worker, iteration, n, err, bytes.Equal(read, data))
					}
					length := min(f.blockSize+17, sectorSize/2)
					begin := (iteration*sectorSize/iterations + f.blockSize - 1) % (sectorSize - length)
					partial := read[1 : length+1]
					if n, err := f.ReadSector(partial, offset+int64(begin)); n != length || err != nil || !bytes.Equal(partial, data[begin:begin+length]) {
						return fmt.Errorf("worker %d iteration %d partial read: n=%d err=%v", worker, iteration, n, err)
					}
				}
				return nil
			}()
		}()
	}
	for range workers {
		<-ready
	}
	close(start)
	for range workers {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		return
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for worker := range workers {
		offset := sectorSize + worker*stride
		for i := range sectorSize {
			expected[offset+i] = byte(worker*31 + (iterations-1)*17 + i*7 + i/251 + i/65521)
		}
	}
	actual, err := os.ReadFile(f.file.Name())
	if err != nil {
		t.Fatal(err)
	} else if len(actual) != len(expected) {
		t.Fatalf("file size changed: got %d, expected %d", len(actual), len(expected))
	}
	for i, v := range actual {
		if v != expected[i] {
			t.Fatalf("file contents after reopen differ at %d: got %02x, expected %02x", i, v, expected[i])
		}
	}
}

func TestFileLifecycle(t *testing.T) {
	f := scratchFile(t)
	if err := f.Truncate(int64(f.blockSize)); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, f.blockSize)
	if n, err := f.ReadSector(read, 0); n != len(read) || err != nil || !bytes.Equal(read, make([]byte, len(read))) {
		t.Fatalf("read grown empty file: n=%d err=%v", n, err)
	} else if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	} else if n, err := f.ReadSector(read, 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty read: n=%d err=%v", n, err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadSector(read, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed read: %v", err)
	} else if _, err := f.WriteSector(new([proto4.SectorSize]byte), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed write: %v", err)
	} else if err := f.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed sync: %v", err)
	} else if err := f.Truncate(0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed truncate: %v", err)
	} else if err := f.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second close: %v", err)
	}
}

func TestOpenFileErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.dat")
	if _, err := openVolumeData(path, os.O_RDWR, 0600); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
	f := scratchFile(t)
	path = f.file.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := openVolumeData(path, os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.WriteSector(new([proto4.SectorSize]byte), 0); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("write error lost Windows error identity: %v", err)
	}
}

func TestOpenFileBufferedFallback(t *testing.T) {
	for _, tc := range []struct {
		name    string
		logical uint32
		err     error
	}{
		{"query error", 512, windows.ERROR_NOT_SUPPORTED},
		{"zero logical size", 0, nil},
		{"non-power-of-two logical size", 768, nil},
		{"oversized logical size", 2 * bufferSize, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries int
			queryInfo := func(_ windows.Handle, _ uint32, p *byte, _ uint32) error {
				queries++
				*(*fileStorageInfo)(unsafe.Pointer(p)) = fileStorageInfo{LogicalBytesPerSector: tc.logical}
				return tc.err
			}
			path := filepath.Join(t.TempDir(), "volume.dat")
			f, err := openVolumeDataWithInfo(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|int(windows.FILE_FLAG_NO_BUFFERING), 0600, queryInfo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			if f.blockSize != 1 || f.alignment != 1 {
				t.Fatalf("expected buffered file, got block size %d and alignment %d", f.blockSize, f.alignment)
			}
			data := new([proto4.SectorSize]byte)
			for i := range data {
				data[i] = byte(i*17 + i/251)
			}
			if n, err := f.WriteSector(data, 0); n != len(data) || err != nil {
				t.Fatalf("buffered write: n=%d err=%v", n, err)
			} else if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			read := make([]byte, 13)
			if n, err := f.ReadSector(read, 3); n != len(read) || err != nil || !bytes.Equal(read, data[3:16]) {
				t.Fatalf("buffered partial read: n=%d err=%v data=%x", n, err, read)
			}
			if existing, err := openVolumeDataWithInfo(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600, queryInfo); !errors.Is(err, os.ErrExist) {
				if existing != nil {
					existing.Close()
				}
				t.Fatalf("exclusive open: %v", err)
			} else if queries != 1 {
				t.Fatalf("queried metadata after failed open: %d queries", queries)
			}
			if actual, err := os.ReadFile(path); err != nil || !bytes.Equal(actual, data[:]) {
				t.Fatalf("exclusive open changed contents: %v", err)
			} else if err := f.Close(); err != nil {
				t.Fatal(err)
			} else if _, err := f.ReadSector(read, 0); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("closed buffered read: %v", err)
			} else if err := os.Remove(path); err != nil {
				t.Fatalf("buffered file handle leaked: %v", err)
			}
		})
	}
}

func TestOpenFileAlignment(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		atomicity, effective, alignment uint32
	}{
		{"ignore performance size", 4096, 4096, 4096},
		{"larger atomicity", 16384, 4096, 16384},
		{"larger effective atomicity", 4096, 16384, 16384},
		{"invalid optional sizes", 768, math.MaxUint32, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryInfo := func(_ windows.Handle, _ uint32, p *byte, _ uint32) error {
				*(*fileStorageInfo)(unsafe.Pointer(p)) = fileStorageInfo{
					LogicalBytesPerSector:                                 512,
					PhysicalBytesPerSectorForAtomicity:                    tc.atomicity,
					PhysicalBytesPerSectorForPerformance:                  64 << 10,
					FileSystemEffectivePhysicalBytesPerSectorForAtomicity: tc.effective,
				}
				return nil
			}
			path := filepath.Join(t.TempDir(), "volume.dat")
			f, err := openVolumeDataWithInfo(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600, queryInfo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			if f.blockSize == 1 {
				probe, err := os.OpenFile(path, os.O_RDWR|int(windows.FILE_FLAG_NO_BUFFERING), 0600)
				if err != nil {
					t.Skip("filesystem does not support unbuffered opening:", err)
				}
				probe.Close()
				t.Fatal("unexpected buffered fallback on a filesystem supporting unbuffered opening")
			} else if f.blockSize != 512 || f.alignment != int(tc.alignment) {
				t.Fatalf("block size=%d alignment=%d, want 512 and %d", f.blockSize, f.alignment, tc.alignment)
			} else if err := f.Close(); err != nil {
				t.Fatal(err)
			} else if err := os.Remove(path); err != nil {
				t.Fatalf("initial buffered handle leaked: %v", err)
			}
		})
	}
}

func TestOpenFileTruncate(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "volume.dat")
			if err := os.WriteFile(path, []byte("old contents"), 0600); err != nil {
				t.Fatal(err)
			}
			marker := []byte("written after initial truncation")
			var size uint64
			var written uint32
			var callbackErr error
			queryInfo := func(h windows.Handle, class uint32, p *byte, length uint32) error {
				var info windows.ByHandleFileInformation
				callbackErr = windows.GetFileInformationByHandle(h, &info)
				if callbackErr != nil {
					return callbackErr
				}
				size = uint64(info.FileSizeHigh)<<32 | uint64(info.FileSizeLow)
				callbackErr = windows.WriteFile(h, marker, &written, nil)
				if callbackErr != nil {
					return callbackErr
				}
				callbackErr = windows.FlushFileBuffers(h)
				if callbackErr != nil {
					return callbackErr
				} else if fallback {
					return windows.ERROR_NOT_SUPPORTED
				}
				return windows.GetFileInformationByHandleEx(h, class, p, length)
			}
			f, err := openVolumeDataWithInfo(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600, queryInfo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			if callbackErr != nil || size != 0 || written != uint32(len(marker)) {
				t.Fatalf("initial truncation: size=%d written=%d err=%v", size, written, callbackErr)
			} else if actual, err := os.ReadFile(path); err != nil || !bytes.Equal(actual, marker) {
				t.Fatalf("file was truncated again: contents=%q err=%v", actual, err)
			} else if fallback && f.blockSize != 1 {
				t.Fatal("expected buffered file")
			} else if !fallback && f.blockSize == 1 {
				t.Skip("filesystem does not support unbuffered opening")
			}
		})
	}
}

func TestOpenFileDirectOpenFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume.dat")
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { windows.SetFileAttributes(name, windows.FILE_ATTRIBUTE_NORMAL) })
	var attributeErr error
	queryInfo := func(_ windows.Handle, _ uint32, p *byte, _ uint32) error {
		*(*fileStorageInfo)(unsafe.Pointer(p)) = fileStorageInfo{LogicalBytesPerSector: 512}
		attributeErr = windows.SetFileAttributes(name, windows.FILE_ATTRIBUTE_READONLY)
		return nil
	}
	f, err := openVolumeDataWithInfo(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600, queryInfo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if attributeErr != nil {
		t.Fatal(attributeErr)
	} else if f.blockSize != 1 || f.alignment != 1 {
		t.Fatal("expected failed unbuffered reopen to retain the buffered file")
	}
	if n, err := f.WriteSector(new([proto4.SectorSize]byte), 0); n != proto4.SectorSize || err != nil {
		t.Fatalf("retained buffered handle is not writable: n=%d err=%v", n, err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	} else if err := windows.SetFileAttributes(name, windows.FILE_ATTRIBUTE_NORMAL); err != nil {
		t.Fatal(err)
	} else if err := os.Remove(path); err != nil {
		t.Fatalf("fallback file handle leaked: %v", err)
	}
}

func TestOpenFileBufferedErrors(t *testing.T) {
	var queries int
	queryInfo := func(windows.Handle, uint32, *byte, uint32) error {
		queries++
		return windows.ERROR_NOT_SUPPORTED
	}
	path := filepath.Join(t.TempDir(), "missing.dat")
	if _, err := openVolumeDataWithInfo(path, os.O_RDONLY, 0600, queryInfo); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	} else if queries != 0 {
		t.Fatal("queried metadata after failed open")
	}
	if err := os.WriteFile(path, []byte("read only"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := openVolumeDataWithInfo(path, os.O_RDONLY, 0600, queryInfo)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteSector(new([proto4.SectorSize]byte), 0); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("buffered write error lost Windows error identity: %v", err)
	}
}
