package directio

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

	"golang.org/x/sys/windows"
)

func scratchFile(t *testing.T) *File {
	t.Helper()
	f, err := OpenFile(filepath.Join(t.TempDir(), "volume.dat"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
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
		if n, err := f.WriteAt(data, offset); n != len(data) || err != nil {
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
		if n, err := f.ReadAt(read, offset); n != len(read) || err != nil || !bytes.Equal(read, data) {
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
	const sectorSize, fileSize, maxWrite = 4 << 20, 24 << 20, 8 << 20
	rng := rand.New(rand.NewSource(1))
	want := make([]byte, fileSize)
	rng.Read(want)
	path := filepath.Join(t.TempDir(), "volume.dat")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	sizes := []int{
		f.blockSize,
		max(f.blockSize, 4<<10),
		max(f.blockSize, (4<<10)+f.blockSize),
		max(f.blockSize, 64<<10),
		max(f.blockSize, (64<<10)+f.blockSize),
		max(f.blockSize, sectorSize-f.blockSize),
		sectorSize,
		sectorSize + f.blockSize,
		maxWrite,
	}
	for i, size := range sizes {
		sizes[i] = (size + f.blockSize - 1) / f.blockSize * f.blockSize
	}
	writeBuffer := testAlignedBuffer(maxWrite+1, f.alignment)
	readBuffer := testAlignedBuffer(maxWrite+1, f.alignment)
	var offset int
	for iteration := range 96 {
		size := sizes[(iteration/2)%len(sizes)]
		if iteration >= 2*len(sizes) {
			size = sizes[rng.Intn(len(sizes))]
		}
		// Leave guards at both ends and repeat locations to exercise overwrites.
		if iteration%4 != 1 {
			offset = sectorSize + rng.Intn((fileSize-2*sectorSize-maxWrite)/f.blockSize+1)*f.blockSize
		}
		data := writeBuffer[iteration%2 : iteration%2+size]
		rng.Read(data)
		data[0] = byte(iteration + 1)
		copy(want[offset:offset+size], data)
		if n, err := f.WriteAt(data, int64(offset)); n != size || err != nil {
			t.Fatalf("iteration %d write offset=%d size=%d: n=%d err=%v", iteration, offset, size, n, err)
		} else if !bytes.Equal(data, want[offset:offset+size]) {
			t.Fatalf("iteration %d write modified its input", iteration)
		} else if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		read := readBuffer[(iteration/2)%2 : (iteration/2)%2+size]
		if n, err := f.ReadAt(read, int64(offset)); n != size || err != nil || !bytes.Equal(read, want[offset:offset+size]) {
			t.Fatalf("iteration %d readback offset=%d size=%d: n=%d err=%v", iteration, offset, size, n, err)
		}
		readOffset := rng.Intn(fileSize)
		readSize := 1 + rng.Intn(min(sectorSize+1, fileSize-readOffset))
		read = readBuffer[1 : 1+readSize]
		if n, err := f.ReadAt(read, int64(readOffset)); n != readSize || err != nil || !bytes.Equal(read, want[readOffset:readOffset+readSize]) {
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

func TestFileReadAt(t *testing.T) {
	f := scratchFile(t)
	data := testAlignedBuffer(3*f.blockSize, f.alignment)
	for i := range data {
		data[i] = byte(i*17 + i/251)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	size := f.blockSize + 5
	if err := f.Truncate(int64(size)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		offset int
		length int
	}{
		{0, size},
		{1, 1},
		{f.blockSize - 3, 11},
		{f.blockSize, f.blockSize},
		{f.blockSize + 1, 3},
		{f.blockSize + 1, 4},
		{f.blockSize + 1, 5},
		{size - 2, 2},
		{size - 2, 7},
		{size, 1},
		{size + f.blockSize, 1},
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
			n, err := f.ReadAt(read, int64(tc.offset))
			if n != wantN || !errors.Is(err, wantErr) {
				t.Fatalf("read: n=%d err=%v, want n=%d err=%v", n, err, wantN, wantErr)
			} else if n > 0 && !bytes.Equal(read[:n], data[tc.offset:tc.offset+n]) {
				t.Fatal("read contents differ")
			} else if guarded[0] != 0xa5 || guarded[len(guarded)-1] != 0xa5 {
				t.Fatal("read overwrote bytes outside the supplied slice")
			}
		})
	}
	aligned := testAlignedBuffer(f.blockSize, f.alignment)
	if n, err := f.ReadAt(aligned, int64(f.blockSize)); n != 5 || !errors.Is(err, io.EOF) {
		t.Fatalf("aligned read across EOF: n=%d err=%v", n, err)
	} else if !bytes.Equal(aligned[:5], data[f.blockSize:size]) {
		t.Fatal("aligned read at EOF returned incorrect contents")
	}
	for _, offset := range []int64{-1, math.MaxInt64 - 1, math.MaxInt64} {
		if n, err := f.ReadAt(make([]byte, 8), offset); n != 0 || err == nil {
			t.Fatalf("read offset %d: n=%d err=%v", offset, n, err)
		}
	}
}

func TestFileLargeRead(t *testing.T) {
	f := scratchFile(t)
	size := ((9 << 20) + f.blockSize - 1) / f.blockSize * f.blockSize
	data := testAlignedBuffer(size+1, f.alignment)[1:]
	for i := range data {
		data[i] = byte(i*13 + i/(4<<20))
	}
	if n, err := f.WriteAt(data, 0); n != len(data) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	read := testAlignedBuffer(size, f.alignment)[1:]
	if n, err := f.ReadAt(read, 1); n != len(read) || err != nil || !bytes.Equal(read, data[1:]) {
		t.Fatalf("large partial read: n=%d err=%v equal=%v", n, err, bytes.Equal(read, data[1:]))
	}
}

func TestFileInvalidWrite(t *testing.T) {
	f := scratchFile(t)
	data := bytes.Repeat([]byte{0x6b}, f.blockSize)
	for _, tc := range []struct {
		offset int64
		data   []byte
	}{
		{-1, data},
		{1, data},
		{0, data[:len(data)-1]},
		{int64(f.blockSize), data[:1]},
	} {
		if n, err := f.WriteAt(tc.data, tc.offset); n != 0 || !errors.Is(err, os.ErrInvalid) {
			t.Fatalf("write offset=%d length=%d: n=%d err=%v", tc.offset, len(tc.data), n, err)
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
	data := bytes.Repeat([]byte{0x37}, f.blockSize)
	const base = int64(1 << 32)
	offset := base + int64(f.blockSize)
	if err := f.Truncate(offset + int64(len(data))); err != nil {
		t.Fatal(err)
	} else if n, err := f.WriteAt(data, offset); n != len(data) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, len(data)+2)
	if n, err := f.ReadAt(read, offset-1); n != len(data)+1 || !errors.Is(err, io.EOF) {
		t.Fatalf("read across sparse boundary: n=%d err=%v", n, err)
	} else if read[0] != 0 || !bytes.Equal(read[1:len(read)-1], data) {
		t.Fatal("contents at large offset differ")
	}
}

func TestFileConcurrentIO(t *testing.T) {
	f := scratchFile(t)
	const workers, iterations, sectorSize = 8, 4, 4 << 20
	stride := sectorSize + f.blockSize
	expected := bytes.Repeat([]byte{0xa5}, workers*stride+f.blockSize)
	if n, err := f.WriteAt(expected, 0); n != len(expected) || err != nil {
		t.Fatalf("initialize guarded file: n=%d err=%v", n, err)
	} else if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	results := make(chan error, workers)
	for worker := range workers {
		go func() {
			results <- func() error {
				offset := int64(f.blockSize + worker*stride)
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
					if n, err := f.WriteAt(data, offset); n != len(data) || err != nil {
						return fmt.Errorf("worker %d iteration %d write: n=%d err=%v", worker, iteration, n, err)
					} else if err := f.Sync(); err != nil {
						return err
					} else if n, err := f.ReadAt(read, offset); n != len(read) || err != nil || !bytes.Equal(read, data) {
						return fmt.Errorf("worker %d iteration %d read: n=%d err=%v equal=%v", worker, iteration, n, err, bytes.Equal(read, data))
					}
					length := min(f.blockSize+17, sectorSize/2)
					begin := (iteration*sectorSize/iterations + f.blockSize - 1) % (sectorSize - length)
					partial := read[1 : length+1]
					if n, err := f.ReadAt(partial, offset+int64(begin)); n != length || err != nil || !bytes.Equal(partial, data[begin:begin+length]) {
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
		offset := f.blockSize + worker*stride
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
	if n, err := f.ReadAt(read, 0); n != len(read) || err != nil || !bytes.Equal(read, make([]byte, len(read))) {
		t.Fatalf("read grown empty file: n=%d err=%v", n, err)
	} else if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	} else if n, err := f.ReadAt(read, 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty read: n=%d err=%v", n, err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(read, 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed read: %v", err)
	} else if _, err := f.WriteAt(read, 0); !errors.Is(err, os.ErrClosed) {
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
	if _, err := OpenFile(path, os.O_RDWR, 0600); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
	f := scratchFile(t)
	path = f.file.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.WriteAt(make([]byte, readOnly.blockSize), 0); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("write error lost Windows error identity: %v", err)
	}
}
