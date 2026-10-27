---
default: patch
---

# Fixed excessive Windows memory usage with large storage volumes

Windows does not properly handle large file IO. Buffered random I/O accumulates gigabytes of kernel paged-pool metadata as more of a volume is accessed, exhausting system memory even though the Go heap stays small.

Windows volumes now use unbuffered file I/O to bypass the system data cache. Reads and writes use disk-aligned buffers, and writes retain explicit durability flushes.
