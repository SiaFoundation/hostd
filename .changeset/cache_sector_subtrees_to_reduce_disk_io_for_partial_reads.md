---
default: minor
---

# Cache sector subtrees to reduce disk IO for partial reads.

This change reduces the minimum read size from 4MiB to 4KiB when reading segments of a sector
