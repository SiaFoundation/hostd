---
default: minor
---

# Added `hostd consensus prune` to shrink the consensus database

The command rewrites `consensus.db` in the data directory, retaining only the blocks and state the host needs from its last scanned chain index, keeping `consensus.pruneTarget` blocks below it. The host must not be running.

This enables older hosts to reduce the size of their consensus database on disk without requiring a new wallet for instant sync.