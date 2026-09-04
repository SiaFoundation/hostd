---
default: patch
---

# Fix the merkle cache bloating the stored sectors table

The cache kept 32 KiB of subtree roots inline on every sector row, which slowed
sector reads, pruning and contract root lookups. Cached roots are discarded on
upgrade and rebuilt on the next read.
