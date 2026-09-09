---
default: minor
---

# Index unreferenced sectors for pruning

Sectors now track how many contract and temp storage references they have, and
a location holds a lock while a sector is being written to it. The prune sweep
reads unreferenced sectors from an index instead of scanning every stored
sector, which on a 100 TiB host held the database for about six seconds every
five minutes. The last access timestamp is removed. The upgrade computes the
counts for existing sectors and rewrites the sector table, which takes about a
minute at that size.
